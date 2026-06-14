// Command murmurd is a murmur peer. In the CRDT path there is no leader and no
// central control plane: each peer runs the VMM, a local reconcile loop, SWIM
// membership, and a replicated desired-state store that it anti-entropy-gossips
// with the other peers. Each peer runs the share of the converged desired state
// assigned to its own node. murmurctl talks to any peer over a unix socket.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/voidcubedotgg/murmur/internal/agent"
	"github.com/voidcubedotgg/murmur/internal/api"
	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/market"
	"github.com/voidcubedotgg/murmur/internal/state"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

func main() {
	var (
		fake        = flag.Bool("fake", false, "use the in-memory fake VMM instead of smolvm")
		node        = flag.String("node", hostname(), "node id (and gossip identity)")
		interval    = flag.Duration("interval", 2*time.Second, "reconcile interval")
		smolbin     = flag.String("smolvm", "smolvm", "smolvm binary path")
		snapDir     = flag.String("snap-dir", "", "shared dir for fake-VMM snapshots (cross-host restore needs this on a shared FS); empty = local default")
		gossipAddr  = flag.String("gossip-addr", "", "UDP address for SWIM membership gossip")
		seeds       = flag.String("seeds", "", "comma-separated SWIM seed addresses")
		stateAddr   = flag.String("state-addr", "", "UDP address for CRDT state gossip")
		stateSeeds  = flag.String("state-seeds", "", "comma-separated state-gossip seed addresses")
		socket      = flag.String("socket", "", "murmurctl unix socket (default murmurd-<node>.sock)")
		capacity    = flag.Int("capacity", 2, "max VMs this peer will claim")
		snapEvery   = flag.Duration("snapshot-interval", 5*time.Second, "how often to snapshot owned VMs")
		gossipEvery = flag.Duration("gossip-interval", 500*time.Millisecond, "state-gossip period")
		clusterSize = flag.Int("cluster-size", 1, "fixed expected cluster size (for quorum)")
		fencing     = flag.Bool("fencing", true, "quorum-gate ownership to prevent split-brain")
	)
	flag.Parse()

	if *socket == "" {
		*socket = api.AgentSocketPath(*node)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var v vmm.VMM
	if *fake {
		// A shared snap dir (NFS mount) is what makes cross-host failover restore
		// real state: the survivor reads the dead owner's snapshot file. Without it
		// each host snapshots locally and a re-claim boots fresh (state lost).
		var fv *vmm.Fake
		if *snapDir != "" {
			fv = vmm.NewFakeWithSnapDir(*snapDir)
		} else {
			fv = vmm.NewFake()
		}
		fv.StartWorkload(ctx, time.Second) // simulate a workload accruing state
		v = fv
		log.Info("using fake VMM", "node", *node, "snap-dir", *snapDir)
	} else {
		v = vmm.NewSmolvm(*smolbin)
		log.Info("using smolvm VMM", "node", *node, "bin", *smolbin)
	}

	// Replicated desired state (the CRDT store), gossiped peer-to-peer.
	var store *state.Store
	if *stateAddr != "" {
		tr, err := cluster.NewUDPTransport(*stateAddr)
		if err != nil {
			log.Error("state transport failed", "err", err)
			os.Exit(1)
		}
		store = state.New(*node, *stateAddr, splitCSV(*stateSeeds), tr, clock.RealClock{},
			rand.New(rand.NewSource(time.Now().UnixNano())), log)
		go store.Run(ctx, *gossipEvery)
		go func() { <-ctx.Done(); _ = tr.Close() }() // release the socket on shutdown
	} else {
		// No gossip configured: a lone in-memory store so single-node runs work.
		store = state.New(*node, "", nil, nil, clock.RealClock{}, nil, log)
	}

	// hasQuorum: can we see a majority of the FIXED cluster? Built after SWIM is
	// created below; declared here so the reconciler source can close over it.
	quorum := *clusterSize/2 + 1
	var sw *cluster.SWIM
	hasQuorum := func() bool {
		if !*fencing || sw == nil {
			return true
		}
		return sw.AliveCount() >= quorum
	}

	// SWIM membership: liveness oracle for the market (a claim is honoured only
	// while its owner is Alive), quorum input, and the `nodes` view. Built BEFORE
	// any goroutine that closes over sw (reconciler/market via hasQuorum), so the
	// assignment of sw happens-before those reads — otherwise it's a data race.
	if *gossipAddr != "" {
		tr, err := cluster.NewUDPTransport(*gossipAddr)
		if err != nil {
			log.Error("gossip transport failed", "err", err)
			os.Exit(1)
		}
		sw = cluster.NewSWIM(*node, *gossipAddr, cluster.DefaultConfig(), tr, clock.RealClock{}, nil, log)
		sw.Join(ctx, splitCSV(*seeds))
		go sw.Run(ctx)
		go func() { <-ctx.Done(); _ = tr.Close() }() // release the socket on shutdown
	}

	// Reconciler: runs exactly the VMs claimed by THIS node, restoring from the
	// snapshot recorded in the claim when one exists (a re-claim from a dead peer).
	r := agent.NewReconciler(*node, v, clock.RealClock{}, *interval, log)
	r.SetSource(func() []agent.DesiredVM {
		// Self-fence: if we've lost quorum (we're the minority side of a
		// partition), desire NOTHING — the reconciler will stop everything we run,
		// so the majority can own it without a second live copy. CAP in action: we
		// give up availability here to keep safety.
		if !hasQuorum() {
			return nil
		}
		desired := map[string]state.Spec{}
		for _, sp := range store.Desired() {
			desired[sp.Name] = sp
		}
		var out []agent.DesiredVM
		for name, c := range store.Claims() {
			if c.Owner != *node {
				continue
			}
			sp, ok := desired[name]
			if !ok {
				continue // claimed but no longer desired; reconciler will kill it
			}
			out = append(out, agent.DesiredVM{
				Spec:        vmm.Spec{Name: name, Image: sp.Image},
				SnapshotRef: vmm.SnapshotRef(c.SnapshotRef),
			})
		}
		return out
	})
	go r.Run(ctx)

	// Market scheduler: claims unowned/dead-owned desired VMs up to capacity.
	sched := market.New(*node, *capacity, store, livenessOf(sw, *node), hasQuorum, *interval, clock.RealClock{}, log)
	go sched.Run(ctx)

	// Snapshot loop: periodically snapshot the VMs I own so a survivor can restore
	// them after I die. Cadence is the durability/RPO knob.
	go snapshotLoop(ctx, *node, *snapEvery, store, v, log)

	if err := serve(ctx, *socket, newServer(*node, store, v, sw), log); err != nil {
		log.Error("agent server exited", "err", err)
		os.Exit(1)
	}
}

func newServer(node string, store *state.Store, v vmm.VMM, sw *cluster.SWIM) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/vms", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			var rr api.RunRequest
			if err := json.NewDecoder(req.Body).Decode(&rr); err != nil || rr.Name == "" {
				http.Error(w, "bad request: need {name}", http.StatusBadRequest)
				return
			}
			// Just record intent; the market decides who runs it.
			store.SetDesired(state.Spec{Name: rr.Name, Image: rr.Image})
			w.WriteHeader(http.StatusAccepted)
		case http.MethodGet:
			writeJSON(w, psRows(req.Context(), node, store, v))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/vms/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/vms/")
		if name == "" {
			http.Error(w, "bad request: need name", http.StatusBadRequest)
			return
		}
		store.RemoveDesired(name)
		w.WriteHeader(http.StatusAccepted)
	})

	mux.HandleFunc("/nodes", func(w http.ResponseWriter, req *http.Request) {
		if sw == nil {
			writeJSON(w, []cluster.Member{})
			return
		}
		writeJSON(w, sw.Members())
	})

	return mux
}

// psRows joins desired VMs with their converged claim (OWNER) and this peer's
// local observation. We can only truthfully report Observed for VMs WE own; for
// others it's "-" (a peer doesn't observe another peer's VMM — that'd be a
// status CRDT, left for later).
func psRows(ctx context.Context, node string, store *state.Store, v vmm.VMM) []api.PSRow {
	observed := map[string]vmm.State{}
	if obs, err := v.List(ctx); err == nil {
		for _, o := range obs {
			observed[o.Name] = o.State
		}
	}
	// Counter is only readable for VMs we own (it lives in our local VMM).
	counter := func(string) int { return 0 }
	if c, ok := v.(interface{ Counter(string) int }); ok {
		counter = c.Counter
	}
	claims := store.Claims()
	var rows []api.PSRow
	for _, sp := range store.Desired() {
		owner := claims[sp.Name].Owner
		o := "-"
		ctr := 0
		if owner == node {
			ctr = counter(sp.Name)
			if st, ok := observed[sp.Name]; ok {
				o = string(st)
			} else {
				o = string(vmm.Missing)
			}
		}
		if owner == "" {
			owner = "(unclaimed)"
		}
		rows = append(rows, api.PSRow{Name: sp.Name, Node: owner, Image: sp.Image, Observed: o, Counter: ctr})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
}

// alwaysAlive is the liveness oracle when membership is disabled (single-node
// runs): the lone peer claims everything.
type alwaysAlive struct{}

func (alwaysAlive) Alive(string) bool { return true }

func livenessOf(sw *cluster.SWIM, _ string) market.Membership {
	if sw == nil {
		return alwaysAlive{}
	}
	return sw
}

// snapshotLoop periodically snapshots the VMs this peer owns and records the
// fresh SnapshotRef in the claim, so a survivor that re-claims after we die can
// Restore rather than boot fresh. Cadence is the durability/RPO knob.
func snapshotLoop(ctx context.Context, node string, every time.Duration, store *state.Store, v vmm.VMM, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Sorted iteration: SetClaim advances our Lamport clock, so we keep the
			// same "behaviour-affecting map iteration is sorted" discipline the
			// simulator relies on (irrelevant to prod correctness, but consistent).
			claims := store.Claims()
			names := make([]string, 0, len(claims))
			for name := range claims {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				c := claims[name]
				if c.Owner != node {
					continue
				}
				ref, err := v.Snapshot(ctx, name)
				if err != nil {
					continue // not running yet / nothing to snapshot
				}
				c.SnapshotRef = string(ref)
				store.SetClaim(name, c)
			}
		}
	}
}

func serve(ctx context.Context, sockPath string, h http.Handler, log *slog.Logger) error {
	_ = os.Remove(sockPath)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return err
	}
	defer os.Remove(sockPath)

	srv := &http.Server{Handler: h}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("agent listening", "socket", sockPath)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, val any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(val)
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "node"
	}
	return h
}
