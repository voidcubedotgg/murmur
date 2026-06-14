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
	"github.com/voidcubedotgg/murmur/internal/state"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

func main() {
	var (
		fake       = flag.Bool("fake", false, "use the in-memory fake VMM instead of smolvm")
		node       = flag.String("node", hostname(), "node id (and gossip identity)")
		interval   = flag.Duration("interval", 2*time.Second, "reconcile interval")
		smolbin    = flag.String("smolvm", "smolvm", "smolvm binary path")
		gossipAddr = flag.String("gossip-addr", "", "UDP address for SWIM membership gossip")
		seeds      = flag.String("seeds", "", "comma-separated SWIM seed addresses")
		stateAddr  = flag.String("state-addr", "", "UDP address for CRDT state gossip")
		stateSeeds = flag.String("state-seeds", "", "comma-separated state-gossip seed addresses")
		socket     = flag.String("socket", "", "murmurctl unix socket (default murmurd-<node>.sock)")
	)
	flag.Parse()

	if *socket == "" {
		*socket = api.AgentSocketPath(*node)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var v vmm.VMM
	if *fake {
		v = vmm.NewFake()
		log.Info("using fake VMM", "node", *node)
	} else {
		v = vmm.NewSmolvm(*smolbin)
		log.Info("using smolvm VMM", "node", *node, "bin", *smolbin)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
		go store.Run(ctx, *interval)
	} else {
		// No gossip configured: a lone in-memory store so single-node runs work.
		store = state.New(*node, "", nil, nil, clock.RealClock{}, nil, log)
	}

	// Reconciler: its desired set is the converged assignments for THIS node.
	r := agent.NewReconciler(*node, v, clock.RealClock{}, *interval, log)
	r.SetSource(func() []vmm.Spec {
		var specs []vmm.Spec
		for _, a := range store.AssignmentsFor(*node) {
			specs = append(specs, vmm.Spec{Name: a.Name, Image: a.Image})
		}
		return specs
	})
	go r.Run(ctx)

	// SWIM membership (for `nodes` + future claim-liveness).
	var sw *cluster.SWIM
	if *gossipAddr != "" {
		tr, err := cluster.NewUDPTransport(*gossipAddr)
		if err != nil {
			log.Error("gossip transport failed", "err", err)
			os.Exit(1)
		}
		sw = cluster.NewSWIM(*node, *gossipAddr, cluster.DefaultConfig(), tr, clock.RealClock{}, nil, log)
		sw.Join(ctx, splitCSV(*seeds))
		go sw.Run(ctx)
	}

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
			if err := json.NewDecoder(req.Body).Decode(&rr); err != nil || rr.Name == "" || rr.Node == "" {
				http.Error(w, "bad request: need {name,node}", http.StatusBadRequest)
				return
			}
			store.Set(state.Assignment{Name: rr.Name, Image: rr.Image, Node: rr.Node})
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
		store.Remove(name)
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

// psRows joins the converged desired assignments with this peer's local
// observation. We can only truthfully report Observed for VMs on our own node;
// others show "-" (a peer doesn't observe another peer's VMM — that's a status
// CRDT we could add later).
func psRows(ctx context.Context, node string, store *state.Store, v vmm.VMM) []api.PSRow {
	observed := map[string]vmm.State{}
	if obs, err := v.List(ctx); err == nil {
		for _, o := range obs {
			observed[o.Name] = o.State
		}
	}
	var rows []api.PSRow
	for _, a := range store.Snapshot() {
		o := "-"
		if a.Node == node {
			if st, ok := observed[a.Name]; ok {
				o = string(st)
			} else {
				o = string(vmm.Missing)
			}
		}
		rows = append(rows, api.PSRow{Name: a.Name, Node: a.Node, Image: a.Image, Observed: o})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows
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
