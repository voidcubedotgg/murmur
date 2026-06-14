// Command murmur-control is the cluster brain. It owns global desired state,
// pushes assignments to agents over RPC, and serves murmurctl over a unix
// socket. Stage 1: single control plane, static node registry, in-memory state.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/voidcubedotgg/murmur/internal/agent"
	"github.com/voidcubedotgg/murmur/internal/api"
	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/control"
)

func main() {
	var (
		nodes      = flag.String("nodes", "", "node registry, e.g. host-a=127.0.0.1:7777,host-b=127.0.0.1:7778")
		sock       = flag.String("socket", api.DefaultControlSocket(), "murmurctl unix socket path")
		interval   = flag.Duration("interval", 3*time.Second, "control reconcile interval")
		rpcTO      = flag.Duration("rpc-timeout", 2*time.Second, "per-RPC timeout to agents")
		gossipAddr = flag.String("gossip-addr", "", "UDP address for SWIM gossip; empty disables membership (all nodes assumed alive)")
		seeds      = flag.String("seeds", "", "comma-separated seed gossip addresses to join")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	registry, err := parseNodes(*nodes)
	if err != nil {
		log.Error("bad --nodes", "err", err)
		os.Exit(2)
	}
	if len(registry) == 0 {
		log.Error("--nodes is required (no agents to control)")
		os.Exit(2)
	}
	log.Info("node registry", "nodes", registry)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Membership: the control plane joins the gossip group as a member that
	// hosts no VMs. Its SWIM view becomes the liveness oracle the controller
	// uses to stop pushing to dead nodes. With no --gossip-addr we fall back to
	// AlwaysAlive (Stage-1 behaviour).
	var live control.Liveness = control.AlwaysAlive{}
	if *gossipAddr != "" {
		tr, err := cluster.NewUDPTransport(*gossipAddr)
		if err != nil {
			log.Error("gossip transport failed", "err", err)
			os.Exit(1)
		}
		sw := cluster.NewSWIM("control", *gossipAddr, cluster.DefaultConfig(), tr, clock.RealClock{}, nil, log)
		go sw.Run(ctx)
		sw.Join(ctx, splitSeeds(*seeds))
		live = sw
	}

	client := control.NewHTTPAgentClient(*rpcTO)
	ctrl := control.NewController(registry, client, agent.RealClock{}, *interval, live, log)

	go ctrl.Run(ctx)

	if err := serve(ctx, *sock, newServer(ctrl), log); err != nil {
		log.Error("control server exited with error", "err", err)
		os.Exit(1)
	}
}

func newServer(ctrl *control.Controller) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/vms", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			var rr api.RunRequest
			if err := json.NewDecoder(req.Body).Decode(&rr); err != nil || rr.Name == "" || rr.Node == "" {
				http.Error(w, "bad request: need {name,node}", http.StatusBadRequest)
				return
			}
			if err := ctrl.Place(api.Assignment{Name: rr.Name, Image: rr.Image, Node: rr.Node}); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		case http.MethodGet:
			writeJSON(w, ctrl.PS(req.Context()))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/nodes", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, ctrl.Members())
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
		ctrl.Unplace(req.Context(), name)
		w.WriteHeader(http.StatusAccepted)
	})

	return mux
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

	log.Info("control listening", "socket", sockPath)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func splitSeeds(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseNodes turns "a=host:port,b=host:port" into a registry map.
func parseNodes(s string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, pair := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return nil, fmt.Errorf("bad node entry %q (want name=addr)", pair)
		}
		out[kv[0]] = kv[1]
	}
	return out, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
