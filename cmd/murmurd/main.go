// Command murmurd is the node-local agent daemon. It owns desired state, runs
// the reconcile loop against a VMM, and serves a tiny unix-socket API for
// murmurctl. This is Stage 0: one host, no cluster, no RPC.
package main

import (
	"context"
	"encoding/json"
	"flag"
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
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

func main() {
	var (
		fake     = flag.Bool("fake", false, "use the in-memory fake VMM instead of smolvm")
		node     = flag.String("node", hostname(), "node id used in logs")
		sock     = flag.String("socket", api.DefaultSocketPath(), "unix socket path")
		interval = flag.Duration("interval", 3*time.Second, "reconcile interval")
		smolbin  = flag.String("smolvm", "smolvm", "smolvm binary path")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var v vmm.VMM
	if *fake {
		v = vmm.NewFake()
		log.Info("using fake VMM")
	} else {
		v = vmm.NewSmolvm(*smolbin)
		log.Info("using smolvm VMM", "bin", *smolbin)
	}

	r := agent.NewReconciler(*node, v, agent.RealClock{}, *interval, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go r.Run(ctx)

	srv := newServer(r, log)
	if err := serve(ctx, *sock, srv, log); err != nil {
		log.Error("server exited with error", "err", err)
		os.Exit(1)
	}
}

func newServer(r *agent.Reconciler, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	// POST /vms            declare a VM desired   {name,image}
	// GET  /vms            ps view (desired vs observed)
	// DELETE /vms/{name}   stop desiring a VM
	mux.HandleFunc("/vms", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			var rr api.RunRequest
			if err := json.NewDecoder(req.Body).Decode(&rr); err != nil || rr.Name == "" {
				http.Error(w, "bad request: need {name}", http.StatusBadRequest)
				return
			}
			r.SetDesired(vmm.Spec{Name: rr.Name, Image: rr.Image})
			w.WriteHeader(http.StatusAccepted)
		case http.MethodGet:
			ps, err := r.PS(req.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			writeJSON(w, ps)
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
		r.RemoveDesired(name)
		w.WriteHeader(http.StatusAccepted)
	})

	return mux
}

func serve(ctx context.Context, sockPath string, h http.Handler, log *slog.Logger) error {
	// Remove a stale socket from a previous (crashed) run; otherwise Listen fails.
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

	log.Info("listening", "socket", sockPath)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "node"
	}
	return h
}
