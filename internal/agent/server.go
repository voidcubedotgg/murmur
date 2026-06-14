package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/voidcubedotgg/murmur/internal/api"
)

// Server is the agent's RPC face to the control plane. It speaks JSON over
// TCP/HTTP and feeds the local Reconciler. It is the receiving end of the
// "network is unreliable" lesson: the control plane retries at-least-once, so
// the same Apply may arrive multiple times — Server must make that harmless.
type Server struct {
	r   *Reconciler
	log *slog.Logger

	// seen records idempotency keys we've already applied.
	//
	// Nuance worth internalising: correctness does NOT depend on this map. The
	// underlying SetDesired is idempotent, so even if we lose `seen` (agent
	// restart) and the control plane re-pushes, re-applying is harmless. The
	// key's job is (a) to dedup duplicate deliveries within a process lifetime
	// and (b) to teach the at-least-once → idempotency pairing. It is an
	// optimisation and a lesson, not the safety mechanism.
	mu   sync.Mutex
	seen map[string]bool
}

// NewServer builds an agent RPC server over the given Reconciler.
func NewServer(r *Reconciler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{r: r, log: log, seen: make(map[string]bool)}
}

// Handler returns the agent's HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// POST /apply  — control plane pushes a spec into local desired.
	mux.HandleFunc("/apply", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var ar api.ApplyRequest
		if err := json.NewDecoder(req.Body).Decode(&ar); err != nil || ar.Spec.Name == "" {
			http.Error(w, "bad request: need spec.name", http.StatusBadRequest)
			return
		}

		applied := s.markSeen(ar.IdempotencyKey)
		if applied {
			// New delivery: do the work. (If the key was empty we always apply;
			// SetDesired is idempotent so that's still safe.)
			s.r.SetDesired(ar.Spec)
		} else {
			s.log.Info("apply deduped", "vm", ar.Spec.Name, "key", ar.IdempotencyKey)
		}
		writeJSON(w, api.ApplyResponse{Applied: applied})
	})

	// DELETE /vms/{name} — stop desiring a VM locally.
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
		s.r.RemoveDesired(name)
		w.WriteHeader(http.StatusAccepted)
	})

	// GET /vms — report what this agent observes (the truthful answer to
	// "did it land?").
	mux.HandleFunc("/vms", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		obs, err := s.r.vmm.List(req.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, obs)
	})

	return mux
}

// markSeen returns true if this key is new (so the caller should do the work).
// An empty key is always treated as new.
func (s *Server) markSeen(key string) bool {
	if key == "" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[key] {
		return false
	}
	s.seen[key] = true
	return true
}

// Serve runs the agent RPC server on addr until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.Handler()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	s.log.Info("agent rpc listening", "addr", addr)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
