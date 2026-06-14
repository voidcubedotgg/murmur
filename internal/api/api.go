// Package api defines the wire contracts between murmur's pieces:
//
//	murmurctl ──unix socket──▶ murmur-control ──TCP/HTTP──▶ murmurd (agent)
//
// The control↔agent hop is the real network (Stage 1): it is unreliable, so the
// control plane retries (at-least-once) and the agent dedups by idempotency key.
// The ctl↔control hop is still local IPC and stays dumb on purpose.
package api

import (
	"os"
	"path/filepath"

	"github.com/voidcubedotgg/murmur/internal/vmm"
)

// RunRequest asks the control plane to place a VM on a named node.
type RunRequest struct {
	Name  string `json:"name"`
	Image string `json:"image,omitempty"`
	Node  string `json:"node"`
}

// Assignment is one entry of the control plane's global desired state: a VM and
// the node it should run on.
type Assignment struct {
	Name  string `json:"name"`
	Image string `json:"image,omitempty"`
	Node  string `json:"node"`
}

// ApplyRequest is the control→agent push: "make this spec part of your local
// desired state". The IdempotencyKey lets the agent recognise a duplicate
// delivery (at-least-once means the same Apply can arrive more than once).
type ApplyRequest struct {
	Spec           vmm.Spec `json:"spec"`
	IdempotencyKey string   `json:"idempotency_key"`
}

// ApplyResponse tells the caller whether this delivery did new work. Applied is
// false when the key was already seen — useful for logs, but NOT relied upon for
// correctness (see internal/agent/server.go).
type ApplyResponse struct {
	Applied bool `json:"applied"`
}

// PSRow is one line of `murmurctl ps`: global desired joined with per-node
// observation.
type PSRow struct {
	Name     string    `json:"name"`
	Node     string    `json:"node"`
	Desired  bool      `json:"desired"`
	Image    string    `json:"image,omitempty"`
	Observed vmm.State `json:"observed"`
}

// DefaultControlSocket is where murmur-control listens and murmurctl dials.
func DefaultControlSocket() string {
	return runtimePath("murmur-control.sock")
}

// DefaultSocketPath is retained for any local agent IPC; control uses
// DefaultControlSocket.
func DefaultSocketPath() string {
	return runtimePath("murmurd.sock")
}

func runtimePath(name string) string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, name)
	}
	return filepath.Join(os.TempDir(), name)
}
