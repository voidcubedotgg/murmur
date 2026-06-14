// Package api defines the tiny local contract between murmurctl and murmurd.
//
// This is deliberately local-only IPC over a unix socket, NOT the distributed
// RPC of Stage 1. There are no retries, idempotency keys, or partial-failure
// handling here on purpose: the network-is-unreliable lesson belongs to the
// next rung of the ladder. Keeping this dumb keeps that lesson honest.
package api

import (
	"os"
	"path/filepath"
)

// RunRequest asks the daemon to declare a VM desired.
type RunRequest struct {
	Name  string `json:"name"`
	Image string `json:"image,omitempty"`
}

// DefaultSocketPath is where murmurd listens and murmurctl dials. Prefers
// XDG_RUNTIME_DIR so it lands in a per-user runtime dir when available.
func DefaultSocketPath() string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "murmurd.sock")
	}
	return filepath.Join(os.TempDir(), "murmurd.sock")
}
