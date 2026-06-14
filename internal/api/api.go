// Package api defines the local contract between murmurctl and a murmurd peer.
//
// In the CRDT path there is no central control plane: murmurctl talks to ANY
// peer over that peer's unix socket. A write lands in that peer's replicated
// store and gossips to the rest; a read returns that peer's converged view.
// Which peer you pick doesn't matter for correctness — only for latency to
// convergence.
package api

import (
	"os"
	"path/filepath"
)

// RunRequest asks a peer to record a desired VM placed on a named node.
type RunRequest struct {
	Name  string `json:"name"`
	Image string `json:"image,omitempty"`
	Node  string `json:"node"`
}

// PSRow is one line of `murmurctl ps`: the converged desired assignment joined,
// for VMs on the queried peer's own node, with what that peer observes.
type PSRow struct {
	Name     string `json:"name"`
	Node     string `json:"node"`
	Image    string `json:"image,omitempty"`
	Observed string `json:"observed"` // observed state if on this peer, else "-"
}

// AgentSocketPath is where a murmurd peer listens for murmurctl. One socket per
// node id so several peers can run on one host during local demos.
func AgentSocketPath(node string) string {
	return runtimePath("murmurd-" + node + ".sock")
}

func runtimePath(name string) string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, name)
	}
	return filepath.Join(os.TempDir(), name)
}
