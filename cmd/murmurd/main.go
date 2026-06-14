// Command murmurd is the node-local agent daemon. It owns local desired state,
// runs the reconcile loop against a VMM, and exposes an RPC server over TCP for
// the control plane to push assignments. Stage 1: one agent per node.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/voidcubedotgg/murmur/internal/agent"
	"github.com/voidcubedotgg/murmur/internal/clock"
	"github.com/voidcubedotgg/murmur/internal/cluster"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

func main() {
	var (
		fake       = flag.Bool("fake", false, "use the in-memory fake VMM instead of smolvm")
		node       = flag.String("node", hostname(), "node id used in logs")
		listen     = flag.String("listen", "127.0.0.1:7777", "agent RPC listen address")
		interval   = flag.Duration("interval", 3*time.Second, "reconcile interval")
		smolbin    = flag.String("smolvm", "smolvm", "smolvm binary path")
		gossipAddr = flag.String("gossip-addr", "", "UDP address for SWIM gossip (e.g. 127.0.0.1:8777); empty disables membership")
		seeds      = flag.String("seeds", "", "comma-separated seed gossip addresses to join")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var v vmm.VMM
	if *fake {
		v = vmm.NewFake()
		log.Info("using fake VMM", "node", *node)
	} else {
		v = vmm.NewSmolvm(*smolbin)
		log.Info("using smolvm VMM", "node", *node, "bin", *smolbin)
	}

	r := agent.NewReconciler(*node, v, agent.RealClock{}, *interval, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go r.Run(ctx)

	// Membership: join the SWIM gossip group so the cluster (and control plane)
	// can tell whether this node is alive. Optional — empty --gossip-addr keeps
	// the Stage-1 behaviour for simple single-node runs.
	if *gossipAddr != "" {
		tr, err := cluster.NewUDPTransport(*gossipAddr)
		if err != nil {
			log.Error("gossip transport failed", "err", err)
			os.Exit(1)
		}
		sw := cluster.NewSWIM(*node, *gossipAddr, cluster.DefaultConfig(), tr, clock.RealClock{}, nil, log)
		go sw.Run(ctx)
		sw.Join(ctx, splitSeeds(*seeds))
	}

	srv := agent.NewServer(r, log.With("node", *node))
	if err := srv.Serve(ctx, *listen); err != nil {
		log.Error("agent server exited with error", "err", err)
		os.Exit(1)
	}
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

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "node"
	}
	return h
}
