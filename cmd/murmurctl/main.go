// Command murmurctl is the Stage 0 CLI. It talks to murmurd over a local unix
// socket: run a VM, list VMs (ps), remove a VM.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/voidcubedotgg/murmur/internal/agent"
	"github.com/voidcubedotgg/murmur/internal/api"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(cmd string, args []string) error {
	c := newClient(api.DefaultSocketPath())
	switch cmd {
	case "run":
		return cmdRun(c, args)
	case "ps":
		return cmdPS(c)
	case "rm":
		return cmdRM(c, args)
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func cmdRun(c *http.Client, args []string) error {
	var name, image string
	parseFlags(args, map[string]*string{"--name": &name, "--image": &image})
	if name == "" {
		return fmt.Errorf("run: --name required")
	}
	body, _ := json.Marshal(api.RunRequest{Name: name, Image: image})
	resp, err := c.Post("http://unix/vms", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("daemon: %s", resp.Status)
	}
	fmt.Printf("declared %q desired\n", name)
	return nil
}

func cmdPS(c *http.Client) error {
	resp, err := c.Get("http://unix/vms")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var ps []agent.Status
	if err := json.NewDecoder(resp.Body).Decode(&ps); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "NAME\tDESIRED\tOBSERVED\tIMAGE")
	for _, s := range ps {
		desired := "no"
		if s.Desired {
			desired = "yes"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.Name, desired, s.Observed, s.Image)
	}
	return tw.Flush()
}

func cmdRM(c *http.Client, args []string) error {
	var name string
	parseFlags(args, map[string]*string{"--name": &name})
	if name == "" {
		return fmt.Errorf("rm: --name required")
	}
	req, _ := http.NewRequest(http.MethodDelete, "http://unix/vms/"+name, nil)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("daemon: %s", resp.Status)
	}
	fmt.Printf("removed %q from desired\n", name)
	return nil
}

// newClient returns an http.Client that dials the unix socket regardless of the
// URL host (we use http://unix/... as a placeholder).
func newClient(sockPath string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
			},
		},
	}
}

// parseFlags is a tiny --key value parser; enough for Stage 0's flat commands.
func parseFlags(args []string, into map[string]*string) {
	for i := 0; i+1 < len(args); i += 2 {
		if dst, ok := into[args[i]]; ok {
			*dst = args[i+1]
		}
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  murmurctl run --name NAME [--image IMAGE]
  murmurctl ps
  murmurctl rm --name NAME`)
}
