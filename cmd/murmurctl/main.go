// Command murmurctl is the Stage 1 CLI. It talks to murmur-control over a local
// unix socket: place a VM on a named node, list VMs (ps), remove a VM.
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

	"github.com/voidcubedotgg/murmur/internal/api"
	"github.com/voidcubedotgg/murmur/internal/cluster"
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
	c := newClient(api.DefaultControlSocket())
	switch cmd {
	case "run":
		return cmdRun(c, args)
	case "ps":
		return cmdPS(c)
	case "nodes":
		return cmdNodes(c)
	case "rm":
		return cmdRM(c, args)
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func cmdRun(c *http.Client, args []string) error {
	var name, image, node string
	parseFlags(args, map[string]*string{"--name": &name, "--image": &image, "--node": &node})
	if name == "" || node == "" {
		return fmt.Errorf("run: --name and --node required")
	}
	body, _ := json.Marshal(api.RunRequest{Name: name, Image: image, Node: node})
	resp, err := c.Post("http://unix/vms", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("control: %s", resp.Status)
	}
	fmt.Printf("placed %q on %q\n", name, node)
	return nil
}

func cmdPS(c *http.Client) error {
	resp, err := c.Get("http://unix/vms")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var rows []api.PSRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "NAME\tNODE\tDESIRED\tOBSERVED\tIMAGE")
	for _, s := range rows {
		desired := "no"
		if s.Desired {
			desired = "yes"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", s.Name, s.Node, desired, s.Observed, s.Image)
	}
	return tw.Flush()
}

func cmdNodes(c *http.Client) error {
	resp, err := c.Get("http://unix/nodes")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var members []cluster.Member
	if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
		return err
	}
	if len(members) == 0 {
		fmt.Println("(no membership; control started without --gossip-addr)")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "ID\tSTATE\tINCARNATION\tADDR")
	for _, m := range members {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", m.ID, m.State, m.Incarnation, m.Addr)
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
		return fmt.Errorf("control: %s", resp.Status)
	}
	fmt.Printf("removed %q\n", name)
	return nil
}

// newClient dials the unix socket regardless of URL host.
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

func parseFlags(args []string, into map[string]*string) {
	for i := 0; i+1 < len(args); i += 2 {
		if dst, ok := into[args[i]]; ok {
			*dst = args[i+1]
		}
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  murmurctl run --name NAME --node NODE [--image IMAGE]
  murmurctl ps
  murmurctl nodes
  murmurctl rm --name NAME`)
}
