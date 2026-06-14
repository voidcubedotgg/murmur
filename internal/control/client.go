// Package control is the cluster brain: it owns global desired state (which VM
// runs on which node) and pushes assignments to agents over RPC. It knows
// nothing about VMs as substrate — it speaks to agents through AgentClient, and
// agents speak to the VMM. That boundary (CLAUDE.md) keeps the cluster logic
// testable against a fake transport.
package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/voidcubedotgg/murmur/internal/api"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

// AgentClient is the control plane's view of an agent. It is an interface so the
// controller can be tested against a fault-injecting fake — the only way to make
// the "network is unreliable" lesson reproducible.
type AgentClient interface {
	Apply(ctx context.Context, addr string, req api.ApplyRequest) (api.ApplyResponse, error)
	Remove(ctx context.Context, addr, name string) error
	List(ctx context.Context, addr string) ([]vmm.Observed, error)
}

// HTTPAgentClient talks to a real murmurd over TCP/HTTP.
type HTTPAgentClient struct {
	hc *http.Client
}

// NewHTTPAgentClient builds a client with a per-call timeout. The timeout is
// itself part of the lesson: a slow node and a dead node look identical from
// here, so every call must be bounded and every failure must be retryable.
func NewHTTPAgentClient(timeout time.Duration) *HTTPAgentClient {
	return &HTTPAgentClient{hc: &http.Client{Timeout: timeout}}
}

var _ AgentClient = (*HTTPAgentClient)(nil)

func (c *HTTPAgentClient) Apply(ctx context.Context, addr string, req api.ApplyRequest) (api.ApplyResponse, error) {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/apply", bytes.NewReader(body))
	if err != nil {
		return api.ApplyResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return api.ApplyResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return api.ApplyResponse{}, fmt.Errorf("apply %s: %s", addr, resp.Status)
	}
	var ar api.ApplyResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return api.ApplyResponse{}, err
	}
	return ar, nil
}

func (c *HTTPAgentClient) Remove(ctx context.Context, addr, name string) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+addr+"/vms/"+name, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("remove %s: %s", addr, resp.Status)
	}
	return nil
}

func (c *HTTPAgentClient) List(ctx context.Context, addr string) ([]vmm.Observed, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/vms", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("list %s: %s", addr, resp.Status)
	}
	var obs []vmm.Observed
	if err := json.NewDecoder(resp.Body).Decode(&obs); err != nil {
		return nil, err
	}
	return obs, nil
}
