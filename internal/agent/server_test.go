package agent

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/voidcubedotgg/murmur/internal/api"
	"github.com/voidcubedotgg/murmur/internal/vmm"
)

func newTestServer() (*httptest.Server, *vmm.Fake) {
	f := vmm.NewFake()
	r := NewReconciler("test", f, testClock{}, time.Second, nil)
	s := NewServer(r, nil)
	return httptest.NewServer(s.Handler()), f
}

func postApply(t *testing.T, ts *httptest.Server, req api.ApplyRequest) api.ApplyResponse {
	t.Helper()
	body, _ := json.Marshal(req)
	resp, err := http.Post(ts.URL+"/apply", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("apply: %s", resp.Status)
	}
	var ar api.ApplyResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		t.Fatal(err)
	}
	return ar
}

// Duplicate delivery of the same idempotency key is deduped: the second Apply
// reports Applied=false. (At-least-once means the same push can arrive twice.)
func TestApplyDedupsByKey(t *testing.T) {
	ts, _ := newTestServer()
	defer ts.Close()

	req := api.ApplyRequest{Spec: vmm.Spec{Name: "counter"}, IdempotencyKey: "counter:1"}

	if ar := postApply(t, ts, req); !ar.Applied {
		t.Fatal("first apply should report Applied=true")
	}
	if ar := postApply(t, ts, req); ar.Applied {
		t.Fatal("duplicate apply (same key) should report Applied=false")
	}

	// A new generation key is treated as new work.
	req2 := api.ApplyRequest{Spec: vmm.Spec{Name: "counter"}, IdempotencyKey: "counter:2"}
	if ar := postApply(t, ts, req2); !ar.Applied {
		t.Fatal("new key should report Applied=true")
	}
}

// An apply with no key is always treated as new work (still safe: SetDesired is
// idempotent).
func TestApplyWithoutKeyAlwaysApplies(t *testing.T) {
	ts, _ := newTestServer()
	defer ts.Close()
	req := api.ApplyRequest{Spec: vmm.Spec{Name: "counter"}}
	if ar := postApply(t, ts, req); !ar.Applied {
		t.Fatal("keyless apply should report Applied=true")
	}
	if ar := postApply(t, ts, req); !ar.Applied {
		t.Fatal("keyless apply should always report Applied=true")
	}
}
