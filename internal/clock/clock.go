// Package clock is the injected sense of time shared across murmur. Both the
// agent reconcile loop and the cluster failure detector take a Clock, so tests
// can drive them on a controllable clock and replay exact timings. This is the
// determinism investment that pays off at the partition/simulation stages.
package clock

import "time"

// Clock is a minimal, injectable view of time.
type Clock interface {
	Now() time.Time
	// After returns a channel that fires once after d.
	After(d time.Duration) <-chan time.Time
}

// RealClock is the wall-clock implementation.
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// SimClock is a virtual clock for deterministic simulation. Time only moves when
// the simulator advances it, so components driven by Tick(clk.Now()) see exactly
// the timeline the simulator dictates — no wall-clock, no flakiness.
//
// After returns a never-firing channel: the simulator drives components through
// Tick/Deliver, not their Run loops, so After is never consumed in sim. (It must
// exist only to satisfy the Clock interface.)
type SimClock struct {
	now time.Time
}

// NewSimClock starts virtual time at a fixed, arbitrary epoch.
func NewSimClock() *SimClock {
	return &SimClock{now: time.Unix(0, 0)}
}

func (c *SimClock) Now() time.Time                       { return c.now }
func (c *SimClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

// Advance moves virtual time forward by d.
func (c *SimClock) Advance(d time.Duration) { c.now = c.now.Add(d) }
