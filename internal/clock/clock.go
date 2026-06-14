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
