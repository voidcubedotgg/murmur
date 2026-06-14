package agent

import "time"

// Clock is injected so the reconcile loop's sense of time is controllable. In
// production it's the wall clock; in tests it's a fake we tick by hand. This is
// the cheap habit that pays off enormously at the partition-testing stage
// (GOALS Stage 7): deterministic time means deterministic, replayable failures.
type Clock interface {
	Now() time.Time
	// After returns a channel that fires once after d.
	After(d time.Duration) <-chan time.Time
}

// RealClock is the wall-clock implementation.
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
