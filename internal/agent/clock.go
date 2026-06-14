package agent

import "github.com/voidcubedotgg/murmur/internal/clock"

// Clock and RealClock now live in internal/clock so the cluster package can
// share them without importing agent. These aliases keep existing agent/control
// call sites (agent.Clock, agent.RealClock) working.
type Clock = clock.Clock

type RealClock = clock.RealClock
