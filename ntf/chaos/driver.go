// Copyright 2026 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chaos

import (
	"context"
	"encoding/json"
	"time"
)

// Driver is a named factory of schedules: it decides when a worker acts, how
// long the worker holds each effect, and how many effects it keeps open. It
// never decides what is done. It is registered by name in a driver Registry,
// and a worker's driver names it in the keyed form.
//
// Info and New are safe to call concurrently.
type Driver interface {
	// Info describes the driver: its params schema and what it needs of the
	// kind it drives.
	Info() DriverInfo

	// New returns a schedule for the params of one worker's driver, the
	// value under the driver's name in the keyed form, that waits on clock.
	// The params have been checked against the driver's schema; New refuses
	// params it still cannot use. The engine calls New once for each worker
	// that names the driver.
	New(params json.RawMessage, clock Clock) (Schedule, error)
}

// DriverInfo describes a driver. Fields may be added to it, so a driver
// returns it as a struct literal with named fields.
type DriverInfo struct {
	// Schema is the JSON Schema of the driver's params.
	Schema json.RawMessage

	// NeedsHolds means the driver only makes sense with a kind that holds
	// its effects, KindInfo.Holds, such as one that keeps n effects open. A
	// run pairing it with an instant kind is refused when it starts.
	NeedsHolds bool
}

// Schedule is one worker's instance of a driver, made by Driver.New with the
// worker's params. The worker calls Next in a loop, never concurrently.
//
// The run suspends a schedule during a quiet window by ending the ctx of the
// Next in progress, and calls Next again when the window closes. A schedule
// never sees the window: a Next whose ctx ended leaves it ready for the next
// call, which picks its timing up afresh.
type Schedule interface {
	// Next waits on the schedule's clock until the worker should act and
	// returns what to do. open is how many effects the worker holds when
	// Next is called. Next returns ctx's error when ctx ends first, and
	// ErrDriverDone once the schedule is finished.
	Next(ctx context.Context, open int) (Act, error)
}

// Act is what a schedule tells its worker to do now.
type Act struct {
	// Apply asks the worker to call its action's Apply once.
	Apply bool

	// Hold is how long the worker holds the Effect the Apply returns before
	// reverting it. Zero holds it until Keep makes room, a quiet window opens
	// or the run ends.
	Hold time.Duration

	// Keep is the most effects the worker holds once it has acted, reverting
	// its oldest ones first to make room: before an Apply it reverts until
	// fewer than Keep remain, and without one until no more than Keep
	// remain. Zero sets no limit.
	Keep int
}

// Clock is the time a schedule waits on. The engine passes a schedule the
// run's clock, and tests pass one they advance by hand, so drivers test
// without sleeping.
type Clock interface {
	// Now returns the clock's current time.
	Now() time.Time

	// After returns a channel that receives the clock's time once d has
	// passed on the clock.
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the Clock of the host, over time.Now and time.After.
type SystemClock struct{}

// Now returns time.Now.
func (SystemClock) Now() time.Time {
	return time.Now()
}

// After returns time.After(d).
func (SystemClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}
