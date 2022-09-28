// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clock

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// Clock is the interface to the clock used in MatrixOne's timestamp ordering
// based transaction module.
type Clock interface {
	// HasNetworkLatency returns a boolean value indicating whether there is network
	// latency involved when retrieving timestamps.
	HasNetworkLatency() bool
	// MaxOffset returns the max offset of the physical clocks in the cluster.
	MaxOffset() time.Duration
	// Now returns the current timestamp and the upper bound of the current time
	// caused by clock offset.
	Now() (timestamp.Timestamp, timestamp.Timestamp)
	// Update updates the clock based on the received timestamp.
	Update(ts timestamp.Timestamp)
}

var (
	defaultClock Clock
)

// SetupDefaultClock setup global default clock
func SetupDefaultClock(clock Clock) {
	defaultClock = clock
}

// DefaultClock return default clock
func DefaultClock() Clock {
	return defaultClock
}
