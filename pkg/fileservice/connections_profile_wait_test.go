// Copyright 2026 Matrix Origin
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

package fileservice

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Long waits for a connection are counted until a warning reports and resets them.
func TestConnWaitReporterCountsLongWaits(t *testing.T) {
	var r connWaitReporter
	r.last = time.Now()
	r.record("host", connWaitWarn/2, false)
	require.Zero(t, r.waits, "a short wait is not counted")
	r.record("host-a", connWaitWarn, true)
	r.record("host-b", 3*connWaitWarn, false)
	require.Equal(t, int64(2), r.waits)
	require.Equal(t, int64(1), r.reused)
	require.Equal(t, 3*connWaitWarn, r.max)
	require.Equal(t, "host-b", r.host)

	r.last = time.Now().Add(-connWaitWarnInterval)
	r.record("host-c", connWaitWarn, false)
	require.Zero(t, r.waits, "the warning resets the counts")
	require.Zero(t, r.max)
}
