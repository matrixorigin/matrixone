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

package disttae

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A scan uses the pace of scans until it has read enough to measure its own, which then
// counts toward the pace of scans.
func TestScanPace(t *testing.T) {
	saved := scanPaces.Load()
	t.Cleanup(func() { scanPaces.Store(saved) })
	scanPaces.Store(math.Float64bits(100))

	now := time.Now()
	ls := &LocalDisttaeDataSource{}
	require.Equal(t, 100.0, ls.scanPace(now), "nothing read yet")
	ls.rangesCursor = scanPaceMinBlocks - 1
	require.Equal(t, 100.0, ls.scanPace(now.Add(time.Second)), "too few blocks")

	ls.rangesCursor = 400
	require.InDelta(t, 200.0, ls.scanPace(now.Add(2*time.Second)), 1e-9)
	require.InDelta(t, (1-scanPaceWeight)*100+scanPaceWeight*200, math.Float64frombits(scanPaces.Load()), 1e-9)
}
