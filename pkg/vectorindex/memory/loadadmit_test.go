// Copyright 2021 Matrix Origin
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

package memory

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeviceAdmitLoadAggregate(t *testing.T) {
	free := func(v uint64) func() (uint64, error) {
		return func() (uint64, error) { return v, nil }
	}
	errGetter := func() (uint64, error) { return 0, fmt.Errorf("boom") }

	t.Run("zero bytes is a no-op even when getter would error", func(t *testing.T) {
		require.NoError(t, DeviceAdmitLoadAggregate(0, errGetter, "T"))
	})
	t.Run("fits at exactly the 60% budget", func(t *testing.T) {
		// 10 GiB free -> budget = 6 GiB. 6 GiB request fits (want <= budget).
		require.NoError(t, DeviceAdmitLoadAggregate(6<<30, free(10<<30), "T"))
	})
	t.Run("one byte over the 60% budget rejects", func(t *testing.T) {
		err := DeviceAdmitLoadAggregate((6<<30)+1, free(10<<30), "T")
		require.Error(t, err)
		require.Contains(t, err.Error(), "T:")
	})
	t.Run("getter error is surfaced as a fail-loud admission error", func(t *testing.T) {
		err := DeviceAdmitLoadAggregate(1, errGetter, "T")
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot query free VRAM")
		require.Contains(t, err.Error(), "boom")
	})
}

func TestDeviceAdmitLoad(t *testing.T) {
	// A fixed per-device free-bytes table simplifies the tests.
	makeGetter := func(freeByDev map[int]uint64) func(int) (uint64, error) {
		return func(d int) (uint64, error) {
			if v, ok := freeByDev[d]; ok {
				return v, nil
			}
			return 0, fmt.Errorf("device %d not in table", d)
		}
	}

	t.Run("empty map: nothing to admit", func(t *testing.T) {
		require.NoError(t, DeviceAdmitLoad(nil, makeGetter(nil), "T"))
		require.NoError(t, DeviceAdmitLoad(map[int]uint64{}, makeGetter(nil), "T"))
	})

	t.Run("zero-demand device is skipped even if getter would error", func(t *testing.T) {
		require.NoError(t, DeviceAdmitLoad(
			map[int]uint64{99: 0},
			func(d int) (uint64, error) { return 0, fmt.Errorf("should not be called") },
			"T",
		))
	})

	t.Run("all devices fit at exactly 60%", func(t *testing.T) {
		require.NoError(t, DeviceAdmitLoad(
			map[int]uint64{0: 6 << 30, 1: 3 << 30},
			makeGetter(map[int]uint64{0: 10 << 30, 1: 5 << 30}),
			"T",
		))
	})

	t.Run("one device over budget rejects and names that device", func(t *testing.T) {
		err := DeviceAdmitLoad(
			map[int]uint64{0: 6 << 30, 1: (3 << 30) + 1},
			makeGetter(map[int]uint64{0: 10 << 30, 1: 5 << 30}),
			"T",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "device 1")
	})

	t.Run("SHARDED across 2 real GPUs fits when aggregate would over-reject", func(t *testing.T) {
		// This is the exact regression: a 30 GiB tar split across 2 GPUs
		// with 25 GiB free each. Aggregate check would reject (30 > 15G budget
		// on one card); per-device fits (15 <= 15G budget per card).
		perDev := map[int]uint64{0: 15 << 30, 1: 15 << 30}
		freeMap := map[int]uint64{0: 25 << 30, 1: 25 << 30}
		require.NoError(t, DeviceAdmitLoad(perDev, makeGetter(freeMap), "T"))
	})

	t.Run("gpu_multi_simulation aliased shards stack on one device", func(t *testing.T) {
		// Sim path: SHARDED index built with sim=2 loads as 2 shards both on
		// physical device 0. Per-device demand is 2*shardBytes on device 0.
		// With 10 GiB free (6 GiB budget), 4 GiB total demand fits.
		require.NoError(t, DeviceAdmitLoad(
			map[int]uint64{0: 4 << 30},
			makeGetter(map[int]uint64{0: 10 << 30}),
			"T",
		))
		// But 7 GiB total demand exceeds the 6 GiB budget and MUST reject —
		// aggregate-single-device semantics would also reject here, so this
		// pins the sim-mode behavior.
		err := DeviceAdmitLoad(
			map[int]uint64{0: 7 << 30},
			makeGetter(map[int]uint64{0: 10 << 30}),
			"T",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "device 0")
	})

	t.Run("getter error for any participating device fails admission", func(t *testing.T) {
		err := DeviceAdmitLoad(
			map[int]uint64{0: 1, 42: 1},
			makeGetter(map[int]uint64{0: 10 << 30}), // 42 missing
			"T",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot query free VRAM on device")
	})

	t.Run("REPLICATED-style: same bytes on every device", func(t *testing.T) {
		// All devices need full copy; budget check per device.
		perDev := map[int]uint64{0: 4 << 30, 1: 4 << 30, 2: 4 << 30}
		freeMap := map[int]uint64{0: 10 << 30, 1: 10 << 30, 2: 10 << 30}
		require.NoError(t, DeviceAdmitLoad(perDev, makeGetter(freeMap), "T"))
	})

	t.Run("error message contains the tag and human-readable remedy", func(t *testing.T) {
		err := DeviceAdmitLoad(
			map[int]uint64{0: 100 << 30},
			makeGetter(map[int]uint64{0: 10 << 30}),
			"Cagra.loadIndexes",
		)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "Cagra.loadIndexes:"))
		require.True(t, strings.Contains(err.Error(), "evict cached indexes"))
	})
}
