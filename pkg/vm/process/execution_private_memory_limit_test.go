// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/stretchr/testify/require"
)

func setPrivateMemoryLimitForTest(t *testing.T, limit uint64) {
	t.Helper()
	previous := executionPrivateMemoryLimit.Load()
	SetExecutionPrivateMemoryLimit(limit)
	t.Cleanup(func() { SetExecutionPrivateMemoryLimit(previous) })
}

func TestPrivateMemoryHeadroom(t *testing.T) {
	require.Equal(t, uint64(30), privateMemoryHeadroom(100, 70))
	require.Zero(t, privateMemoryHeadroom(100, 100))
	require.Zero(t, privateMemoryHeadroom(100, 170))
}

// With a private limit the headroom is the room left under the limit by this
// process's own resident memory; the host's free memory plays no part.
func TestExecutionPhysicalMemoryHeadroomUsesPrivateLimit(t *testing.T) {
	const gib = uint64(1 << 30)
	before, ok := system.ProcessResidentMemory()
	require.True(t, ok)
	require.NotZero(t, before)

	setPrivateMemoryLimitForTest(t, 1)
	available, measured := executionPhysicalMemoryHeadroom()
	require.True(t, measured)
	require.Zero(t, available, "a process above its private limit has no headroom")

	limit := before + 4*gib
	SetExecutionPrivateMemoryLimit(limit)
	available, measured = executionPhysicalMemoryHeadroom()
	require.True(t, measured)
	after, ok := system.ProcessResidentMemory()
	require.True(t, ok)
	require.LessOrEqual(t, available, limit-min(before, after))
	require.GreaterOrEqual(t, available, limit-max(before, after))
}

func TestExecutionMemoryHeadroomSafetyUsesPrivateLimit(t *testing.T) {
	const gib = uint64(1 << 30)
	ceiling := ExecutionMemoryCeiling{EffectiveCN: 100 * gib, Reserve: 20 * gib}
	require.Equal(t, 5*gib, executionMemoryHeadroomSafety(ceiling))

	// The margin is sized from the private limit, so a large host does not
	// reserve more than the process may use.
	setPrivateMemoryLimitForTest(t, 8*gib)
	require.Equal(t, gib, executionMemoryHeadroomSafety(ceiling))
	SetExecutionPrivateMemoryLimit(60 * gib)
	require.Equal(t, 3*gib, executionMemoryHeadroomSafety(ceiling))
	// A limit above the CN's effective memory changes nothing.
	SetExecutionPrivateMemoryLimit(200 * gib)
	require.Equal(t, 5*gib, executionMemoryHeadroomSafety(ceiling))
}

// Admission through the CN headroom source follows this process's own size
// under a private limit.
func TestExecutionAdmissionFollowsPrivateMemoryLimit(t *testing.T) {
	const gib = uint64(1 << 30)
	for _, tc := range []struct {
		name     string
		limit    func(resident uint64) uint64
		admitted bool
	}{
		{name: "process above its limit", limit: func(uint64) uint64 { return 1 }},
		{name: "room under the limit", limit: func(resident uint64) uint64 { return resident + 2*gib }, admitted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resident, ok := system.ProcessResidentMemory()
			require.True(t, ok)
			setPrivateMemoryLimitForTest(t, tc.limit(resident))
			budget := MustNewExecutionResourceBudget(1000, 800)
			budget.memoryHeadroomProvider = executionPhysicalMemoryHeadroom
			budget.memoryHeadroomSafety = 100
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			defer generation.Close()
			reservation, err := generation.ReserveTransientMemory(48)
			if tc.admitted {
				require.NoError(t, err)
				require.True(t, reservation.Release())
				return
			}
			require.ErrorIs(t, err, ErrExecutionResourceAdmission)
			require.Nil(t, reservation)
		})
	}
}
