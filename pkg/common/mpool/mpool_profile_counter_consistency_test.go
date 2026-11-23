// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mpool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Counter mirror oracle for the fixed source's empty-store fast path.
// Kept deliberately in its own file: profileTracked exists only in the fixed
// mpool_profile.go, so this file must stay outside the clean main-447 RED
// compilation set (only mpool_profile_toggle_test.go and the integrated
// realloc failure sources are copied there). The toggle oracle itself avoids
// the private counter for exactly that reason, so the same assertions give the
// exact non-zero RED at pristine 447 and the GREEN on the fixed tree.
//
// Real MPool transitions, no shard-map hand deletions, no Parallel: the live
// counter must mirror ProfileTrackedCount at every quiescent step and return
// to baseline in lock-step with shard retirement. Same FailNow resource
// closure as the toggle regression: the live slice is freed before pool
// deletion on every path, cleared after an explicit Free, and transferred to
// the replacement after a successful ReallocZero.
func TestProfileTrackedCounterMirrorsShardPopulationAcrossToggleTransitions(t *testing.T) {
	wasEnabled := ProfilingEnabled()
	t.Cleanup(func() {
		if wasEnabled {
			EnableProfiling()
		} else {
			DisableProfiling()
		}
	})
	baseline := ProfileTrackedCount()
	require.Equal(t, baseline, int(profileTracked.Load()),
		"counter must mirror the shard population before any transition")

	mp, err := NewMPool("test-profile-counter-consistency", 0, NoFixed)
	require.NoError(t, err)
	var live []byte
	t.Cleanup(func() {
		if live != nil {
			mp.Free(live)
			live = nil
		}
		DeleteMPool(mp)
	})

	EnableProfiling()
	live, err = mp.Alloc(512, true)
	require.NoError(t, err)
	require.Equal(t, baseline+1, ProfileTrackedCount(),
		"publish must track the live entry")
	require.Equal(t, baseline+1, int(profileTracked.Load()),
		"counter must count the published record")

	DisableProfiling()
	next, err := mp.ReallocZero(live, 2048, true)
	require.NoError(t, err)
	live = next // the native realloc consumed the old block; the handle transfers
	require.Equal(t, baseline, ProfileTrackedCount(),
		"successful realloc must retire the disabled-switch record")
	require.Equal(t, baseline, int(profileTracked.Load()),
		"detach must decrement the counter in lock-step with the shard map")

	mp.Free(live)
	live = nil
	require.Equal(t, baseline, ProfileTrackedCount(),
		"final free must retire back to the empty store")
	require.Equal(t, baseline, int(profileTracked.Load()),
		"retirement must leave counter and shard population consistent")
}
