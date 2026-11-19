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
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/stretchr/testify/require"
)

// Regression sources for the profile toggle contract of issue 32334:
// DisableProfiling only gates NEW sampling. A pointer recorded while profiling
// was on must still retire on Free/teardown/successful Realloc once profiling
// is off, and a failed realloc must restore the exact detached record.
//
// These tests share the package-global profile store and switch, so none of
// them may run in Parallel. Each test restores the original switch state (and
// the malloc default config it touched) and proves retirement through the real
// MPool paths: no record is ever deleted by hand to make an assertion pass.
//
// This file deliberately avoids referencing the fixed source's private
// profileTracked counter, so the same oracle compiles and shows the exact
// non-zero RED on a pristine main-447 tree; the counter mirror oracle lives in
// mpool_profile_counter_consistency_test.go with the fix.
//
// Resource closure (MPool.destroy only sweeps noLock pools, so DeleteMPool
// does NOT free plain off-heap allocations left live on a normal pool): every
// test registers a cleanup that frees its live owner slice(s) before deleting
// the pool, on every path including assertion FailNow; handles are cleared
// after an explicit Free and transferred to the replacement after a successful
// ReallocZero so the old block is never freed twice.

// restoreProfilingSwitch registers cleanup that returns the global profile
// switch to the state observed at test start.
func restoreProfilingSwitch(t *testing.T, wasEnabled bool) {
	t.Cleanup(func() {
		if wasEnabled {
			EnableProfiling()
		} else {
			DisableProfiling()
		}
	})
}

// lookupProfileRecord returns the live address-keyed sample for ptr, if any.
// The profile states being asserted must be read from the shard store itself,
// exactly like the production retire path does.
func lookupProfileRecord(ptr uintptr) (*malloc.HeapSampleValues, bool) {
	shard := getProfileShard(ptr)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	values, ok := shard.m[ptr]
	return values, ok
}

func TestProfileDisableStillRetiresPlainOffHeapOnFree(t *testing.T) {
	restoreProfilingSwitch(t, ProfilingEnabled())
	DisableProfiling()
	trackedBefore := ProfileTrackedCount()

	mp, err := NewMPool("test-profile-toggle-free", 0, NoFixed)
	require.NoError(t, err)
	var live []byte
	t.Cleanup(func() {
		if live != nil {
			mp.Free(live)
			live = nil
		}
		DeleteMPool(mp)
	})

	poolBefore := mp.CurrNB()
	globalBefore := GlobalStats().NumCurrBytes.Load()

	EnableProfiling()
	bs, err := mp.Alloc(4096, true)
	require.NoError(t, err)
	live = bs
	require.Equal(t, trackedBefore+1, ProfileTrackedCount(),
		"alloc while profiling must record the pointer identity")
	ptr := reflect.ValueOf(bs).Pointer()
	recorded, found := lookupProfileRecord(ptr)
	require.True(t, found, "fresh plain off-heap alloc must hold a record")
	require.NotNil(t, recorded)
	afterAllocBytesInuse := recorded.Bytes.Inuse.Load()
	afterAllocObjectsInuse := recorded.Objects.Inuse.Load()

	// Disable only stops NEW sampling; retirement of the recorded identity
	// must follow the record, not the current toggle.
	DisableProfiling()
	require.False(t, ProfilingEnabled())
	mp.Free(bs)
	live = nil

	require.Equal(t, trackedBefore, ProfileTrackedCount(),
		"free after disable must retire the recorded identity exactly")
	_, stillTracked := lookupProfileRecord(ptr)
	require.False(t, stillTracked, "retired pointer must leave the profile shards")
	require.Equal(t, afterAllocBytesInuse-4096, recorded.Bytes.Inuse.Load(),
		"free after disable must retire the sample's inuse bytes")
	require.Equal(t, afterAllocObjectsInuse-1, recorded.Objects.Inuse.Load(),
		"free after disable must retire the sample's inuse objects")
	require.Equal(t, poolBefore, mp.CurrNB())
	require.Equal(t, globalBefore, GlobalStats().NumCurrBytes.Load())
}

func TestProfileUnprofiledLifetimeAcrossEnableToggle(t *testing.T) {
	restoreProfilingSwitch(t, ProfilingEnabled())
	DisableProfiling()
	trackedBefore := ProfileTrackedCount()

	mp, err := NewMPool("test-profile-unprofiled-lifetime", 0, NoFixed)
	require.NoError(t, err)
	var primary, witness []byte
	t.Cleanup(func() {
		if witness != nil {
			mp.Free(witness)
			witness = nil
		}
		if primary != nil {
			mp.Free(primary)
			primary = nil
		}
		DeleteMPool(mp)
	})

	poolBefore := mp.CurrNB()

	bs, err := mp.Alloc(4096, true)
	require.NoError(t, err)
	primary = bs
	require.Equal(t, trackedBefore, ProfileTrackedCount(),
		"allocation while disabled must not create a record")
	ptr := reflect.ValueOf(bs).Pointer()
	_, created := lookupProfileRecord(ptr)
	require.False(t, created)

	// Re-enabling mid-life must not retrofit a record onto a live pointer
	// that was allocated while profiling was off...
	EnableProfiling()
	require.True(t, ProfilingEnabled())
	_, stillAbsent := lookupProfileRecord(ptr)
	require.False(t, stillAbsent)

	// ...while new allocations in the same state do get sampled.
	witness, err = mp.Alloc(256, true)
	require.NoError(t, err)
	require.Equal(t, trackedBefore+1, ProfileTrackedCount())
	mp.Free(witness)
	witness = nil
	require.Equal(t, trackedBefore, ProfileTrackedCount())
	require.Equal(t, poolBefore+4096, mp.CurrNB())

	// Retiring a never-recorded pointer must stay neutral under any switch
	// state: no record invented, no inuse underflow, no count anomaly.
	mp.Free(bs)
	primary = nil
	require.Equal(t, trackedBefore, ProfileTrackedCount(),
		"free of a never-recorded pointer must not change tracked state")
	_, resurrected := lookupProfileRecord(ptr)
	require.False(t, resurrected)
	require.Equal(t, poolBefore, mp.CurrNB())
}

func TestProfileConfigSwitchParityRetiresOnFree(t *testing.T) {
	restoreProfilingSwitch(t, ProfilingEnabled())
	DisableProfiling()
	trackedBefore := ProfileTrackedCount()

	mp, err := NewMPool("test-profile-config-toggle-free", 0, NoFixed)
	require.NoError(t, err)
	var live []byte
	t.Cleanup(func() {
		if live != nil {
			mp.Free(live)
			live = nil
		}
		DeleteMPool(mp)
	})

	poolBefore := mp.CurrNB()

	// The runtime config path (malloc.SetDefaultConfig -> the handler
	// registered in init) must behave exactly like EnableProfiling /
	// DisableProfiling for retirement. WithTempDefaultConfig restores the
	// untouched default config pointer, including a nil MpoolProfiling field,
	// when the test is done.
	malloc.WithTempDefaultConfig(malloc.GetDefaultConfig(), func() {
		enabled := true
		malloc.SetDefaultConfig(malloc.Config{MpoolProfiling: &enabled})
		require.True(t, ProfilingEnabled(),
			"malloc config patch must drive the mpool switch")

		bs, err := mp.Alloc(4096, true)
		require.NoError(t, err)
		live = bs
		require.Equal(t, trackedBefore+1, ProfileTrackedCount())
		ptr := reflect.ValueOf(bs).Pointer()
		recorded, found := lookupProfileRecord(ptr)
		require.True(t, found)
		bytesInuse := recorded.Bytes.Inuse.Load()
		objectsInuse := recorded.Objects.Inuse.Load()

		disabled := false
		malloc.SetDefaultConfig(malloc.Config{MpoolProfiling: &disabled})
		require.False(t, ProfilingEnabled())

		mp.Free(bs)
		live = nil
		require.Equal(t, trackedBefore, ProfileTrackedCount(),
			"config-disabled free must retire the recorded identity")
		_, stillThere := lookupProfileRecord(ptr)
		require.False(t, stillThere)
		require.Equal(t, bytesInuse-4096, recorded.Bytes.Inuse.Load())
		require.Equal(t, objectsInuse-1, recorded.Objects.Inuse.Load())
		require.Equal(t, poolBefore, mp.CurrNB())
	})
}

func TestProfileReallocAfterDisableRetiresOldIdentityWithoutResampling(t *testing.T) {
	restoreProfilingSwitch(t, ProfilingEnabled())
	DisableProfiling()
	trackedBefore := ProfileTrackedCount()

	mp, err := NewMPool("test-profile-toggle-realloc", 0, NoFixed)
	require.NoError(t, err)
	var live []byte
	t.Cleanup(func() {
		if live != nil {
			mp.Free(live)
			live = nil
		}
		DeleteMPool(mp)
	})

	poolBefore := mp.CurrNB()
	globalBefore := GlobalStats().NumCurrBytes.Load()

	EnableProfiling()
	bs, err := mp.Alloc(2048, true)
	require.NoError(t, err)
	live = bs
	require.Equal(t, trackedBefore+1, ProfileTrackedCount())
	oldPtr := reflect.ValueOf(bs).Pointer()
	recorded, found := lookupProfileRecord(oldPtr)
	require.True(t, found)
	bytesInuse := recorded.Bytes.Inuse.Load()
	objectsInuse := recorded.Objects.Inuse.Load()

	DisableProfiling()
	require.False(t, ProfilingEnabled())
	next, err := mp.ReallocZero(bs, 12288, true)
	require.NoError(t, err)
	live = next // the native realloc consumed the old block; no re-free of bs

	// The old identity must retire and the disabled switch must gate any new
	// sampling for the replacement pointer. The tracked population is asserted
	// FIRST: whether the native realloc moved or grew in place, only a retired
	// old identity plus a gated new allocation leaves it unchanged, so this is
	// the deterministic retirement oracle. Assertion order only - every check
	// below still runs, none dropped.
	require.Equal(t, trackedBefore, ProfileTrackedCount(),
		"successful realloc must retire the old identity without adding one")
	newPtr := reflect.ValueOf(next).Pointer()
	_, resampled := lookupProfileRecord(newPtr)
	require.False(t, resampled,
		"realloc resampling must stay gated while profiling is off")
	_, oldStillTracked := lookupProfileRecord(oldPtr)
	require.False(t, oldStillTracked,
		"successful realloc must retire the address-keyed identity, in place or moved")
	require.Equal(t, bytesInuse-2048, recorded.Bytes.Inuse.Load())
	require.Equal(t, objectsInuse-1, recorded.Objects.Inuse.Load())
	require.Equal(t, poolBefore+12288, mp.CurrNB())
	require.Equal(t, globalBefore+12288, GlobalStats().NumCurrBytes.Load())

	// The replacement was never sampled: its Free must stay neutral and leave
	// no residue behind, whether the store is empty or not.
	mp.Free(next)
	live = nil
	require.Equal(t, trackedBefore, ProfileTrackedCount())
	require.Equal(t, poolBefore, mp.CurrNB())
	require.Equal(t, globalBefore, GlobalStats().NumCurrBytes.Load())
}

func TestProfileTeardownAfterDisableRetiresPlainRecords(t *testing.T) {
	restoreProfilingSwitch(t, ProfilingEnabled())
	trackedBefore := ProfileTrackedCount()

	DisableProfiling()
	EnableProfiling()
	mp := MustNewNoLock("profile-teardown-after-disable")
	// Single lifecycle: the pool is deleted exactly once, by the test's
	// explicit teardown assertion run or by this FailNow cleanup.
	deleted := false
	t.Cleanup(func() {
		if !deleted {
			DeleteMPool(mp)
		}
	})

	bs, err := mp.Alloc(128, true)
	require.NoError(t, err)
	require.Equal(t, trackedBefore+1, ProfileTrackedCount())
	ptr := reflect.ValueOf(bs).Pointer()
	recorded, found := lookupProfileRecord(ptr)
	require.True(t, found)
	bytesInuse := recorded.Bytes.Inuse.Load()
	objectsInuse := recorded.Objects.Inuse.Load()

	// Pool teardown, like an ordinary Free, must retire a plain off-heap
	// record even when profiling is disabled meanwhile.
	DisableProfiling()
	require.False(t, ProfilingEnabled())
	DeleteMPool(mp)
	deleted = true

	require.Equal(t, trackedBefore, ProfileTrackedCount(),
		"teardown after disable must retire remaining plain records")
	_, stillTracked := lookupProfileRecord(ptr)
	require.False(t, stillTracked)
	require.Equal(t, bytesInuse-128, recorded.Bytes.Inuse.Load())
	require.Equal(t, objectsInuse-1, recorded.Objects.Inuse.Load())
}

// BenchmarkProfilePlainOffHeapAlwaysOff measures the plain off-heap retire
// attempt when profiling was never enabled: the Free path must stay at one
// atomic read, not a shard-lock probe. It pairs with the enabled-mode
// BenchmarkProfileAllocFree as the bounded hot-path cost control for the
// empty-store fast path.
func BenchmarkProfilePlainOffHeapAlwaysOff(b *testing.B) {
	// Restore the original switch instead of leaving it disabled: the shared
	// benchmark suite continues with its own enabled/disabled expectations.
	wasEnabled := ProfilingEnabled()
	defer func() {
		if wasEnabled {
			EnableProfiling()
		} else {
			DisableProfiling()
		}
	}()
	DisableProfiling()
	mp := MustNew("profile-benchmark-always-off")
	defer DeleteMPool(mp)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buffer, err := mp.Alloc(64, true)
		if err != nil {
			b.Fatal(err)
		}
		mp.Free(buffer)
	}
}
