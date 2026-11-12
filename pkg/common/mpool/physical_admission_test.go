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

package mpool

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPhysicalAdmissionGrowthAndLifetime(t *testing.T) {
	mp := MustNew("physical-admission-owner")
	other := MustNew("physical-admission-cross-free")
	var data []byte
	t.Cleanup(func() {
		if data != nil {
			mp.Free(data)
		}
		DeleteMPool(other)
		DeleteMPool(mp)
	})
	want := errors.New("budget")
	var live, limit uint64
	limit = 64
	admit := func(n uint64) (func(), error) {
		if n > limit-live {
			return nil, want
		}
		live += n
		return func() { live -= n }, nil
	}
	var err error
	data, err = mp.AllocAdmitted(64, admit)
	require.NoError(t, err)
	data[0] = 42
	runtime.GC() // the registry is the allocation token's remaining owner
	require.EqualValues(t, 64, live)
	_, err = mp.Grow(data, 128, true)
	require.ErrorIs(t, err, want)
	require.EqualValues(t, 64, live)
	require.EqualValues(t, 42, data[0])
	limit = 512
	next, err := mp.Grow(data, 128, true)
	require.NoError(t, err)
	data = next
	require.EqualValues(t, 42, data[0])
	require.EqualValues(t, cap(data), live)
	oldLen := len(data)
	next, err = mp.ReallocZero(data, 256, true)
	require.NoError(t, err)
	data = next
	require.EqualValues(t, 42, data[0])
	require.Zero(t, data[oldLen])
	require.EqualValues(t, 256, live)
	other.Free(data)
	data = nil
	require.Zero(t, live)
	require.Zero(t, mp.CurrNB())
}

func TestPhysicalAdmissionFailuresAndTeardown(t *testing.T) {
	want := errors.New("admission")
	mp := MustNewNoLock("physical-admission-teardown")
	t.Cleanup(func() {
		if mp != nil {
			DeleteMPool(mp)
		}
	})
	var live int
	_, err := mp.AllocAdmitted(64, func(uint64) (func(), error) {
		live++
		return func() { live-- }, want
	})
	require.ErrorIs(t, err, want)
	require.Zero(t, live)
	_, err = mp.AllocAdmitted(64, func(uint64) (func(), error) { return nil, nil })
	require.Error(t, err)
	_, err = mp.AllocAdmitted(128, func(n uint64) (func(), error) {
		live += int(n)
		return func() { live -= int(n) }, nil
	})
	require.NoError(t, err)
	require.Equal(t, 128, live)
	DeleteMPool(mp)
	mp = nil
	require.Zero(t, live)

	limited, err := NewMPool("physical-admission-pool-limit", 1<<20, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(limited)
	_, err = limited.AllocAdmitted(2<<20, func(n uint64) (func(), error) {
		live += int(n)
		return func() { live -= int(n) }, nil
	})
	require.Error(t, err)
	require.Zero(t, live)
}

func TestPhysicalAdmissionBufferAndPinTransfer(t *testing.T) {
	mp := MustNew("physical-admission-buffer")
	defer DeleteMPool(mp)
	var live uint64
	admit := func(n uint64) (func(), error) {
		live += n
		return func() { live -= n }, nil
	}
	pin, err := NewAdmittedCapacityLease(10, admit)
	require.NoError(t, err)
	defer pin.Release()
	buffer, err := NewAdmittedBuffer(mp, admit)
	require.NoError(t, err)
	defer buffer.Free()
	_, err = buffer.WriteString("content")
	require.NoError(t, err)
	data, owner, err := buffer.Detach()
	require.NoError(t, err)
	defer func() {
		if data != nil {
			owner.Free(data)
		}
	}()
	buffer.Free()
	require.Equal(t, "content", string(data))
	require.GreaterOrEqual(t, live, uint64(cap(data)+10))
	pin.Release()
	pin.Release()
	owner.Free(data)
	data = nil
	require.Zero(t, live)
}

func TestPhysicalAdmissionProfilingRetiresOnRelease(t *testing.T) {
	restoreProfilingSwitch(t, ProfilingEnabled())
	for _, noLock := range []bool{false, true} {
		t.Run(fmt.Sprintf("noLock=%t", noLock), func(t *testing.T) {
			DisableProfiling()
			before := ProfileTrackedCount()
			var mp *MPool
			if noLock {
				mp = MustNewNoLock("physical-admission-profile-nolock")
			} else {
				mp = MustNew("physical-admission-profile")
			}
			var data []byte
			t.Cleanup(func() {
				if mp != nil {
					if !noLock && data != nil {
						mp.Free(data)
					}
					DeleteMPool(mp)
				}
			})
			var live uint64
			admit := func(n uint64) (func(), error) {
				live += n
				return func() { live -= n }, nil
			}
			EnableProfiling()
			var err error
			data, err = mp.AllocAdmitted(128, admit)
			require.NoError(t, err)
			require.Equal(t, before+1, ProfileTrackedCount())
			DisableProfiling()
			if noLock {
				DeleteMPool(mp)
			} else {
				mp.Free(data)
				data = nil
				DeleteMPool(mp)
			}
			mp = nil
			require.Equal(t, before, ProfileTrackedCount())
			require.Zero(t, live)
		})
	}
}

func TestPhysicalAdmissionIndependentConcurrentAllocations(t *testing.T) {
	mp := MustNew("physical-admission-concurrent")
	defer DeleteMPool(mp)
	var live atomic.Int64
	admit := func(n uint64) (func(), error) {
		live.Add(int64(n))
		return func() { live.Add(-int64(n)) }, nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				data, err := mp.AllocAdmitted(64, admit)
				if err != nil {
					errs <- err
					return
				}
				next, err := mp.Grow(data, 128, true)
				if err != nil {
					mp.Free(data)
					errs <- err
					return
				}
				mp.Free(next)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, live.Load())
	require.Zero(t, mp.CurrNB())
}
