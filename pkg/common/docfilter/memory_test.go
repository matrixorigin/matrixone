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

package docfilter

import (
	"errors"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type recordingMemoryAdmission struct {
	mu       sync.Mutex
	grant    bool
	acquired []int64
	released []int64
}

type refreshingMemoryAdmission struct {
	recordingMemoryAdmission
	refreshes int
	order     []string
}

func (a *refreshingMemoryAdmission) ShouldRefreshBeforeRelease() bool {
	return true
}

func (a *refreshingMemoryAdmission) ForceRefresh() {
	a.refreshes++
	a.order = append(a.order, "refresh")
}

func (a *refreshingMemoryAdmission) Release(bytes int64) int64 {
	a.order = append(a.order, "release")
	return a.recordingMemoryAdmission.Release(bytes)
}

func (a *recordingMemoryAdmission) Acquire(bytes int64) (int64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acquired = append(a.acquired, bytes)
	return 0, a.grant
}

func (a *recordingMemoryAdmission) Release(bytes int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.released = append(a.released, bytes)
	return 0
}

func TestBuildMemoryAdmissionAtAllocationSite(t *testing.T) {
	mp := mpool.MustNewZero()

	for _, tc := range []struct {
		name string
		vec  func() *vector.Vector
		want func(*vector.Vector) int64
	}{
		{
			name: "dense integer",
			vec: func() *vector.Vector {
				return buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4}, nil)
			},
			want: func(v *vector.Vector) int64 {
				return cbitmapBuildPeakUpperBound(4)
			},
		},
		{
			name: "sparse integer",
			vec: func() *vector.Vector {
				return buildIntVec(t, mp, types.T_int64.ToType(), []int64{0, 1 << 30}, nil)
			},
			want: func(v *vector.Vector) int64 {
				sortedBytes, ok := sorted64BuildPeakUpperBound(v)
				require.True(t, ok)
				return sortedBytes
			},
		},
		{
			name: "varchar bloom",
			vec: func() *vector.Vector {
				return buildVarcharVec(t, mp, []string{"alpha", "beta", "gamma"})
			},
			want: func(v *vector.Vector) int64 {
				bytes, ok := bloomfilter.EstimateCBloomFilterMemoryBytes(
					int64(v.Length()), bloomFpProbability)
				require.True(t, ok)
				return bytes*2 + 1
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.vec()
			defer v.Free(mp)
			admission := &recordingMemoryAdmission{grant: true}
			payload, err := BuildWithMemoryAdmission(v, admission)
			require.NoError(t, err)
			require.NotEmpty(t, payload)
			want := tc.want(v)
			require.Equal(t, []int64{want}, admission.acquired)
			require.Equal(t, []int64{want}, admission.released)
		})
	}
}

func TestBuildMemoryAdmissionDenialFailsClosed(t *testing.T) {
	mp := mpool.MustNewZero()
	v := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4}, nil)
	defer v.Free(mp)
	admission := &recordingMemoryAdmission{grant: false}

	payload, err := BuildWithMemoryAdmission(v, admission)
	require.Nil(t, payload)
	var admissionErr *MemoryAdmissionError
	require.True(t, errors.As(err, &admissionErr))
	require.Positive(t, admissionErr.Requested)
	require.Empty(t, admission.released)
}

func TestBuildRefreshesPersistentPayloadBeforeReleasingAdmission(t *testing.T) {
	mp := mpool.MustNewZero()
	v := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3}, nil)
	defer v.Free(mp)
	admission := &refreshingMemoryAdmission{
		recordingMemoryAdmission: recordingMemoryAdmission{grant: true},
	}

	payload, err := BuildWithMemoryAdmission(v, admission)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.Equal(t, 1, admission.refreshes)
	require.Equal(t, []string{"refresh", "release"}, admission.order)
}

func TestNewMemoryLeaseFollowsLastReader(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, tc := range []struct {
		name string
		vec  func() *vector.Vector
	}{
		{
			name: "cbitmap",
			vec: func() *vector.Vector {
				return buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4}, nil)
			},
		},
		{
			name: "bloom",
			vec: func() *vector.Vector {
				return buildVarcharVec(t, mp, []string{"alpha", "beta"})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.vec()
			defer v.Free(mp)
			payload, err := Build(v)
			require.NoError(t, err)
			admission := &recordingMemoryAdmission{grant: true}
			f, err := NewWithMemoryAdmission(payload, admission)
			require.NoError(t, err)
			require.Len(t, admission.acquired, 1)

			shared := f.Share()
			f.Free()
			require.Empty(t, admission.released)
			shared.Free()
			require.Equal(t, admission.acquired, admission.released)
		})
	}
}

func TestNewMemoryLeaseRefreshesBeforeLastRelease(t *testing.T) {
	mp := mpool.MustNewZero()

	for _, tc := range []struct {
		name string
		vec  func() *vector.Vector
	}{
		{
			name: "cbitmap",
			vec: func() *vector.Vector {
				return buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4}, nil)
			},
		},
		{
			name: "sorted64",
			vec: func() *vector.Vector {
				return buildIntVec(t, mp, types.T_int64.ToType(), []int64{0, 1 << 30}, nil)
			},
		},
		{
			name: "bloom",
			vec: func() *vector.Vector {
				return buildVarcharVec(t, mp, []string{"alpha", "beta"})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := tc.vec()
			defer v.Free(mp)
			payload, err := Build(v)
			require.NoError(t, err)

			admission := &refreshingMemoryAdmission{
				recordingMemoryAdmission: recordingMemoryAdmission{grant: true},
			}
			filter, err := NewWithMemoryAdmission(payload, admission)
			require.NoError(t, err)
			shared := filter.Share()

			filter.Free()
			require.Empty(t, admission.order,
				"a shared filter must retain its single consumer lease")
			shared.Free()
			require.Equal(t, []string{"refresh", "release"}, admission.order)
			require.Equal(t, 1, admission.refreshes)
			require.Equal(t, admission.acquired, admission.released)
		})
	}
}

func TestReconstructAllocationCoversWireAndLiveFilter(t *testing.T) {
	const payloadBytes = 128
	payload := make([]byte, payloadBytes)

	for _, tc := range []struct {
		name string
		tag  byte
		want int64
	}{
		{name: "cbitmap", tag: TagCbitmap, want: 2*payloadBytes + 8},
		{name: "sorted64", tag: TagSorted64, want: payloadBytes},
		{name: "bloom", tag: TagBloom, want: 2 * payloadBytes},
		{name: "legacy croaring", tag: TagCRoaring, want: 32*payloadBytes + 64<<10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := reconstructAllocationBytes(tc.tag, payload)
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNewSorted64ChargesConsumerForPayloadBytes(t *testing.T) {
	mp := mpool.MustNewZero()
	v := buildIntVec(t, mp, types.T_int64.ToType(), []int64{0, 1 << 30}, nil)
	defer v.Free(mp)
	payload, err := Build(v)
	require.NoError(t, err)
	require.Equal(t, TagSorted64, payload[0])

	admission := &recordingMemoryAdmission{grant: true}
	f, err := NewWithMemoryAdmission(payload, admission)
	require.NoError(t, err)
	require.Len(t, admission.acquired, 1)
	require.Equal(t, int64(len(payload)-1), admission.acquired[0],
		"Sored64 consumer must charge the payload size")
	f.Free()
}

func TestLegacyCRoaringMemoryLeaseFollowsLastReader(t *testing.T) {
	mp := mpool.MustNewZero()
	v := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 1 << 30}, nil)
	defer v.Free(mp)
	payload, err := BuildCRoaringBytes(v)
	require.NoError(t, err)
	payload = append([]byte{TagCRoaring}, payload...)
	admission := &recordingMemoryAdmission{grant: true}

	f, err := NewWithMemoryAdmission(payload, admission)
	require.NoError(t, err)
	require.Len(t, admission.acquired, 1)
	shared := f.Share()
	f.Free()
	require.Empty(t, admission.released)
	shared.Free()
	require.Equal(t, admission.acquired, admission.released)
}

func TestNewDecodeFailureReleasesAdmission(t *testing.T) {
	admission := &recordingMemoryAdmission{grant: true}
	f, err := NewWithMemoryAdmission(
		[]byte{TagCbitmap, 1, 2, 3}, admission)
	require.Error(t, err)
	require.Nil(t, f)
	require.Equal(t, admission.acquired, admission.released)
}

func TestMemoryAdmissionUsesProductionParallelPolicy(t *testing.T) {
	const (
		request    = int64(1 << 20)
		limitSlots = int64(10)
		// The production CN policy's hard cap is 80% of the pool.
		grantedSlots = int64(8)
		workers      = 100
	)
	admission := rscthrottler.NewMemThrottler(
		"docfilter-parallel-test",
		1,
		rscthrottler.WithConstLimit(request*limitSlots),
		rscthrottler.WithAcquirePolicy(
			rscthrottler.AcquirePolicyForCNFlushS3),
	)

	start := make(chan struct{})
	releaseAll := make(chan struct{})
	results := make(chan bool, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, err := acquireMemory(admission, request)
			if err != nil {
				results <- false
				return
			}
			results <- true
			<-releaseAll
			release()
		}()
	}
	close(start)
	granted := 0
	for range workers {
		if <-results {
			granted++
		}
	}
	require.Equal(t, int(grantedSlots), granted)
	close(releaseAll)
	wg.Wait()
	require.Equal(t, request*limitSlots, admission.Available())
}
