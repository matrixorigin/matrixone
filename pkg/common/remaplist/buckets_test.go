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

package remaplist

import (
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

// checkBuckets asserts that bucket b of rows/offs holds exactly the rows whose
// id is b, in ascending order.
func checkBuckets(t *testing.T, ids []uint64, bucket func(int) []int64, buckets int) {
	t.Helper()
	seen := 0
	for b := range buckets {
		var want []int64
		for row, id := range ids {
			if id == uint64(b) {
				want = append(want, int64(row))
			}
		}
		got := bucket(b)
		if len(want) == 0 {
			require.Empty(t, got, "bucket %d", b)
		} else {
			require.Equal(t, want, got, "bucket %d", b)
		}
		seen += len(got)
	}
	require.Equal(t, len(ids), seen)
}

func TestSplitBuckets(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for _, buckets := range []int{1, 2, 3, 32, 1000} {
		for _, n := range []int{0, 1, 17, 8192} {
			ids := make([]uint64, n)
			for i := range ids {
				ids[i] = uint64(r.Intn(buckets))
			}
			// Leave the last bucket empty sometimes, and leave stale data in rows and offs.
			if buckets > 1 && n > 0 {
				for i := range ids {
					if ids[i] == uint64(buckets-1) {
						ids[i] = 0
					}
				}
			}
			rows := make([]int64, n+3)
			for i := range rows {
				rows[i] = -9
			}
			offs := make([]int64, buckets+1)
			for i := range offs {
				offs[i] = 99
			}
			require.NoError(t, SplitBuckets(ids, rows, offs))
			require.Equal(t, int64(0), offs[0])
			require.Equal(t, int64(n), offs[buckets])
			checkBuckets(t, ids, func(b int) []int64 { return rows[offs[b]:offs[b+1]] }, buckets)
		}
	}
}

func TestSplitBucketsErrors(t *testing.T) {
	require.Error(t, SplitBuckets([]uint64{0}, make([]int64, 1), nil))
	require.Error(t, SplitBuckets([]uint64{0, 0}, make([]int64, 1), make([]int64, 2)))
	require.Error(t, SplitBuckets([]uint64{0, 2}, make([]int64, 2), make([]int64, 3)))
}

func TestListBuildBuckets(t *testing.T) {
	mp := mpool.MustNewZero()
	var r List
	require.NoError(t, r.Init(mp, 1))
	require.Equal(t, 0, r.Buckets())

	ids := []uint64{2, 0, 2, 1, 0, 2}
	require.NoError(t, r.BuildBuckets(ids, 3))
	require.Equal(t, 3, r.Buckets())
	require.Equal(t, len(ids), r.Len())
	require.Equal(t, []int64{1, 4}, r.Bucket(0))
	require.Equal(t, []int64{3}, r.Bucket(1))
	require.Equal(t, []int64{0, 2, 5}, r.Bucket(2))

	// A second, wider split reuses the List and grows the offsets.
	ids = []uint64{4, 3}
	require.NoError(t, r.BuildBuckets(ids, 5))
	require.Equal(t, 5, r.Buckets())
	checkBuckets(t, ids, r.Bucket, 5)

	// A failed split leaves an empty List.
	require.Error(t, r.BuildBuckets([]uint64{5}, 5))
	require.Equal(t, 0, r.Len())
	require.Equal(t, 0, r.Buckets())
	require.Error(t, r.BuildBuckets(nil, 0))

	// Identity and Reset drop the buckets.
	require.NoError(t, r.BuildBuckets([]uint64{0}, 1))
	require.NoError(t, r.Identity(2))
	require.Equal(t, 0, r.Buckets())
	require.NoError(t, r.BuildBuckets([]uint64{0}, 1))
	r.Reset()
	require.Equal(t, 0, r.Buckets())

	r.Free()
	r.Free()
	require.Equal(t, int64(0), mp.CurrNB())

	var unset List
	require.Error(t, unset.BuildBuckets([]uint64{0}, 1))
	require.Error(t, unset.BuildBuckets(nil, 1))
}
