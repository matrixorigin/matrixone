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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// minSubIndexRows is the smallest row count planCapacity accepts for one sub-index.
func TestMinSubIndexRows(t *testing.T) {
	require.Equal(t, int64(17), minSubIndexRows(17, false, 4))
	require.Equal(t, int64(17), minSubIndexRows(17, true, 1))
	for _, threshold := range []int64{1, 4, 17, 32, 33, 1024} {
		for _, shards := range []int{2, 3, 4} {
			n := minSubIndexRows(threshold, true, shards)
			_, err := planCapacity(n, 0, 0, 0, threshold, true, shards, "x", "c")
			require.NoError(t, err, "threshold %d shards %d rows %d", threshold, shards, n)
			_, err = planCapacity(n-1, 0, 0, 0, threshold, true, shards, "x", "c")
			require.Error(t, err, "threshold %d shards %d rows %d", threshold, shards, n-1)
		}
	}
}

func TestReserveHeldRows(t *testing.T) {
	// not measured, or nothing held: unchanged
	got, err := reserveHeldRows(0, 10, 100, 10, "x")
	require.NoError(t, err)
	require.Zero(t, got)
	got, err = reserveHeldRows(1000, 0, 100, 10, "x")
	require.NoError(t, err)
	require.Equal(t, int64(1000), got)
	// 10 held rows of 100 bytes take 100 rows of 10 bytes, 101 of 9
	got, err = reserveHeldRows(1000, 10, 100, 10, "x")
	require.NoError(t, err)
	require.Equal(t, int64(900), got)
	got, err = reserveHeldRows(1000, 10, 100, 9, "x")
	require.NoError(t, err)
	require.Equal(t, int64(888), got)
	// the held rows alone exceed the memory
	_, err = reserveHeldRows(100, 10, 100, 10, "x")
	require.Error(t, err)
}

func TestIgnoredBuild(t *testing.T) {
	require.Nil(t, newIgnoredBuild(vectorindex.IndexTableConfig{}, ""))
	(*ignoredBuild)(nil).close()

	g := newIgnoredBuild(vectorindex.IndexTableConfig{IgnoredTable: "ign", MaxBadRecords: 1}, t.TempDir())
	defer g.close()
	// an error that is not about the row's value is returned as is
	other := moerr.NewInternalErrorNoCtx("io")
	require.Equal(t, other, g.leaveOut("1", other))
	require.Zero(t, g.left)

	bad := moerr.NewIndexVectorOutOfRangeNoCtx("x")
	require.NoError(t, g.leaveOut("1", bad))
	require.Error(t, g.allBad())
	g.stored()
	require.NoError(t, g.allBad())
	err := g.leaveOut("2", bad)
	code, _ := moerr.GetMoErrCode(err)
	require.Equal(t, moerr.ErrIndexTooManyBadRecords, code)
}
