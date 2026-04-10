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

package frontend

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func normalizeSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

func Test_getSqlForAccountInfo(t *testing.T) {
	type arg struct {
		name            string
		s               string
		accID           int64
		needObjectCount bool
		wantContains    []string
		wantNotContains []string
	}
	args := []arg{
		{
			name:  "all accounts",
			s:     "show accounts;",
			accID: -1,
			wantContains: []string{
				"WITH db_counts AS (",
				"COUNT(DISTINCT md.dat_id) AS db_count",
				"tbl_counts AS (",
				"COUNT(DISTINCT mt.rel_id) AS tbl_count",
				"JOIN db_counts ON ma.account_id = db_counts.account_id",
				"JOIN tbl_counts ON ma.account_id = tbl_counts.account_id",
			},
			wantNotContains: []string{
				"mo_catalog.mo_tables AS mt JOIN mo_catalog.mo_database AS md",
				"db_tbl_counts",
				"LEFT JOIN",
			},
		},
		{
			name:  "like filter",
			s:     "show accounts like '%abc';",
			accID: -1,
			wantContains: []string{
				"where ma.account_name like '%abc'",
			},
		},
		{
			name:  "like filter with quote",
			s:     "show accounts like 'ab''cd';",
			accID: -1,
			wantContains: []string{
				"where ma.account_name like 'ab''cd'",
			},
		},
		{
			name:  "like filter with backslash",
			s:     "show accounts like 'ab\\_cd';",
			accID: -1,
			wantContains: []string{
				"where ma.account_name like 'ab\\\\_cd'",
			},
		},
		{
			name:  "exact account filter",
			s:     "show accounts;",
			accID: 100,
			wantContains: []string{
				"WHERE md.account_id = 100",
				"AND mt.account_id = 100",
				"where ma.account_id = 100",
			},
		},
		{
			name:  "like and exact account filter",
			s:     "show accounts like '%abc';",
			accID: 100,
			wantContains: []string{
				"where ma.account_name like '%abc' and ma.account_id = 100",
				"WHERE md.account_id = 100",
				"AND mt.account_id = 100",
			},
		},
		{
			name:            "object count",
			s:               "show accounts;",
			accID:           -1,
			needObjectCount: true,
			wantContains: []string{
				"CAST(0 AS BIGINT) AS object_count",
			},
		},
		{
			name:            "object count with exact account filter",
			s:               "show accounts;",
			accID:           100,
			needObjectCount: true,
			wantContains: []string{
				"CAST(0 AS BIGINT) AS object_count",
				"WHERE md.account_id = 100",
				"AND mt.account_id = 100",
				"where ma.account_id = 100",
			},
		},
	}

	for _, a := range args {
		t.Run(a.name, func(t *testing.T) {
			one, err := parsers.ParseOne(context.Background(), dialect.MYSQL, a.s, 1)
			require.NoError(t, err)
			sa1 := one.(*tree.ShowAccounts)
			got := normalizeSQL(getSqlForAccountInfo(sa1.Like, a.accID, a.needObjectCount))
			for _, fragment := range a.wantContains {
				assert.Contains(t, got, normalizeSQL(fragment))
			}
			for _, fragment := range a.wantNotContains {
				assert.NotContains(t, got, normalizeSQL(fragment))
			}
			_, err = parsers.ParseOne(context.Background(), dialect.MYSQL, got, 1)
			require.NoError(t, err)
		})
	}
}

func Test_updateStorageSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc(t)

	size := uint64(9999 * 1024 * 1024)
	bat := batch.Batch{}
	bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_float64.ToType()))
	vector.AppendFixed[float64](bat.Vecs[0], float64(0x00), false, proc.GetMPool())
	updateStorageSize(bat.Vecs[0], uint64(size), 0)
	require.Equal(t, float64(size)/1024/1024, vector.GetFixedAtWithTypeCheck[float64](bat.Vecs[0], 0))
}

func Test_updateCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	proc := testutil.NewProc(t)

	ori := int64(0x12)
	add := int64(0x12)
	bat := batch.Batch{}
	bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_int64.ToType()))
	vector.AppendFixed[int64](bat.Vecs[0], ori, false, proc.GetMPool())
	updateCount(bat.Vecs[0], add, 0)
	require.Equal(t, ori+add, vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0))
}

func Test_updateStorageUsageCache_V2(t *testing.T) {
	rep := cmd_util.StorageUsageResp_V2{}

	for i := 0; i < 10; i++ {
		rep.AccIds = append(rep.AccIds, int64(i))
		rep.Sizes = append(rep.Sizes, rand.Uint64())
		rep.ObjCnts = append(rep.ObjCnts, rand.Uint64())
		rep.BlkCnts = append(rep.BlkCnts, rand.Uint64())
		rep.RowCnts = append(rep.RowCnts, rand.Uint64())
	}

	updateStorageUsageCache_V2(&rep)

	usages := cnUsageCache.GatherAllAccSize()
	for i := 0; i < len(rep.AccIds); i++ {
		require.Equal(t, rep.Sizes[i], usages[uint64(i)][0])
	}
}

func Test_updateStorageUsageCache(t *testing.T) {
	rep := cmd_util.StorageUsageResp_V3{}

	for i := 0; i < 10; i++ {
		rep.AccIds = append(rep.AccIds, int64(i))
		rep.Sizes = append(rep.Sizes, rand.Uint64())
		rep.SnapshotSizes = append(rep.SnapshotSizes, rand.Uint64())
		rep.ObjCnts = append(rep.ObjCnts, rand.Uint64())
		rep.BlkCnts = append(rep.BlkCnts, rand.Uint64())
		rep.RowCnts = append(rep.RowCnts, rand.Uint64())
	}

	updateStorageUsageCache(&rep)

	usages := cnUsageCache.GatherAllAccSize()
	for i := 0; i < len(rep.AccIds); i++ {
		require.Equal(t, rep.Sizes[i], usages[uint64(i)][0])
		require.Equal(t, rep.SnapshotSizes[i], usages[uint64(i)][1])
	}
}

func Test_checkStorageUsageCache_V2(t *testing.T) {
	origCache := cnUsageCache
	cnUsageCache = logtail.NewStorageUsageCache(logtail.WithLazyThreshold(1))
	cnUsageCache = logtail.NewStorageUsageCache(logtail.WithLazyThreshold(1))
	t.Cleanup(func() {
		cnUsageCache = origCache
	})

	rep := cmd_util.StorageUsageResp_V2{}

	for i := 0; i < 10; i++ {
		rep.AccIds = append(rep.AccIds, int64(i))
		rep.Sizes = append(rep.Sizes, rand.Uint64())
		rep.ObjCnts = append(rep.ObjCnts, rand.Uint64())
		rep.BlkCnts = append(rep.BlkCnts, rand.Uint64())
		rep.RowCnts = append(rep.RowCnts, rand.Uint64())
	}

	updateStorageUsageCache_V2(&rep)

	usages, ok := checkStorageUsageCache([][]int64{rep.AccIds})
	require.True(t, ok)
	for i := 0; i < len(rep.AccIds); i++ {
		require.Equal(t, rep.Sizes[i], usages[int64(i)][0])
	}

	require.Eventually(t, func() bool {
		_, ok = checkStorageUsageCache([][]int64{rep.AccIds})
		return !ok
	}, time.Second+200*time.Millisecond, 10*time.Millisecond)
}

func Test_checkStorageUsageCache(t *testing.T) {
	origCache := cnUsageCache
	cnUsageCache = logtail.NewStorageUsageCache(logtail.WithLazyThreshold(1))
	cnUsageCache = logtail.NewStorageUsageCache(logtail.WithLazyThreshold(1))
	t.Cleanup(func() {
		cnUsageCache = origCache
	})

	rep := cmd_util.StorageUsageResp_V3{}

	for i := 0; i < 10; i++ {
		rep.AccIds = append(rep.AccIds, int64(i))
		rep.Sizes = append(rep.Sizes, rand.Uint64())
		rep.SnapshotSizes = append(rep.SnapshotSizes, rand.Uint64())
		rep.ObjCnts = append(rep.ObjCnts, rand.Uint64())
		rep.BlkCnts = append(rep.BlkCnts, rand.Uint64())
		rep.RowCnts = append(rep.RowCnts, rand.Uint64())
	}

	updateStorageUsageCache(&rep)

	usages, ok := checkStorageUsageCache([][]int64{rep.AccIds})
	require.True(t, ok)
	for i := 0; i < len(rep.AccIds); i++ {
		require.Equal(t, rep.Sizes[i], usages[int64(i)][0])
		require.Equal(t, rep.SnapshotSizes[i], usages[int64(i)][1])
	}

	require.Eventually(t, func() bool {
		_, ok = checkStorageUsageCache([][]int64{rep.AccIds})
		return !ok
	}, time.Second+200*time.Millisecond, 10*time.Millisecond)
}

func Test_GetObjectCount_V2(t *testing.T) {
	rep := cmd_util.StorageUsageResp_V2{}

	for i := 0; i < 10; i++ {
		rep.AccIds = append(rep.AccIds, int64(i))
		rep.Sizes = append(rep.Sizes, rand.Uint64())
		rep.ObjCnts = append(rep.ObjCnts, rand.Uint64())
		rep.BlkCnts = append(rep.BlkCnts, rand.Uint64())
		rep.RowCnts = append(rep.RowCnts, rand.Uint64())
	}

	updateStorageUsageCache_V2(&rep)

	abstract := cnUsageCache.GatherObjectAbstractForAccounts()
	for i := 0; i < len(rep.AccIds); i++ {
		require.Equal(t, int(rep.ObjCnts[i]), abstract[uint64(rep.AccIds[i])].TotalObjCnt)
	}
}

func Test_GetObjectCount(t *testing.T) {
	rep := cmd_util.StorageUsageResp_V3{}

	for i := 0; i < 10; i++ {
		rep.AccIds = append(rep.AccIds, int64(i))
		rep.Sizes = append(rep.Sizes, rand.Uint64())
		rep.SnapshotSizes = append(rep.SnapshotSizes, rand.Uint64())
		rep.ObjCnts = append(rep.ObjCnts, rand.Uint64())
		rep.BlkCnts = append(rep.BlkCnts, rand.Uint64())
		rep.RowCnts = append(rep.RowCnts, rand.Uint64())
	}

	updateStorageUsageCache(&rep)

	abstract := cnUsageCache.GatherObjectAbstractForAccounts()
	for i := 0; i < len(rep.AccIds); i++ {
		require.Equal(t, int(rep.ObjCnts[i]), abstract[uint64(rep.AccIds[i])].TotalObjCnt)
	}
}
