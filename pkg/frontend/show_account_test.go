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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func Test_getSqlForAccountInfo(t *testing.T) {
	type arg struct {
		s    string
		want string
	}
	args := []arg{
		{
			s:    "show accounts;",
			want: "WITH db_tbl_counts AS (\tSELECT\t\tCAST(mt.account_id AS BIGINT) AS account_id,\t\tCOUNT(DISTINCT md.dat_id) AS db_count,\t\tCOUNT(DISTINCT mt.rel_id) AS tbl_count\tFROM\t\tmo_catalog.mo_tables AS mt\tJOIN\t\tmo_catalog.mo_database AS md\tON \t\tmt.account_id = md.account_id AND\t\tmt.relkind IN ('v','e','r','cluster') AND\t\tmd.dat_type != 'subscription'\tGROUP BY\t\tmt.account_id),final_result AS (\tSELECT\t\tCAST(ma.account_id AS BIGINT) AS account_id,\t\tma.account_name,\t\tma.admin_name,\t\tma.created_time,\t\tma.status,\t\tma.suspended_time,\t\tdb_tbl_counts.db_count,\t\tdb_tbl_counts.tbl_count,\t\tCAST(0 AS DOUBLE) AS size,\t\tma.comments\tFROM\t\tdb_tbl_counts\tJOIN\t\tmo_catalog.mo_account AS ma \tON \t\tdb_tbl_counts.account_id = ma.account_id \t\t   )SELECT * FROM final_result;",
		},
		{
			s:    "show accounts like '%abc';",
			want: "WITH db_tbl_counts AS (\tSELECT\t\tCAST(mt.account_id AS BIGINT) AS account_id,\t\tCOUNT(DISTINCT md.dat_id) AS db_count,\t\tCOUNT(DISTINCT mt.rel_id) AS tbl_count\tFROM\t\tmo_catalog.mo_tables AS mt\tJOIN\t\tmo_catalog.mo_database AS md\tON \t\tmt.account_id = md.account_id AND\t\tmt.relkind IN ('v','e','r','cluster') AND\t\tmd.dat_type != 'subscription'\tGROUP BY\t\tmt.account_id),final_result AS (\tSELECT\t\tCAST(ma.account_id AS BIGINT) AS account_id,\t\tma.account_name,\t\tma.admin_name,\t\tma.created_time,\t\tma.status,\t\tma.suspended_time,\t\tdb_tbl_counts.db_count,\t\tdb_tbl_counts.tbl_count,\t\tCAST(0 AS DOUBLE) AS size,\t\tma.comments\tFROM\t\tdb_tbl_counts\tJOIN\t\tmo_catalog.mo_account AS ma \tON \t\tdb_tbl_counts.account_id = ma.account_id \t\twhere ma.account_name like '%abc'  )SELECT * FROM final_result;",
		},
	}

	for _, a := range args {
		one, err := parsers.ParseOne(context.Background(), dialect.MYSQL, a.s, 1, 0)
		assert.NoError(t, err)
		sa1 := one.(*tree.ShowAccounts)
		r1 := getSqlForAccountInfo(sa1.Like, -1)
		assert.Equal(t, a.want, r1)
	}
}

func Test_updateStorageSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	size := uint64(9999 * 1024 * 1024)
	bat := batch.Batch{}
	bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_float64.ToType()))
	vector.AppendFixed[float64](bat.Vecs[0], float64(0x00), false, ses.pool)
	updateStorageSize(bat.Vecs[0], uint64(size), 0)
	require.Equal(t, float64(size)/1024/1024, vector.GetFixedAt[float64](bat.Vecs[0], 0))
}

func Test_updateCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ori := int64(0x12)
	add := int64(0x12)
	bat := batch.Batch{}
	bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_int64.ToType()))
	vector.AppendFixed[int64](bat.Vecs[0], ori, false, ses.pool)
	updateCount(bat.Vecs[0], add, 0)
	require.Equal(t, ori+add, vector.GetFixedAt[int64](bat.Vecs[0], 0))
}

func Test_updateStorageUsageCache(t *testing.T) {
	var accIds []int64
	var sizes []uint64

	for i := 0; i < 10; i++ {
		accIds = append(accIds, int64(i))
		sizes = append(sizes, rand.Uint64())
	}

	updateStorageUsageCache(accIds, sizes)

	usages := cnUsageCache.GatherAllAccSize()
	for i := 0; i < len(accIds); i++ {
		require.Equal(t, sizes[i], usages[uint64(i)])
	}
}

func Test_checkStorageUsageCache(t *testing.T) {
	var accIds [][]int64
	var sizes []uint64

	accIds = append(accIds, []int64{int64(0)})
	sizes = append(sizes, rand.Uint64())
	updateStorageUsageCache(accIds[0], []uint64{sizes[0]})

	usages, ok := checkStorageUsageCache(accIds)
	require.True(t, ok)
	for i := 0; i < len(accIds); i++ {
		require.Equal(t, sizes[i], usages[int64(i)])
	}

	time.Sleep(time.Second * 6)
	_, ok = checkStorageUsageCache(accIds)
	require.False(t, ok)
}
