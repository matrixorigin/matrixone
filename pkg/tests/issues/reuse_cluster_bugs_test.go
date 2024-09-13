// Copyright 2021-2024 Matrix Origin
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

package issues

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestBugsFix(t *testing.T) {
	c, err := embed.NewCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	require.NoError(t, c.Start())
	defer func() {
		require.NoError(t, c.Close())
	}()

	cn, err := c.GetCNService(0)
	require.NoError(t, err)

	t.Run("bug1", func(t *testing.T) {
		binarySearchBlkDataOnUnSortedFakePKCol(t, cn)
	})

	t.Run("bug2", func(t *testing.T) {
		cnFlushS3Deletes(t, cn)
	})
}

//#region BUGS

func cnFlushS3Deletes(t *testing.T, svc embed.ServiceOperator) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	exec := svc.RawService().(cnservice.Service).GetSQLExecutor()
	require.NotNil(t, exec)

	{
		_, err := exec.Exec(ctx, "create database a;", executor.Options{})
		require.NoError(t, err)

		_, err = exec.Exec(ctx, "create table t1 (a int primary key, b varchar(256));",
			executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		_, err = exec.Exec(ctx, "insert into t1 select *,'yep' from generate_series(1,512*1024)g;",
			executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		resp, err := exec.Exec(ctx, "select count(1) from t1;",
			executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		resp.ReadRows(func(rows int, cols []*vector.Vector) bool {
			cnt := vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
			require.Equal(t, int64(512*1024), cnt)
			return true
		})
	}

	deletion.SetCNFlushDeletesThreshold(1)
	defer deletion.SetCNFlushDeletesThreshold(32)

	{
		_, err := exec.Exec(ctx, "delete from t1 where a > 1;", executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		resp, err := exec.Exec(ctx, "select count(1) from t1;", executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		resp.ReadRows(func(rows int, cols []*vector.Vector) bool {
			cnt := vector.GetFixedAtWithTypeCheck[int64](cols[0], 0)
			require.Equal(t, int64(1), cnt)
			return true
		})

		resp, err = exec.Exec(ctx, "select * from t1 where a = 1;", executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		require.Equal(t, int(1), len(resp.Batches))
		require.Equal(t, int(1), resp.Batches[0].RowCount())
		resp.ReadRows(func(rows int, cols []*vector.Vector) bool {
			aVals := executor.GetFixedRows[int32](cols[0])
			bdata, barea := vector.MustVarlenaRawData(cols[1])
			bVal := bdata[0].GetString(barea)

			require.Equal(t, int(1), len(aVals))
			require.Equal(t, int32(1), aVals[0])
			require.Equal(t, "yep", bVal)

			return true
		})
	}
}

// #18754
func binarySearchBlkDataOnUnSortedFakePKCol(
	t *testing.T,
	cn embed.ServiceOperator) {

	sqlExecutor := testutils.GetSQLExecutor(cn)
	require.NotNil(t, sqlExecutor)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

	_, err := sqlExecutor.Exec(ctx, "create database testdb;", executor.Options{})
	require.NoError(t, err)

	_, err = sqlExecutor.Exec(ctx,
		"create table hhh(a int) cluster by(`a`);",
		executor.Options{}.WithDatabase("testdb"))
	require.NoError(t, err)

	willInsertRows := 10000
	for i := 0; i < 100; i++ {
		_, err = sqlExecutor.Exec(ctx,
			fmt.Sprintf(
				"insert into hhh "+
					"select FLOOR(RAND()*1000*1000)"+
					"from generate_series(1, %d);", willInsertRows/100),
			executor.Options{}.WithDatabase("testdb"))
		require.NoError(t, err)

		_, err = sqlExecutor.Exec(ctx,
			"select mo_ctl('dn', 'flush', 'testdb.hhh');",
			executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
	}

	res, err := sqlExecutor.Exec(ctx,
		"select count(*) from hhh",
		executor.Options{}.WithDatabase("testdb"))
	require.NoError(t, err)

	n := int64(0)
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			n = executor.GetFixedRows[int64](cols[0])[0]
			return true
		},
	)
	require.Equal(t, int64(willInsertRows), n)

	eng := cn.RawService().(cnservice.Service).GetEngine()
	require.NotNil(t, eng)

	sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		op := txn.Txn()
		db, err := eng.Database(ctx, "testdb", op)
		require.NoError(t, err)
		proc := op.GetWorkspace().(*disttae.Transaction).GetProc()
		rel, err := db.Relation(ctx, "hhh", proc)
		require.NoError(t, err)

		for r := 0; r < 100; r++ {
			var keys []int64
			for i := 0; i < willInsertRows; i++ {
				keys = append(keys, rand.Int63()%int64(willInsertRows))
			}

			vec := vector.NewVec(types.T_int64.ToType())
			for i := 0; i < len(keys); i++ {
				vector.AppendFixed[int64](vec, keys[i], false, proc.GetMPool())
			}

			rel.PrimaryKeysMayBeModified(ctx, types.TS{}, types.MaxTs(), vec)

			vec.Free(proc.GetMPool())
		}

		return nil
	}, executor.Options{})
}

//#endregion
