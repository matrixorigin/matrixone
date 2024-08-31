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

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCNFlushS3Deletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	c, err := embed.NewCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	require.NoError(t, c.Start())

	svc, err := c.GetCNService(0)
	require.NoError(t, err)

	exec := svc.RawService().(cnservice.Service).GetSQLExecutor()
	require.NotNil(t, exec)

	{
		_, err = exec.Exec(ctx, "create database a;", executor.Options{})
		require.NoError(t, err)

		_, err = exec.Exec(ctx, "create table t1 (a int primary key, b varchar(1024));",
			executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		_, err = exec.Exec(ctx, "insert into t1 select *,'yep' from generate_series(1,512*1024)g;",
			executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		resp, err := exec.Exec(ctx, "select count(1) from t1;",
			executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		resp.ReadRows(func(rows int, cols []*vector.Vector) bool {
			cnt := vector.GetFixedAt[int64](cols[0], 0)
			require.Equal(t, int64(512*1024), cnt)
			return true
		})
	}

	deletion.SetCNFlushDeletesThreshold(10)

	{
		_, err = exec.Exec(ctx, "delete from t1 where a > 1;", executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		resp, err := exec.Exec(ctx, "select count(1) from t1;", executor.Options{}.WithDatabase("a"))
		require.NoError(t, err)

		resp.ReadRows(func(rows int, cols []*vector.Vector) bool {
			cnt := vector.GetFixedAt[int64](cols[0], 0)
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
	require.NoError(t, c.Close())
}
