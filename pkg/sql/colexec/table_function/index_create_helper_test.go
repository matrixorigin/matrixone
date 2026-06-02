// Copyright 2022 Matrix Origin
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

package table_function

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// makeCountBatch builds a 1-row, 1-col int64 batch holding the given count —
// the shape `SELECT count(*) FROM ...` produces.
func makeCountBatch(proc *process.Process, n int64) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
	vector.AppendFixed[int64](bat.Vecs[0], n, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

func TestFetchSrcTableRowCount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	t.Run("happy path returns count", func(t *testing.T) {
		var capturedSQL string
		runSql := func(sp *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			capturedSQL = sql
			return executor.Result{Mp: sp.Proc.Mp(), Batches: []*batch.Batch{makeCountBatch(sp.Proc, 42)}}, nil
		}

		got, err := fetchSrcTableRowCount(proc, runSql, "mydb", "mytbl")
		require.NoError(t, err)
		require.Equal(t, int64(42), got)
		require.Equal(t, "SELECT count(*) FROM `mydb`.`mytbl`", capturedSQL)
	})

	t.Run("zero count is returned as zero", func(t *testing.T) {
		runSql := func(sp *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			return executor.Result{Mp: sp.Proc.Mp(), Batches: []*batch.Batch{makeCountBatch(sp.Proc, 0)}}, nil
		}

		got, err := fetchSrcTableRowCount(proc, runSql, "mydb", "mytbl")
		require.NoError(t, err)
		require.Equal(t, int64(0), got)
	})

	t.Run("sql error is propagated", func(t *testing.T) {
		runSql := func(sp *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			return executor.Result{}, fmt.Errorf("boom")
		}

		_, err := fetchSrcTableRowCount(proc, runSql, "mydb", "mytbl")
		require.Error(t, err)
		require.Contains(t, err.Error(), "boom")
	})

	t.Run("empty batches returns error", func(t *testing.T) {
		runSql := func(sp *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			return executor.Result{Mp: sp.Proc.Mp(), Batches: []*batch.Batch{}}, nil
		}

		_, err := fetchSrcTableRowCount(proc, runSql, "mydb", "mytbl")
		require.Error(t, err)
	})

	t.Run("wrong row count returns error", func(t *testing.T) {
		runSql := func(sp *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
			bat.SetRowCount(0)
			return executor.Result{Mp: sp.Proc.Mp(), Batches: []*batch.Batch{bat}}, nil
		}

		_, err := fetchSrcTableRowCount(proc, runSql, "mydb", "mytbl")
		require.Error(t, err)
	})
}
