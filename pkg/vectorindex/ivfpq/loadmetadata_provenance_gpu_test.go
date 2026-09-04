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

//go:build gpu

package ivfpq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// makeMetaBatch6 builds the post-upgrade metadata row: the original four columns
// plus nrow and build_ts.
func makeMetaBatch6(proc *process.Process, id, checksum string, timestamp, filesize, nrow, buildTS int64) *batch.Batch {
	bat := batch.NewWithSize(6)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 65536, 0))
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))
	bat.Vecs[3] = vector.NewVec(types.New(types.T_int64, 8, 0))
	bat.Vecs[4] = vector.NewVec(types.New(types.T_int64, 8, 0))
	bat.Vecs[5] = vector.NewVec(types.New(types.T_int64, 8, 0))

	vector.AppendBytes(bat.Vecs[0], []byte(id), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], []byte(checksum), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[2], timestamp, false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[3], filesize, false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[4], nrow, false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[5], buildTS, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

// LoadMetadata reads all three metadata shapes. nrow and build_ts were appended
// after the original four columns, and the metadata table is created per index at
// CREATE INDEX (REINDEX rewrites its rows, not the table), so a pre-upgrade index
// still presents four columns and loads with the new fields left at 0.
func TestIvfpqLoadMetadataProvenanceColumns(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	orig := runSql
	defer func() { runSql = orig }()

	t.Run("post-upgrade six columns", func(t *testing.T) {
		runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return executor.Result{
				Mp:      proc.Mp(),
				Batches: []*batch.Batch{makeMetaBatch6(proc, "idx0", "sum", 7, 4096, 1234, 5678)},
			}, nil
		}

		models, err := LoadMetadata[float32, float32](sqlproc, "db", "meta")
		require.NoError(t, err)
		require.Len(t, models, 1)
		require.Equal(t, int64(1234), models[0].Nrow)
		require.Equal(t, int64(5678), models[0].BuildTS)
		require.Equal(t, int64(4096), models[0].FileSize)
	})

	t.Run("pre-upgrade four columns", func(t *testing.T) {
		runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return executor.Result{
				Mp:      proc.Mp(),
				Batches: []*batch.Batch{makeMetaBatch(proc, "idx0", "sum", 7, 4096)},
			}, nil
		}

		models, err := LoadMetadata[float32, float32](sqlproc, "db", "meta")
		require.NoError(t, err)
		require.Len(t, models, 1)
		require.Equal(t, int64(0), models[0].Nrow, "absent means unknown, not an error")
		require.Equal(t, int64(0), models[0].BuildTS)
		require.Equal(t, int64(4096), models[0].FileSize)
	})

	// A five-column row reads nrow and leaves build_ts at 0.
	t.Run("five columns", func(t *testing.T) {
		runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			bat := makeMetaBatch6(proc, "idx0", "sum", 7, 4096, 1234, 5678)
			bat.Vecs = bat.Vecs[:5]
			return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{bat}}, nil
		}

		models, err := LoadMetadata[float32, float32](sqlproc, "db", "meta")
		require.NoError(t, err)
		require.Equal(t, int64(1234), models[0].Nrow)
		require.Equal(t, int64(0), models[0].BuildTS)
	})
}
