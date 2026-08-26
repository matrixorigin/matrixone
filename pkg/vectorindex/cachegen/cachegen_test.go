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

package cachegen

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// TestCdcGenerationSqls pins the two freshness-generation queries: MAX(timestamp) over the
// metadata table (REBUILD/MERGE signal) and MAX(chunk_id) over the tag=1 CdcTail (CDC-append
// signal), scoped to (CdcTailId, tag=1) so a base sub-index cannot mask a fresh append.
func TestCdcGenerationSqls(t *testing.T) {
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "__meta", IndexTable: "__store"}
	tsSQL, tailSQL := CdcGenerationSqls(cfg)
	require.Contains(t, tsSQL, "MAX(timestamp)")
	require.Contains(t, tsSQL, "`db`.`__meta`")
	require.Contains(t, tailSQL, "MAX(chunk_id)")
	require.Contains(t, tailSQL, "`db`.`__store`")
	require.Contains(t, tailSQL, "tag = 1")             // tag = Tag_CdcEvents
	require.Contains(t, tailSQL, vectorindex.CdcTailId) // scoped to the single CDC tail
}

// TestGenScalarInt64 covers the scalar reader: a populated batch returns its first int64, and
// empty / nil batches fall through to the 0 default.
func TestGenScalarInt64(t *testing.T) {
	mp := mpool.MustNewZero()
	require.Equal(t, int64(0), genScalarInt64(executor.Result{}))                             // no batches
	require.Equal(t, int64(0), genScalarInt64(executor.Result{Batches: []*batch.Batch{nil}})) // nil batch skipped

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], int64(42), false, mp))
	bat.SetRowCount(1)
	require.Equal(t, int64(42), genScalarInt64(executor.Result{Batches: []*batch.Batch{bat}}))
	bat.Clean(mp)
}

// TestLoadCdcGenerationRecover: a unit-test sqlproc has no internal SQL executor, so RunSql
// panics ("missing lock service"); LoadCdcGeneration must recover it into an error (best-effort
// capture — the caller then leaves genValid=false rather than failing the load).
func TestLoadCdcGenerationRecover(t *testing.T) {
	proc := sqlexec.NewSqlProcess(testutil.NewProc(t))
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "__meta", IndexTable: "__store"}
	_, _, err := LoadCdcGeneration(proc, cfg)
	require.Error(t, err)
}

// TestQueryCdcGenerationError: an unknown CN service has no registered internal SQL executor, so
// RunSqlAutoCommit returns an error, which QueryCdcGeneration propagates (IsStale then treats it
// as stale).
func TestQueryCdcGenerationError(t *testing.T) {
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "__meta", IndexTable: "__store"}
	_, _, err := QueryCdcGeneration(context.Background(), "no-such-cn-uuid", 0, cfg)
	require.Error(t, err)
}

func int64Result(t *testing.T, mp *mpool.MPool, v int64) executor.Result {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], v, false, mp))
	bat.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

// TestLoadCdcGenerationHappy stubs runSql to cover the full two-read body (timestamp then tail)
// plus the second-read error branch.
func TestLoadCdcGenerationHappy(t *testing.T) {
	mp := mpool.MustNewZero()
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"}

	old := runSql
	defer func() { runSql = old }()

	n := 0
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		n++
		if n == 1 {
			return int64Result(t, mp, 100), nil // timestamp
		}
		return int64Result(t, mp, 5), nil // tail
	}
	ts, tail, err := LoadCdcGeneration(nil, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(100), ts)
	require.Equal(t, int64(5), tail)

	// tail read errors → propagated.
	n = 0
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		n++
		if n == 1 {
			return int64Result(t, mp, 100), nil
		}
		return executor.Result{}, moerr.NewInternalErrorNoCtx("tail read failed")
	}
	_, _, err = LoadCdcGeneration(nil, cfg)
	require.Error(t, err)
}

// TestQueryCdcGenerationHappy stubs runSqlAutoCommit to cover the full background two-read body.
func TestQueryCdcGenerationHappy(t *testing.T) {
	mp := mpool.MustNewZero()
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"}

	old := runSqlAutoCommit
	defer func() { runSqlAutoCommit = old }()

	n := 0
	runSqlAutoCommit = func(_ context.Context, _ string, _ uint32, _, _ string) (executor.Result, error) {
		n++
		if n == 1 {
			return int64Result(t, mp, 7), nil
		}
		return int64Result(t, mp, 9), nil
	}
	ts, tail, err := QueryCdcGeneration(context.Background(), "cn", 0, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(7), ts)
	require.Equal(t, int64(9), tail)
}
