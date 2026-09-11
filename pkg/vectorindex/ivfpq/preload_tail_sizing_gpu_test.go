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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// routedExecutor answers each of Preload's reads by what the SQL asks for, so a test can fail
// one read and leave the others intact. It is installed as the service's InternalSQLExecutor,
// which is what RunSql reaches: LoadMetadata and the tail sizing both run through it, so what
// Preload does with a partial failure is exercised composed rather than stubbed per package.
type routedExecutor struct {
	meta     func() (executor.Result, error)
	coverage func() (executor.Result, error)
	count    func() (executor.Result, error)
}

func (r *routedExecutor) Exec(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
	switch {
	case strings.Contains(sql, "COUNT(*)"):
		return r.count()
	case strings.Contains(sql, "SUM("):
		return r.coverage()
	default:
		return r.meta()
	}
}

func (r *routedExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return nil
}

// twoInt64Result is the coverage read's shape: summed rows, and the chunks those flushes span.
func twoInt64Result(mp *mpool.MPool, a, b int64) executor.Result {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendFixed[int64](bat.Vecs[0], a, false, mp)
	_ = vector.AppendFixed[int64](bat.Vecs[1], b, false, mp)
	bat.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func oneInt64Result(mp *mpool.MPool, v int64) executor.Result {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendFixed[int64](bat.Vecs[0], v, false, mp)
	bat.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func readErr() (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalErrorNoCtx("read unavailable")
}

// emptyTailExecutor reads successfully and finds nothing: no frame rows, no chunks -- an empty
// tail, which is a real answer and not an unreadable one.
type emptyTailExecutor struct{}

func (emptyTailExecutor) Exec(context.Context, string, executor.Options) (executor.Result, error) {
	return executor.Result{}, nil
}

func (emptyTailExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return nil
}

// installEmptyTailSizing answers the tail sizing for a test that stubs the package's own runSql.
// The sizing goes through sqlexec.RunSql directly rather than that seam, and Preload now REFUSES
// a tail whose size cannot be read -- so a process with no internal SQL executor is refused for
// a tail the test never gave it. Production cannot reach that shape: the metadata and index
// reads run through the same executor, so a load that gets as far as sizing has one.
func installEmptyTailSizing(t *testing.T, sid string) {
	t.Helper()
	if moruntime.ServiceRuntime(sid) == nil {
		moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
	}
	moruntime.ServiceRuntime(sid).SetGlobalVariables(moruntime.InternalSQLExecutor, emptyTailExecutor{})
}

// preloadHarness installs exec as the service's internal SQL executor and returns the sqlproc
// and the CDC-only search whose Preload is under test: no built sub-index, which is exactly the
// generation that measures 0/0 and bypasses makeRoom entirely if the tail is not sized.
func preloadHarness(t *testing.T, sid string, exec executor.SQLExecutor) (*sqlexec.SqlProcess, *IvfpqSearch[float32, float32]) {
	t.Helper()
	moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
	moruntime.ServiceRuntime(sid).SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(context.Background(), sid, nil, 0, nil))

	s := &IvfpqSearch[float32, float32]{}
	s.Idxcfg.CuvsIvfpq.Dimensions = 128
	s.Tblcfg.DbName = "db"
	s.Tblcfg.MetadataTable = "meta"
	s.Tblcfg.IndexTable = "store"
	return sqlproc, s
}

// The state the review named, sized correctly: a migrated tail of 400 legacy chunks that no
// frame row describes, plus ONE later flush that recorded its single row. The sum alone says 1;
// the chunks say the load will replay all 401. Preload must charge the bound, because that
// charge is the reservation makeRoom takes before Load allocates the overflow on the GPU.
func TestIvfpqPreloadChargesThePartiallyCoveredTail(t *testing.T) {
	mp := mpool.MustNewZero()
	const legacyChunks, newRows, newChunks = 400, 1, 1
	sqlproc, s := preloadHarness(t, "ut-ivfpq-preload-covered", &routedExecutor{
		meta:     func() (executor.Result, error) { return executor.Result{Mp: mp}, nil },
		coverage: func() (executor.Result, error) { return twoInt64Result(mp, newRows, newChunks), nil },
		count:    func() (executor.Result, error) { return oneInt64Result(mp, legacyChunks+newChunks), nil },
	})

	require.NoError(t, s.Preload(sqlproc))

	vecBytes := int64(128 * 4)
	perChunk := int64(vectorindex.MaxChunkSize) / vecBytes
	wantRows := int64(newRows) + legacyChunks*perChunk
	require.Equal(t, int64(51201), wantRows)

	_, device := s.GetIndexSize()
	require.Equal(t, wantRows*vecBytes, device,
		"admission reserves from GetIndexSize, so the whole tail has to be in it")
}

// Metadata readable, chunk COUNT not. Discarding that error read 0 chunks, made the one covered
// chunk look like the whole tail, and returned 1 row -- so a 401-chunk tail was admitted on a
// reservation for one row, and Load allocated the rest with nothing held for it. Refuse instead,
// before any tar is fetched.
func TestIvfpqPreloadRefusesWhenTheChunkCountIsUnreadable(t *testing.T) {
	mp := mpool.MustNewZero()
	sqlproc, s := preloadHarness(t, "ut-ivfpq-preload-count-err", &routedExecutor{
		meta:     func() (executor.Result, error) { return executor.Result{Mp: mp}, nil },
		coverage: func() (executor.Result, error) { return twoInt64Result(mp, 1, 1), nil },
		count:    readErr,
	})

	err := s.Preload(sqlproc)
	require.Error(t, err, "an unsizeable tail must not be admitted with no reservation")
	require.Nil(t, s.Indexes, "Preload refused, so it owns nothing to load or destroy")
	_, device := s.GetIndexSize()
	require.Zero(t, device, "nothing was charged because nothing may be allocated")
}

// Neither read available: the narrow-table fallback errors too, so there is no bound at all.
// This one used to return 0 through chunkBound, which is the same silent admission.
func TestIvfpqPreloadRefusesWhenNoSizingReadSucceeds(t *testing.T) {
	mp := mpool.MustNewZero()
	sqlproc, s := preloadHarness(t, "ut-ivfpq-preload-all-err", &routedExecutor{
		meta:     func() (executor.Result, error) { return executor.Result{Mp: mp}, nil },
		coverage: readErr,
		count:    readErr,
	})

	err := s.Preload(sqlproc)
	require.Error(t, err)
	require.Nil(t, s.Indexes)
}

// A NARROW table has no nrow column, so the coverage sum errors -- an ordinary rollout state,
// not an outage. The chunks still bound it, and Preload still admits the load, sized. This is
// the case the refusals above must not swallow.
func TestIvfpqPreloadFallsBackToChunksOnANarrowTable(t *testing.T) {
	mp := mpool.MustNewZero()
	const chunks = 10
	sqlproc, s := preloadHarness(t, "ut-ivfpq-preload-narrow", &routedExecutor{
		meta:     func() (executor.Result, error) { return executor.Result{Mp: mp}, nil },
		coverage: readErr,
		count:    func() (executor.Result, error) { return oneInt64Result(mp, chunks), nil },
	})

	require.NoError(t, s.Preload(sqlproc), "a narrow table is a supported state, not a load failure")

	vecBytes := int64(128 * 4)
	_, device := s.GetIndexSize()
	require.Equal(t, chunks*(int64(vectorindex.MaxChunkSize)/vecBytes)*vecBytes, device)
	require.Positive(t, device, "it must not silently reserve zero")
}

// The metadata read itself failing is refused where it always was, ahead of the sizing.
func TestIvfpqPreloadRefusesWhenMetadataIsUnreadable(t *testing.T) {
	sqlproc, s := preloadHarness(t, "ut-ivfpq-preload-meta-err", &routedExecutor{
		meta:     readErr,
		coverage: readErr,
		count:    readErr,
	})

	require.Error(t, s.Preload(sqlproc))
	require.Nil(t, s.Indexes)
}
