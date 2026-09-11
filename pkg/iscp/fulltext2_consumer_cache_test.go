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

package iscp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fulltext2FlushSQLExecutor is deliberately narrow: RunFulltext2 still owns the
// TailBuilder and transaction callback; this fake only supplies the two SQL
// results that the callback needs and records the real tail INSERT statement.
type fulltext2FlushSQLExecutor struct {
	mp         *mpool.MPool
	nextChunk  int64
	insertOnce sync.Once
	insertSeen chan string
}

func (e *fulltext2FlushSQLExecutor) Exec(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upper, "SELECT GREATEST") {
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		if err := vector.AppendFixed[int64](b.Vecs[0], e.nextChunk, false, e.mp); err != nil {
			return executor.Result{}, err
		}
		b.SetRowCount(1)
		return executor.Result{Mp: e.mp, Batches: []*batch.Batch{b}}, nil
	}
	if strings.HasPrefix(upper, "INSERT") && strings.Contains(sql, "__store") {
		e.insertOnce.Do(func() { e.insertSeen <- sql })
	}
	return executor.Result{}, nil
}

func (e *fulltext2FlushSQLExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return nil
}

// cacheFlushProbe is a complete VectorIndexSearchIf implementation so the test
// exercises the cache wrapper, including Preload/Load/Destroy, while keeping the
// fake search result itself independent from the cache's implementation.
type cacheFlushProbe struct {
	loads    atomic.Int32
	destroys atomic.Int32
	searches atomic.Int32
}

func (p *cacheFlushProbe) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	p.searches.Add(1)
	return []int64{42}, []float64{0.25}, nil
}

func (p *cacheFlushProbe) SearchFloat32(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	if len(outKeys) > 0 {
		outKeys[0] = 42
	}
	if len(outDists) > 0 {
		outDists[0] = 0.25
	}
	return nil
}

func (p *cacheFlushProbe) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return fmt.Errorf("SearchInto is outside this cache contract test")
}

func (*cacheFlushProbe) Preload(*sqlexec.SqlProcess) error { return nil }

func (p *cacheFlushProbe) Load(*sqlexec.SqlProcess) error {
	p.loads.Add(1)
	return nil
}

func (*cacheFlushProbe) GetIndexSize() (int64, int64) { return 0, 0 }

func (p *cacheFlushProbe) Destroy() { p.destroys.Add(1) }

var _ veccache.VectorIndexSearchIf = (*cacheFlushProbe)(nil)
var _ executor.SQLExecutor = (*fulltext2FlushSQLExecutor)(nil)

// TestRunFulltext2KeepsWarmCacheOnNonEmptyCDCFlush proves the #28005 contract
// through the real consumer path. A non-empty encoded writer blob is consumed by
// RunFulltext2, the tail INSERT and watermark happen before a commit barrier, and
// a reader of the exact index key still uses the same warm object while that flush
// is blocked. Only a later, explicitly driven ordinary TTL sweep destroys it and
// permits a replacement load.
func TestRunFulltext2KeepsWarmCacheOnNonEmptyCDCFlush(t *testing.T) {
	const (
		indexKey    = "__store"
		serviceID   = "ft2-cache-contract-28005"
		phaseWait   = 5 * time.Second
		cleanupWait = 5 * time.Second
	)

	oldCache := veccache.Cache
	testCache := veccache.NewVectorIndexCache()
	veccache.Cache = testCache

	commitRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseCommit := func() { releaseOnce.Do(func() { close(commitRelease) }) }

	runCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	runDone := make(chan struct{})
	var consumerStarted bool
	var readerStarted bool
	readerFinished := make(chan struct{})

	// Register cache teardown first so it runs last under testing.T's LIFO cleanup
	// order, after the consumer, runtime executor, mocks, and temp resources are gone.
	t.Cleanup(func() {
		testCache.Destroy()
		veccache.Cache = oldCache
	})

	proc := testutil.NewProc(t)
	exec := &fulltext2FlushSQLExecutor{
		mp:         proc.Mp(),
		nextChunk:  17,
		insertSeen: make(chan string, 1),
	}
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, serviceID, zap.NewNop())
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	t.Cleanup(func() {
		rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, exec)
	})

	ctrl := gomock.NewController(t)
	// This cleanup must be registered after runtime teardown and gomock's
	// controller cleanup so it runs first. Assertion failures and timeouts can
	// otherwise finish the executor/mocks while RunFulltext2 is still in its txn.
	t.Cleanup(func() {
		releaseCommit()
		cancel()
		if consumerStarted {
			select {
			case <-runDone:
			case <-time.After(cleanupWait):
				t.Errorf("RunFulltext2 did not stop before test teardown")
			}
		}
		if readerStarted {
			select {
			case <-readerFinished:
			case <-time.After(cleanupWait):
				t.Errorf("warm cache reader did not stop before test teardown")
			}
		}
	})
	cnEngine := mock_frontend.NewMockEngine(ctrl)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	cnEngine.EXPECT().LatestLogtailAppliedTime().Return(timestamp.Timestamp{})
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOp, nil)
	cnEngine.EXPECT().New(gomock.Any(), txnOp).Return(nil)
	commitStarted := make(chan struct{})
	txnOp.EXPECT().Commit(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		close(commitStarted)
		select {
		case <-commitRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	w := newFT2Writer(fulltext2.ParserNgram)
	w.cfg.IndexTable = indexKey
	w.capacity = 64
	w.postingCap = 1024
	require.NoError(t, w.Insert(context.Background(), []any{int64(101), "cache contract"}))
	blob, err := w.ToSql()
	require.NoError(t, err)
	require.NotEmpty(t, blob, "the consumer must receive a non-empty encoded CDC batch")
	decoded, err := fulltext2.DecodeCdc(blob)
	require.NoError(t, err)
	require.Len(t, decoded.Events, 1, "the flush fixture must carry one real INSERT event")

	warm := &cacheFlushProbe{}
	_, _, err = testCache.Search(nil, indexKey, warm, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, int32(1), warm.loads.Load())
	value, ok := testCache.IndexMap.Load(indexKey)
	require.True(t, ok)
	warmEntry, ok := value.(*veccache.VectorIndexSearch)
	require.True(t, ok)
	require.Same(t, warm, warmEntry.Algo)

	watermarkSeen := make(chan struct{})
	var watermarkOnce sync.Once
	var watermarkService atomic.Value
	var watermarkTxn atomic.Value
	r := &MockRetriever{
		dtype: ISCPDataType_Tail,
		updateWatermark: func(_ context.Context, service string, txn client.TxnOperator) error {
			watermarkService.Store(service)
			watermarkTxn.Store(txn)
			watermarkOnce.Do(func() { close(watermarkSeen) })
			return nil
		},
	}
	c := &IndexConsumer{
		cnUUID:       serviceID,
		cnEngine:     cnEngine,
		cnTxnClient:  txnClient,
		sqlWriter:    w,
		sqlBufSendCh: make(chan []byte, 1),
	}
	errch := make(chan error, 1)
	consumerStarted = true
	go func() {
		defer close(runDone)
		RunFulltext2(c, runCtx, errch, r)
	}()
	c.sqlBufSendCh <- blob
	close(c.sqlBufSendCh)

	select {
	case sql := <-exec.insertSeen:
		require.Contains(t, sql, indexKey)
	case <-time.After(phaseWait):
		t.Fatal("non-empty CDC flush did not issue a tail INSERT")
	}
	select {
	case <-watermarkSeen:
	case <-time.After(phaseWait):
		t.Fatal("CDC flush did not update the watermark")
	}
	select {
	case <-commitStarted:
	case <-time.After(phaseWait):
		t.Fatal("CDC flush did not reach the commit barrier")
	}

	// The commit is blocked, so this reader proves that the flush did not remove
	// or reload the exact warm entry. A newalgo is supplied deliberately: a loaded
	// cache key must continue using the resident backend and leave the candidate
	// untouched.
	candidate := &cacheFlushProbe{}
	readerDone := make(chan error, 1)
	readerStarted = true
	go func() {
		defer close(readerFinished)
		keys, distances, searchErr := testCache.Search(nil, indexKey, candidate, nil, vectorindex.RuntimeConfig{})
		if searchErr == nil && (keys == nil || len(distances) != 1) {
			searchErr = fmt.Errorf("warm reader returned unexpected result: keys=%v distances=%v", keys, distances)
		}
		readerDone <- searchErr
	}()
	select {
	case readerErr := <-readerDone:
		require.NoError(t, readerErr)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("warm cache reader waited for CDC flush commit")
	}
	require.Equal(t, int32(0), candidate.loads.Load(), "a warm read must not load a replacement")
	require.Equal(t, int32(2), warm.searches.Load(), "the resident backend must serve both the warm-up and blocked-commit read")
	require.Equal(t, int32(0), warm.destroys.Load(), "CDC flush must not destroy the warm backend")
	require.Same(t, warm, warmEntry.Algo)

	releaseCommit()
	select {
	case <-runDone:
	case <-time.After(phaseWait):
		t.Fatal("RunFulltext2 did not finish after commit release")
	}
	select {
	case flushErr := <-errch:
		require.NoError(t, flushErr)
	default:
	}
	require.Equal(t, serviceID, watermarkService.Load())
	require.Same(t, txnOp, watermarkTxn.Load())
	require.Equal(t, int32(1), warm.loads.Load())
	require.Equal(t, int32(0), warm.destroys.Load())
	require.Same(t, warm, warmEntry.Algo)

	// A separately driven ordinary stale/TTL sweep is allowed to retire the
	// entry. This is intentionally after the successful flush: it distinguishes
	// ordinary cache lifecycle from a CDC-triggered invalidation.
	warmEntry.ExpireAt.Store(time.Now().Add(-time.Second).UnixMicro())
	testCache.HouseKeeping()
	_, stillWarm := testCache.IndexMap.Load(indexKey)
	require.False(t, stillWarm)
	require.Equal(t, int32(1), warm.destroys.Load())

	replacement := &cacheFlushProbe{}
	_, _, err = testCache.Search(nil, indexKey, replacement, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, int32(1), replacement.loads.Load())
	value, ok = testCache.IndexMap.Load(indexKey)
	require.True(t, ok)
	replacementEntry, ok := value.(*veccache.VectorIndexSearch)
	require.True(t, ok)
	require.Same(t, replacement, replacementEntry.Algo)
}
