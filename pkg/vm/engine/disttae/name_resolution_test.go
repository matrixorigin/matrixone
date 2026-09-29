// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

func TestMode2CatalogVisitRejectsGCStartAdvance(t *testing.T) {
	eng, _, txn, _ := newTempCatalogFixture(t)
	insertTempCatalogDatabase(t, eng, txn, tempCatalogSnapshot)
	indexRoot := tempCatalogRoot(tempCatalogSession, tempCatalogChildID)
	insertTempCatalogTable(t, eng, txn, indexRoot, tempCatalogSnapshot)
	cc := eng.GetLatestCatalogCache()
	snapshot := tempCatalogSnapshot.ToTimestamp()
	for _, tc := range []struct {
		name  string
		visit func(func())
	}{
		{
			name: "database",
			visit: func(advance func()) {
				cc.VisitFoldedDatabases(tempCatalogAccountID, tempCatalogDatabase, snapshot,
					func(*cache.DatabaseItem) bool { advance(); return false })
			},
		},
		{
			name: "table",
			visit: func(advance func()) {
				cc.VisitFoldedTables(tempCatalogAccountID, tempCatalogDatabaseID,
					strings.ToUpper(indexRoot.TableName), snapshot,
					func(*cache.TableItem) bool { advance(); return false })
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc.UpdateDuration(types.TS{}, types.MaxTs())
			visited := false
			complete := visitCompleteMode2CatalogSnapshot(cc, snapshot, func() {
				tc.visit(func() {
					visited = true
					// GC advances this watermark before deleting catalog versions.
					cc.UpdateStart(types.BuildTS(101, 0))
				})
			})
			require.True(t, visited)
			require.False(t, complete)
		})
	}
}

func TestMode2TxnFoldedIndexTracksCreateDeleteAndRollback(t *testing.T) {
	dbs := newDbOps()
	dbKey := databaseKey{accountId: 7, name: "Foo"}
	dbs.addCreateDatabase(dbKey, 1, &txnDatabase{databaseId: 10})
	require.Equal(t, uint64(10), dbs.foldedSnapshot(7, "foo")["Foo"].databaseId)
	dbs.addDeleteDatabase(dbKey, 2, 10)
	require.Equal(t, DELETE, dbs.foldedSnapshot(7, "foo")["Foo"].kind)
	dbs.rollbackLastStatement(2)
	require.Equal(t, INSERT, dbs.foldedSnapshot(7, "foo")["Foo"].kind)
	dbs.rollbackLastStatement(1)
	require.Empty(t, dbs.foldedSnapshot(7, "foo"))
	require.Empty(t, dbs.foldedNames)
	dbs.addCreateDatabase(databaseKey{accountId: 7, name: "fOo"}, 3, &txnDatabase{databaseId: 11})
	require.Equal(t, uint64(11), dbs.foldedSnapshot(7, "foo")["fOo"].databaseId)

	tables := newTableOps()
	table := tableKey{accountId: 7, databaseId: 9, dbName: "Qa", name: "MixT"}
	tables.addCreateTable(table, 1, &txnTable{tableId: 20})
	require.Equal(t, uint64(20), tables.foldedSnapshot(7, 9, "mixt")["MixT"].tableId)
	tables.addDeleteTable(table, 2, 20)
	require.Equal(t, DELETE, tables.foldedSnapshot(7, 9, "mixt")["MixT"].kind)
	tables.rollbackLastStatement(2)
	require.Equal(t, INSERT, tables.foldedSnapshot(7, 9, "mixt")["MixT"].kind)
	tables.rollbackLastStatement(1)
	require.Empty(t, tables.foldedSnapshot(7, 9, "mixt"))
	require.Empty(t, tables.foldedNames)
}

func TestMode2TxnFoldedIndexConcurrentSnapshotsAndWrites(t *testing.T) {
	ops := newTableOps()
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 500; i++ {
			key := tableKey{accountId: 7, databaseId: 9, dbName: "Qa", name: fmt.Sprintf("Tbl_%04d", i)}
			ops.addCreateTable(key, 1, &txnTable{tableId: uint64(i + 10)})
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 500; i++ {
			ops.foldedSnapshot(7, 9, "tbl_0000")
		}
	}()
	close(start)
	wg.Wait()
	require.Len(t, ops.foldedNames, 500)
	require.Equal(t, uint64(10), ops.foldedSnapshot(7, 9, "tbl_0000")["Tbl_0000"].tableId)
}

func BenchmarkMode2TxnLocalTableLookup(b *testing.B) {
	for _, size := range []int{1, 100, 10000} {
		ops := newTableOps()
		for i := 0; i < size-1; i++ {
			key := tableKey{accountId: 7, databaseId: 9, dbName: "Qa", name: fmt.Sprintf("Tbl_%05d", i)}
			ops.addCreateTable(key, 1, &txnTable{tableId: uint64(i + 10)})
		}
		ops.addCreateTable(tableKey{accountId: 7, databaseId: 9, dbName: "Qa", name: "TargetTbl"}, 1, &txnTable{tableId: 100000})
		ops.foldedSnapshot(7, 9, "targettbl")
		b.Run(fmt.Sprintf("names_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if ops.foldedSnapshot(7, 9, "targettbl")["TargetTbl"].tableId != 100000 {
					b.Fatal("transaction-local table lookup lost target")
				}
			}
		})
	}
}

type nameScanExecutor struct {
	executor.SQLExecutor
	run func(context.Context, string, executor.Options) (executor.Result, error)
}

func (e *nameScanExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	return e.run(ctx, sql, opts)
}

func setupNameScanTest(t testing.TB, run func(context.Context, string, executor.Options) (executor.Result, error)) *mock_frontend.MockTxnOperator {
	t.Helper()
	proc := testutil.NewProc(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	fake := &nameScanExecutor{run: run}
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, fake)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, fake)
		}
	})
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().GetWorkspace().Return(&Transaction{proc: proc}).AnyTimes()
	return op
}

func nameScanResult(mp *mpool.MPool, names ...string) (executor.Result, error) {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	result := executor.NewResult(mp)
	result.Batches = []*batch.Batch{bat}
	for _, name := range names {
		if err := vector.AppendBytes(bat.Vecs[0], []byte(name), false, mp); err != nil {
			result.Close()
			return executor.Result{}, err
		}
	}
	bat.SetRowCount(len(names))
	return result, nil
}

func TestHistoricalMode2NameScanStreamsOneScopedQuery(t *testing.T) {
	const accountID = uint32(7)
	ts := timestamp.Timestamp{PhysicalTime: 123}
	for _, tc := range []struct {
		name    string
		scan    func(context.Context, *mock_frontend.MockTxnOperator, func(string) bool) error
		wantSQL []string
	}{
		{
			name: "database",
			scan: func(ctx context.Context, op *mock_frontend.MockTxnOperator, visit func(string) bool) error {
				return scanHistoricalDatabaseNames(ctx, op, accountID, "foo", visit)
			},
			wantSQL: []string{"select datname", "account_id = 7"},
		},
		{
			name: "table",
			scan: func(ctx context.Context, op *mock_frontend.MockTxnOperator, visit func(string) bool) error {
				return scanHistoricalTableNames(ctx, op, accountID, "Qa", 19, "foo", visit)
			},
			wantSQL: []string{"select relname", "account_id = 7", "reldatabase = 'Qa'", "reldatabase_id = 19"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			queries := 0
			op := setupNameScanTest(t, func(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
				queries++
				for _, clause := range tc.wantSQL {
					if !strings.Contains(sql, clause) {
						return executor.Result{}, fmt.Errorf("historical scan omitted %q from %q", clause, sql)
					}
				}
				if strings.Contains(strings.ToLower(sql), "order by") {
					return executor.Result{}, fmt.Errorf("historical scan sorts catalog: %q", sql)
				}
				results, _, streaming := opts.Streaming()
				if !streaming {
					return executor.Result{}, errors.New("historical scan is not streaming")
				}
				result, err := nameScanResult(mp, "Foo", "foo", "FoO")
				if err != nil {
					return executor.Result{}, err
				}
				results <- result
				return executor.Result{}, nil
			})
			op.EXPECT().SnapshotTS().Return(ts).AnyTimes()
			var matches []string
			err := tc.scan(context.Background(), op, func(name string) bool {
				matches = append(matches, name)
				return len(matches) < 2
			})
			require.NoError(t, err)
			require.Equal(t, []string{"Foo", "foo"}, matches)
			require.Equal(t, 1, queries)
		})
	}
}

// This exercises the production historical scanner and its streaming batch
// ownership with 100,000 names without requiring 100,000 catalog DDLs.
func BenchmarkHistoricalMode2DatabaseScan100kStream(b *testing.B) {
	const total, batchSize = 100000, 1000
	for iteration := 0; iteration < b.N; iteration++ {
		mp := mpool.MustNewZero()
		var batches atomic.Int64
		op := setupNameScanTest(b, func(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
			if !strings.Contains(sql, "account_id = 7") {
				return executor.Result{}, fmt.Errorf("scan lost tenant scope: %s", sql)
			}
			results, _, streaming := opts.Streaming()
			if !streaming {
				return executor.Result{}, errors.New("historical scan is not streaming")
			}
			for start := 0; start < total; start += batchSize {
				names := make([]string, batchSize)
				for i := range names {
					names[i] = fmt.Sprintf("PerfDB29418_%05d", start+i)
				}
				result, err := nameScanResult(mp, names...)
				if err != nil {
					return executor.Result{}, err
				}
				results <- result
				batches.Add(1)
			}
			return executor.Result{}, nil
		})
		ts := timestamp.Timestamp{PhysicalTime: 123}
		op.EXPECT().SnapshotTS().Return(ts).AnyTimes()
		runtime.GC()
		var before, current runtime.MemStats
		runtime.ReadMemStats(&before)
		peak := before.Alloc
		stop := make(chan struct{})
		sampled := make(chan struct{})
		go func() {
			defer close(sampled)
			ticker := time.NewTicker(100 * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					runtime.ReadMemStats(&current)
					if current.Alloc > peak {
						peak = current.Alloc
					}
				}
			}
		}()
		matches := 0
		err := scanHistoricalDatabaseNames(context.Background(), op, 7,
			"perfdb29418_99999", func(name string) bool {
				matches++
				return name == "PerfDB29418_99999"
			})
		close(stop)
		<-sampled
		runtime.ReadMemStats(&current)
		if current.Alloc > peak {
			peak = current.Alloc
		}
		if err != nil || matches != 1 || batches.Load() != total/batchSize || mp.CurrNB() != 0 {
			b.Fatalf("scan failed: err=%v matches=%d batches=%d mpool=%d", err, matches, batches.Load(), mp.CurrNB())
		}
		b.ReportMetric(float64(peak-before.Alloc)/(1024*1024), "peak_heap_delta_MiB")
	}
}

func TestHistoricalMode2NameScanDrainsRepeatedExecutorErrors(t *testing.T) {
	want := errors.New("stream failed")
	op := setupNameScanTest(t, func(_ context.Context, _ string, opts executor.Options) (executor.Result, error) {
		_, errorCh, streaming := opts.Streaming()
		if !streaming {
			return executor.Result{}, errors.New("historical scan is not streaming")
		}
		errorCh <- want
		errorCh <- errors.New("second report")
		return executor.Result{}, want
	})
	ts := timestamp.Timestamp{PhysicalTime: 123}
	op.EXPECT().SnapshotTS().Return(ts).AnyTimes()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := scanHistoricalDatabaseNames(ctx, op, 7, "foo", func(string) bool { return true })
	require.ErrorIs(t, err, want)
}

func TestHistoricalMode2NameScanCancelsAndJoinsProducerOnConsumerError(t *testing.T) {
	want := errors.New("consumer failed")
	producerDone := make(chan struct{})
	mp := mpool.MustNewZero()
	op := setupNameScanTest(t, func(ctx context.Context, _ string, opts executor.Options) (executor.Result, error) {
		results, errorCh, streaming := opts.Streaming()
		if !streaming {
			return executor.Result{}, errors.New("historical scan is not streaming")
		}
		result, err := nameScanResult(mp, "Foo")
		if err != nil {
			return executor.Result{}, err
		}
		results <- result
		<-ctx.Done()
		errorCh <- ctx.Err()
		errorCh <- ctx.Err()
		close(producerDone)
		return executor.Result{}, ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := scanReadSql(ctx, op, "select datname from mo_catalog.mo_database", func(executor.Result) error {
		return want
	})
	require.ErrorIs(t, err, want)
	select {
	case <-producerDone:
	case <-ctx.Done():
		t.Fatal("streaming SQL producer did not join after cancellation")
	}
}
