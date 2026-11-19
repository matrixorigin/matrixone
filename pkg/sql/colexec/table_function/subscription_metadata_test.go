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

package table_function

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/subscriptionmetadata"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func makeSubscriptionCandidateResult(
	t *testing.T,
	mp *mpool.MPool,
	startDatabaseID uint64,
	count int,
	accountList string,
) executor.Result {
	t.Helper()
	columnTypes := []types.Type{
		types.T_uint64.ToType(),
		types.New(types.T_varchar, 5000, 0),
		types.T_uint32.ToType(),
		types.New(types.T_varchar, 300, 0),
		types.T_uint32.ToType(),
		types.New(types.T_varchar, 300, 0),
		types.New(types.T_varchar, 5000, 0),
		types.T_text.ToType(),
		types.T_text.ToType(),
		types.T_uint32.ToType(),
	}
	bat := batch.NewWithSize(len(columnTypes))
	for i := range columnTypes {
		bat.Vecs[i] = vector.NewVec(columnTypes[i])
	}
	for i := 0; i < count; i++ {
		databaseID := startDatabaseID + uint64(i)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], databaseID, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(fmt.Sprintf("sub_%d", databaseID)), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], uint32(9), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte("subscriber"), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], uint32(22), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte("publisher"), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], []byte("source_db"), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], []byte("*"), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[8], []byte(accountList), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[9], uint32(22), false, mp))
	}
	bat.SetRowCount(count)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func makeSubscriptionAliasResult(t *testing.T, mp *mpool.MPool, start uint64, count int) executor.Result {
	result := makeSubscriptionCandidateResult(t, mp, start, count, "all")
	bat := result.Batches[0]
	for _, column := range []int{3, 4, 7, 8, 9} {
		bat.Vecs[column].Free(mp)
	}
	bat.Vecs = []*vector.Vector{bat.Vecs[0], bat.Vecs[1], bat.Vecs[2], bat.Vecs[5], bat.Vecs[6]}
	return result
}

func makeSubscriptionTableSourceResult(
	t *testing.T,
	mp *mpool.MPool,
	accounts []uint32,
	owners []uint32,
) executor.Result {
	t.Helper()
	require.Len(t, owners, len(accounts))
	bat := batch.NewWithSize(len(subscriptionTablesConfig.columnTypes))
	for i, typ := range subscriptionTablesConfig.columnTypes {
		bat.Vecs[i] = vector.NewVec(typ)
	}
	for row := range accounts {
		for column := range bat.Vecs {
			switch column {
			case 0:
				require.NoError(t, vector.AppendFixed(bat.Vecs[column], accounts[row], false, mp))
			case 12:
				require.NoError(t, vector.AppendFixed(bat.Vecs[column], owners[row], false, mp))
			default:
				require.NoError(t, vector.AppendNull(bat.Vecs[column], mp))
			}
		}
	}
	bat.SetRowCount(len(accounts))
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func closedSignal() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func TestSubscriptionMetadataSourceOwnerAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		publisher, source, subscriber uint32
		accounts                      string
		allowed                       bool
	}{
		{"ordinary", 22, 22, 7, "all", true},
		{"system foreign third party", 0, 41, 7, "all", true},
		{"owner-only foreign", 22, 7, 7, "subscriber", true},
		{"duplicate owner names", 22, 7, 7, "subscriber,subscriber", true},
		{"canonical trimmed names", 22, 7, 7, "subscriber, subscriber ", true},
		{"system physical owner", 22, 0, 0, "sys", true},
		{"foreign third party", 22, 41, 7, "subscriber", false},
		{"foreign all targets", 22, 7, 7, "all", false},
		{"foreign wider targets", 22, 7, 7, "subscriber,another", false},
		{"withdrawn membership", 0, 41, 7, "another", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "subscriber"
			if tc.subscriber == 0 {
				name = "sys"
			}
			require.Equal(t, tc.allowed, subscriptionCandidateAuthorized(subscriptionCandidate{
				subscriberID: tc.subscriber, subscriberName: name,
				publisherID: tc.publisher, publisherName: "publisher", sourceAccountID: tc.source,
				accountList: tc.accounts,
			}))
		})
	}
}

func TestSubscriptionMetadataQueryBuilders(t *testing.T) {
	candidateSQL := buildSubscriptionAliasQuery(17, 91)
	require.Contains(t, candidateSQL, "d.account_id = 17")
	require.Contains(t, candidateSQL, "d.dat_id > 91")
	require.Contains(t, candidateSQL, "d.dat_type = "+sqlquote.String("subscription"))
	require.NotContains(t, candidateSQL, "s.status", "historical authorization must not filter aliases")
	require.NotContains(t, candidateSQL, "mo_pubs")
	authorized := buildAuthorizedSubscriptionQuery(17, []subscriptionAlias{{id: 99, name: "sub'alias", owner: 8, publisher: "pub'account", publication: "pub'name"}})
	require.Contains(t, authorized, "subscriber.account_id = 17")
	require.Contains(t, authorized, sqlquote.String("pub'account"))
	require.Contains(t, authorized, sqlquote.String("pub'name"))
	require.Contains(t, authorized, "publisher.status <> 'suspend'")
	require.Contains(t, authorized, "source.dat_id = p.database_id AND source.datname = p.database_name")
	require.Contains(t, authorized, "CASE WHEN p.database_id = 0 THEN p.account_id ELSE source.account_id END")
	require.Contains(t, authorized, "p.database_id = 0 OR source.dat_id IS NOT NULL", "a missing persisted source must not fall back to the publisher decoy")
	require.NotContains(t, candidateSQL, "p.all_table")
	require.Contains(t, candidateSQL, "LIMIT 64")

	candidate := subscriptionCandidate{
		subscriberID:      17,
		localDatabaseID:   99,
		localDatabaseName: "sub'alias",
		localOwner:        8,
		publisherID:       23,
		sourceAccountID:   41,
		sourceDatabase:    "source'db",
	}
	tablesSQL := buildSubscriptionTablesQuery(candidate, []string{"orders", "odd'name"})
	require.Contains(t, tablesSQL, "CAST(17 AS INT UNSIGNED) AS account_id")
	require.Contains(t, tablesSQL, "CAST(99 AS BIGINT UNSIGNED) AS reldatabase_id")
	require.Contains(t, tablesSQL, "CAST(8 AS INT UNSIGNED) AS owner")
	require.Contains(t, tablesSQL, "CAST(41 AS INT UNSIGNED) AS publisher_account_id")
	require.NotContains(t, tablesSQL, "CAST(23 AS INT UNSIGNED) AS publisher_account_id")
	require.Contains(t, tablesSQL, "CAST("+sqlquote.String("sub'alias")+" AS VARCHAR(5000))")
	require.Contains(t, tablesSQL, "tbl.reldatabase = "+sqlquote.String("source'db"))
	require.Contains(t, tablesSQL, sqlquote.String("odd'name"))

	columnsSQL := buildSubscriptionColumnsQuery(candidate, []string{"orders"})
	require.Contains(t, columnsSQL, "CAST(17 AS INT UNSIGNED) AS account_id")
	require.Contains(t, columnsSQL, "CAST("+sqlquote.String("sub'alias")+" AS VARCHAR(256))")
	require.Contains(t, columnsSQL, "CAST(mk.key_priority AS BIGINT) AS key_priority")
	require.Contains(t, columnsSQL, "JOIN mo_catalog.mo_tables mt")
	require.Contains(t, columnsSQL, "CAST(8 AS INT UNSIGNED) AS table_owner, mc.att_is_unsigned ")
	require.Contains(t, columnsSQL, "mc.att_relname IN ("+sqlquote.String("orders")+")")
	require.Contains(t, columnsSQL, "kt.relname IN ("+sqlquote.String("orders")+")")
	require.Contains(t, columnsSQL, "pt.relname IN ("+sqlquote.String("orders")+")")
	// Persisted V58 COLUMNS still consumes this producer on a newer CN.
	// Filtering View rows here would hide them until the tenant migration runs.
	require.NotContains(t, columnsSQL, "mt.relkind = 'v'")

	allTablesSQL := buildSubscriptionTablesQuery(candidate, nil)
	require.NotContains(t, allTablesSQL, "tbl.relname IN")
}

func TestSubscriptionMetadataAliasVisibility(t *testing.T) {
	original := subscriptionMetadataRunSQL
	t.Cleanup(func() { subscriptionMetadataRunSQL = original })
	for _, mode := range []string{"visible", "absent", "query failure", "bad type", "missing vector", "short vector", "null identity", "nonadvancing", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			data := sqlexec.NewSqlContext(t.Context(), "", nil, 7, nil)
			calls := 0
			subscriptionMetadataRunSQL = func(proc *sqlexec.SqlProcess, query string) (executor.Result, error) {
				calls++
				require.Same(t, data, proc.SqlCtx)
				require.Contains(t, query, "account_id = 7")
				require.Contains(t, query, "dat_id > 11")
				count := 1
				if mode == "absent" {
					count = 0
				}
				if mode == "oversized" {
					count = subscriptionCandidatePageSize + 1
				}
				start := uint64(12)
				if mode == "nonadvancing" {
					start = 11
				}
				result := makeSubscriptionAliasResult(t, mp, start, count)
				bat := result.Batches[0]
				if mode == "bad type" || mode == "short vector" || mode == "null identity" {
					bat.Vecs[0].Free(mp)
					typ := types.T_uint64
					if mode == "bad type" {
						typ = types.T_varchar
					}
					bat.Vecs[0] = vector.NewVec(typ.ToType())
					if mode == "bad type" {
						require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("12"), false, mp))
					} else if mode == "null identity" {
						require.NoError(t, vector.AppendFixed(bat.Vecs[0], uint64(12), true, mp))
					}
				}
				if mode == "missing vector" {
					bat.Vecs[4].Free(mp)
					bat.Vecs = bat.Vecs[:4]
				}
				if mode == "query failure" {
					return result, context.Canceled
				}
				return result, nil
			}
			aliases, _, err := readSubscriptionAliases(data, 7, 11)
			require.Equal(t, 1, calls)
			if mode == "visible" || mode == "absent" {
				require.NoError(t, err)
				if mode == "visible" {
					require.Equal(t, "sub_12", aliases[0].name)
				} else {
					require.Empty(t, aliases)
				}
			} else {
				require.Error(t, err)
			}
			require.Zero(t, mp.CurrNB(), "even malformed/failed alias results must be released")
		})
	}
}

type failedSubscriptionAuthorization struct{}

func (failedSubscriptionAuthorization) SubscriptionMetadataSnapshot(context.Context) (*client.SnapshotAdmission, error) {
	return nil, context.DeadlineExceeded
}

func TestSubscriptionMetadataAuthorizationFailureTerminatesProducer(t *testing.T) {
	ctx := context.WithValue(t.Context(), subscriptionmetadata.AuthorizationSnapshotKey{}, failedSubscriptionAuthorization{})
	stream := make(chan executor.Result, 1)
	errors := make(chan error, 1)
	done := make(chan struct{})
	runSubscriptionMetadataProducer(ctx, sqlexec.NewSqlContext(ctx, "", nil, 7, nil), 7, &subscriptionColumnsConfig, stream, errors, done)
	<-done
	_, open := <-stream
	require.False(t, open, "failed admission cannot expose historical success")
	require.ErrorIs(t, <-errors, context.DeadlineExceeded)
}

func TestSubscriptionMetadataCancelWaitsForAuthorizationTransaction(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	queryEntered, rollbackEntered, releaseRollback := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseRollback) })
	ctrl := gomock.NewController(t)
	sqlExec := executortest.NewMockSQLExecutor(ctrl)
	txnExec := executortest.NewMockTxnExecutor(ctrl)
	txnExec.EXPECT().Txn().Return(nil)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, existed := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
		}
	})
	var finished atomic.Int32
	sqlExec.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, callback func(executor.TxnExecutor) error, opts executor.Options) error {
			if opts.HasExistsTxn() || !opts.HasTxnIsolation() || opts.TxnIsolation() != txn.TxnIsolation_SI {
				return fmt.Errorf("authorization must own an independent SI transaction")
			}
			err := callback(txnExec)
			close(rollbackEntered)
			select {
			case <-releaseRollback:
			case <-ctx.Done():
			}
			finished.Add(1)
			return err
		})
	original := subscriptionMetadataRunSQL
	t.Cleanup(func() { subscriptionMetadataRunSQL = original })
	dataCtx, stopData := context.WithCancel(ctx)
	subscriptionMetadataRunSQL = func(*sqlexec.SqlProcess, string) (executor.Result, error) {
		close(queryEntered)
		<-dataCtx.Done()
		return executor.Result{}, dataCtx.Err()
	}
	state := subscriptionMetadataState{streaming: true, streamCh: make(chan executor.Result, 1), errCh: make(chan error, 1), streamDone: make(chan struct{}), streamCancel: stopData}
	stopped := make(chan struct{})
	startStop := sync.OnceFunc(func() { go func() { state.stopStreaming(); close(stopped) }() })
	t.Cleanup(func() {
		stopData()
		release()
		startStop()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("producer cleanup did not terminate")
		}
	})
	go runSubscriptionMetadataProducer(dataCtx, sqlexec.NewSqlContext(dataCtx, proc.GetService(), nil, 7, nil), 7, &subscriptionColumnsConfig, state.streamCh, state.errCh, state.streamDone)
	select {
	case <-queryEntered:
	case <-ctx.Done():
		t.Fatal("alias read was not entered")
	}
	startStop()
	select {
	case <-rollbackEntered:
	case <-ctx.Done():
		t.Fatal("authorization cleanup was not entered")
	}
	select {
	case <-stopped:
		t.Fatal("stop returned before authorization transaction cleanup")
	default:
	}
	require.Zero(t, finished.Load())
	release()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("stop did not join producer")
	}
	require.EqualValues(t, 1, finished.Load())
	require.False(t, state.streaming)
	require.Nil(t, state.streamCh)
}

func TestSubscriptionMetadataProducerFailureClosure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		partial bool
	}{
		{name: "complete"},
		{name: "authorization query"},
		{name: "authorization decode"},
		{name: "source before rows"},
		{name: "source after rows", partial: true},
		{name: "executor channel after rows", partial: true},
		{name: "transaction finish after rows", partial: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			proc.Ctx = defines.AttachAccountId(ctx, 7)
			t.Cleanup(cancel)
			mp := mpool.MustNewZero()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB(), "producer and queued Results must be released")
			})
			ctrl := gomock.NewController(t)
			// No Commit/Rollback expectation: neither borrowed operator may be closed here.
			dataTxn := mock_frontend.NewMockTxnOperator(ctrl)
			authorityTxn := mock_frontend.NewMockTxnOperator(ctrl)
			proc.Base.TxnOperator = dataTxn
			sqlExec := executortest.NewMockSQLExecutor(ctrl)
			txnExec := executortest.NewMockTxnExecutor(ctrl)
			txnExec.EXPECT().Txn().Return(authorityTxn)
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, existed := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
			t.Cleanup(func() {
				if existed {
					rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
				}
			})
			fault := fmt.Errorf("injected subscription metadata failure")
			var finished atomic.Int32
			var callbackErr error
			sqlExec.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, callback func(executor.TxnExecutor) error, opts executor.Options) error {
					defer finished.Add(1)
					if opts.HasExistsTxn() || !opts.HasTxnIsolation() || opts.TxnIsolation() != txn.TxnIsolation_SI {
						return fmt.Errorf("authorization must own an independent SI transaction")
					}
					callbackErr = callback(txnExec)
					if callbackErr == nil && tc.name == "transaction finish after rows" {
						return fault
					}
					return callbackErr
				})
			originalSQL, originalStream := subscriptionMetadataRunSQL, subscriptionMetadataRunStreamingSQL
			t.Cleanup(func() {
				subscriptionMetadataRunSQL, subscriptionMetadataRunStreamingSQL = originalSQL, originalStream
			})
			var aliases, authorizations, sources atomic.Int32
			subscriptionMetadataRunSQL = func(p *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				if !strings.Contains(sql, "mo_pubs") {
					if p.SqlCtx.TxnOperator != dataTxn || aliases.Add(1) != 1 {
						return executor.Result{}, fmt.Errorf("aliases must use the original data transaction exactly once")
					}
					return makeSubscriptionAliasResult(t, mp, 1, 1), nil
				}
				authorizations.Add(1)
				if p.SqlCtx.TxnOperator != authorityTxn {
					return executor.Result{}, fmt.Errorf("publication query must use authorization transaction")
				}
				result := makeSubscriptionCandidateResult(t, mp, 1, 1, "all")
				if tc.name == "authorization query" {
					return result, fault // A failed query may still return resources.
				}
				if tc.name == "authorization decode" {
					result.Batches[0].Vecs[0].Free(mp)
					result.Batches[0].Vecs[0] = vector.NewConstNull(types.T_varchar.ToType(), 1, mp)
				}
				return result, nil
			}
			releaseSource := make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseSource) })
			subscriptionMetadataRunStreamingSQL = func(ctx context.Context, p *sqlexec.SqlProcess, _ string, stream chan executor.Result, errors chan error) (executor.Result, error) {
				sources.Add(1)
				if p.SqlCtx.TxnOperator != dataTxn || p.AccountIDOverride == nil || *p.AccountIDOverride != 22 {
					return executor.Result{}, fmt.Errorf("source must retain data T and publisher identity")
				}
				if tc.name == "source before rows" {
					return executor.Result{}, fault
				}
				rows := 1
				if tc.partial {
					rows = subscriptionOutputBatchSize
				}
				// Constant vectors cross the real output boundary without a large fixture.
				bat := batch.NewWithSize(len(subscriptionTablesConfig.columnTypes))
				for i, typ := range subscriptionTablesConfig.columnTypes {
					bat.Vecs[i] = vector.NewConstNull(typ, rows, mp)
				}
				bat.Vecs[0].Free(mp)
				var err error
				bat.Vecs[0], err = vector.NewConstFixed(types.T_uint32.ToType(), uint32(7), rows, mp)
				if err != nil {
					bat.Clean(mp)
					return executor.Result{}, err
				}
				bat.SetRowCount(rows)
				result := executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
				select {
				case stream <- result:
				case <-ctx.Done():
					result.Close()
					return executor.Result{}, ctx.Err()
				}
				if !tc.partial {
					return executor.Result{}, nil
				}
				select {
				case <-releaseSource:
				case <-ctx.Done():
					return executor.Result{}, ctx.Err()
				}
				queued := makeSubscriptionTableSourceResult(t, mp, []uint32{7}, []uint32{9})
				select {
				case stream <- queued:
				case <-ctx.Done():
					queued.Close()
					return executor.Result{}, ctx.Err()
				}
				if tc.name == "executor channel after rows" {
					errors <- fault
					return executor.Result{}, nil
				}
				if tc.name == "transaction finish after rows" {
					return executor.Result{}, nil
				}
				return executor.Result{}, fault
			}
			state := &subscriptionMetadataState{config: &subscriptionTablesConfig}
			t.Cleanup(func() { cancel(); release(); state.free(nil, proc, false, nil) })
			tf := &TableFunction{Attrs: []string{"account_id"}}
			tf.ctr.retSchema = []types.Type{types.T_uint32.ToType()}
			err := state.start(tf, proc, 0, nil)
			if tc.partial || tc.name == "complete" {
				require.NoError(t, err)
				out, callErr := state.call(tf, proc)
				require.NoError(t, callErr)
				expectedRows := 1
				if tc.partial {
					expectedRows = subscriptionOutputBatchSize
				}
				require.Equal(t, expectedRows, out.Batch.RowCount())
				require.Equal(t, uint32(7), vector.GetFixedAtWithTypeCheck[uint32](out.Batch.Vecs[0], 0))
				release()
				select {
				case <-state.streamDone:
				case <-ctx.Done():
					t.Fatal("authorization transaction did not terminate")
				}
				out, err = state.call(tf, proc)
				if tc.name == "complete" {
					require.NoError(t, err)
					require.Nil(t, out.Batch)
				}
			}
			if tc.name == "authorization decode" {
				require.ErrorContains(t, err, "invalid vector")
			} else if tc.name != "complete" {
				require.ErrorIs(t, err, fault, "an error after output must not become successful EOF")
			}
			state.free(nil, proc, false, err)
			require.EqualValues(t, 1, finished.Load())
			require.EqualValues(t, 1, aliases.Load())
			require.EqualValues(t, 1, authorizations.Load())
			if strings.HasPrefix(tc.name, "authorization") {
				require.Zero(t, sources.Load())
			} else {
				require.EqualValues(t, 1, sources.Load())
			}
			if tc.name == "complete" || tc.name == "transaction finish after rows" {
				require.NoError(t, callbackErr)
			} else {
				require.Error(t, callbackErr, "independent transaction owner must observe scan failure")
			}
			require.False(t, state.streaming)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestSubscriptionMetadataRejectsCrossAccountCaller(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	state := &subscriptionMetadataState{config: &subscriptionTablesConfig}
	t.Cleanup(func() { state.free(nil, proc, false, nil) })
	tf := &TableFunction{
		Attrs:        []string{"account_id"},
		ScanSnapshot: &planpb.Snapshot{Tenant: &planpb.SnapshotTenant{TenantID: 9}},
	}
	tf.ctr.retSchema = []types.Type{types.T_uint32.ToType()}
	require.ErrorContains(t, state.start(tf, proc, 0, nil), "requires the system account")
	require.False(t, state.streaming)
	require.Nil(t, state.streamDone, "rejection must precede producer admission")
	account, err := defines.GetAccountId(proc.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), account)
}

func TestPublicationTableChunksAreBounded(t *testing.T) {
	names := make([]string, subscriptionTableFilterSize+2)
	for i := range names {
		names[i] = fmt.Sprintf("t%d", i)
	}
	candidate := subscriptionCandidate{tableList: strings.Join(names, ",")}

	var chunks [][]string
	require.NoError(t, forEachPublicationTableChunk(candidate, func(chunk []string) error {
		chunks = append(chunks, append([]string(nil), chunk...))
		return nil
	}))
	require.Len(t, chunks, 2)
	require.Len(t, chunks[0], subscriptionTableFilterSize)
	require.Len(t, chunks[1], 2)

	candidate.tableList = "valid,,invalid"
	require.ErrorContains(t, forEachPublicationTableChunk(candidate, func([]string) error { return nil }),
		"empty table name")
	candidate.tableList = ""
	require.ErrorContains(t, forEachPublicationTableChunk(candidate, func([]string) error { return nil }),
		"empty table list")

	chunks = nil
	candidate.tableList = "still_scoped"
	require.NoError(t, forEachPublicationTableChunk(candidate, func(chunk []string) error {
		chunks = append(chunks, append([]string(nil), chunk...))
		return nil
	}))
	require.Equal(t, [][]string{{"still_scoped"}}, chunks)

	chunks = nil
	candidate.tableList = pubsub.TableAll
	require.NoError(t, forEachPublicationTableChunk(candidate, func(chunk []string) error {
		chunks = append(chunks, chunk)
		return nil
	}))
	require.Equal(t, [][]string{nil}, chunks)
}

func TestDecodeSubscriptionCandidatesAndAuthorization(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	result := makeSubscriptionCandidateResult(t, mp, 10, 2, "subscriber,other")
	require.NoError(t, vector.SetFixedAtWithTypeCheck(result.Batches[0].Vecs[9], 1, uint32(41)))
	candidates, rows, lastID, err := decodeSubscriptionCandidates(result, 9)
	require.NoError(t, err)
	require.Equal(t, 2, rows)
	require.Equal(t, uint64(11), lastID)
	require.Len(t, candidates, 2)
	require.Equal(t, uint32(22), candidates[1].publisherID)
	require.Equal(t, uint32(41), candidates[1].sourceAccountID)
	require.False(t, subscriptionCandidateAuthorized(candidates[1]))
	require.Equal(t, "sub_10", candidates[0].localDatabaseName)
	require.True(t, subscriptionCandidateAuthorized(candidates[0]))
	result.Close()
	require.Equal(t, "sub_10", candidates[0].localDatabaseName, "decoded strings must outlive the result")
	require.Zero(t, mp.CurrNB())

	candidates[0].accountList = "another"
	require.False(t, subscriptionCandidateAuthorized(candidates[0]))
	candidates[0].accountList = "all"
	require.True(t, subscriptionCandidateAuthorized(candidates[0]))
	candidates[0].publisherName = candidates[0].subscriberName
	require.False(t, subscriptionCandidateAuthorized(candidates[0]))
	for _, mode := range []string{"null", "short", "type"} {
		t.Run(mode, func(t *testing.T) {
			result := makeSubscriptionCandidateResult(t, mp, 1, 1, "all")
			defer result.Close()
			bat := result.Batches[0]
			bat.Vecs[9].Free(mp)
			typ := types.T_uint32
			if mode == "type" {
				typ = types.T_varchar
			}
			bat.Vecs[9] = vector.NewVec(typ.ToType())
			if mode == "null" {
				require.NoError(t, vector.AppendFixed(bat.Vecs[9], uint32(0), true, mp))
			} else if mode == "type" {
				require.NoError(t, vector.AppendBytes(bat.Vecs[9], []byte("22"), false, mp))
			}
			_, _, _, err := decodeSubscriptionCandidates(result, 0)
			require.Error(t, err, "unknown physical ownership must not become publisher fallback")
		})
		require.Zero(t, mp.CurrNB())
	}
}

func TestSubscriptionMetadataProducerPaginatesAndUsesPhysicalSourceIdentity(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	originalRunSQL := subscriptionMetadataRunSQL
	originalRunStreamingSQL := subscriptionMetadataRunStreamingSQL
	t.Cleanup(func() {
		subscriptionMetadataRunSQL = originalRunSQL
		subscriptionMetadataRunStreamingSQL = originalRunStreamingSQL
	})

	firstPage := makeSubscriptionAliasResult(t, mp, 1, subscriptionCandidatePageSize)
	var mu sync.Mutex
	var candidateQueries []string
	candidateCalls := 0
	subscriptionMetadataRunSQL = func(proc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "mo_pubs") {
			if proc.SqlCtx.CNUuid != "authority" {
				return executor.Result{}, fmt.Errorf("publication eligibility must use A")
			}
			// The final alias has lost current eligibility; pagination must still
			// advance by the full historical alias page, not the admitted rows.
			result := makeSubscriptionCandidateResult(t, mp, 1, subscriptionCandidatePageSize-1, "all")
			// A sys publication now sources account 22. Its publisher must not
			// become the execution identity for the historical catalog scan.
			for row := 0; row < result.Batches[0].RowCount(); row++ {
				require.NoError(t, vector.SetFixedAtWithTypeCheck(result.Batches[0].Vecs[4], row, uint32(0)))
			}
			return result, nil
		}
		if proc.SqlCtx.CNUuid != "data" {
			return executor.Result{}, fmt.Errorf("aliases must use data T")
		}
		mu.Lock()
		defer mu.Unlock()
		candidateQueries = append(candidateQueries, sql)
		candidateCalls++
		if candidateCalls == 1 {
			return firstPage, nil
		}
		return executor.Result{}, nil
	}

	publisherCalls := 0
	subscriptionMetadataRunStreamingSQL = func(
		_ context.Context,
		sqlProcess *sqlexec.SqlProcess,
		sql string,
		_ chan executor.Result,
		_ chan error,
	) (executor.Result, error) {
		if sqlProcess.SqlCtx.CNUuid != "data" {
			return executor.Result{}, fmt.Errorf("source schema must remain at data T")
		}
		if sqlProcess.AccountIDOverride == nil || *sqlProcess.AccountIDOverride != 22 {
			return executor.Result{}, fmt.Errorf("unexpected physical source identity")
		}
		if !strings.Contains(sql, "CAST(7 AS INT UNSIGNED) AS account_id") {
			return executor.Result{}, fmt.Errorf("subscriber identity missing from source query")
		}
		publisherCalls++
		return executor.Result{}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	authority := sqlexec.NewSqlContext(ctx, "authority", nil, 7, nil)
	data := sqlexec.NewSqlContext(ctx, "data", nil, 7, nil)
	streamCh := make(chan executor.Result, subscriptionResultBufferSize)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(streamCh)
		errCh <- scanSubscriptionMetadata(ctx, authority, data, 7, &subscriptionTablesConfig, streamCh)
	}()
	for result := range streamCh {
		result.Close()
	}
	<-done

	require.Equal(t, subscriptionCandidatePageSize-1, publisherCalls)
	require.Len(t, candidateQueries, 2)
	require.Contains(t, candidateQueries[1], "d.dat_id > 64")
	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
	require.Zero(t, mp.CurrNB(), "candidate pages must be closed before the producer exits")
}

func TestSubscriptionMetadataStateProjectionAndLimit(t *testing.T) {
	proc := testutil.NewProc(t)
	internalMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(internalMP)

	newState := func(result executor.Result, limited bool) *subscriptionMetadataState {
		output := batch.NewWithSize(2)
		output.Attrs = []string{"owner", "account_id"}
		output.Vecs[0] = vector.NewVec(types.T_uint32.ToType())
		output.Vecs[1] = vector.NewVec(types.T_uint32.ToType())
		streamCh := make(chan executor.Result, 1)
		streamCh <- result
		close(streamCh)
		return &subscriptionMetadataState{
			batch:            output,
			config:           &subscriptionTablesConfig,
			outputSources:    []int{12, 0},
			streaming:        true,
			limited:          limited,
			limit:            1,
			streamCh:         streamCh,
			errCh:            make(chan error, 1),
			streamDone:       closedSignal(),
			streamCancel:     func() {},
			currentResult:    nil,
			currentRowOffset: 0,
		}
	}

	state := newState(makeSubscriptionTableSourceResult(t, internalMP, []uint32{7}, []uint32{9}), false)
	require.NoError(t, state.fillBatch(proc))
	require.Equal(t, []uint32{9}, vector.MustFixedColWithTypeCheck[uint32](state.batch.Vecs[0]))
	require.Equal(t, []uint32{7}, vector.MustFixedColWithTypeCheck[uint32](state.batch.Vecs[1]))
	require.NoError(t, state.end(nil, proc))
	require.Zero(t, internalMP.CurrNB())
	state.free(nil, proc, false, nil)

	state = newState(
		makeSubscriptionTableSourceResult(t, internalMP, []uint32{7, 8}, []uint32{9, 10}),
		true,
	)
	require.NoError(t, state.fillBatch(proc))
	require.Equal(t, 1, state.batch.RowCount())
	require.Equal(t, []uint32{9}, vector.MustFixedColWithTypeCheck[uint32](state.batch.Vecs[0]))
	require.False(t, state.streaming, "reaching the pushed limit must join the producer immediately")
	require.Zero(t, internalMP.CurrNB())
	state.free(nil, proc, false, nil)
}

func TestSubscriptionMetadataClosedStreamPreservesCallerCancellation(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()

	streamCh := make(chan executor.Result)
	close(streamCh)
	state := &subscriptionMetadataState{
		streamCh: streamCh,
		errCh:    make(chan error, 1),
	}

	for i := 0; i < 100; i++ {
		err := state.readStreamResult(proc)
		require.ErrorIsf(t, err, context.Canceled,
			"iteration %d reported caller cancellation as clean EOF", i)
	}
}

func TestSubscriptionMetadataStateMalformedResultClosesResources(t *testing.T) {
	proc := testutil.NewProc(t)
	internalMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(internalMP)
	malformed := batch.NewWithSize(1)
	malformed.Vecs[0] = vector.NewVec(types.T_uint32.ToType())
	require.NoError(t, vector.AppendFixed(malformed.Vecs[0], uint32(7), false, internalMP))
	malformed.SetRowCount(1)
	result := executor.Result{Mp: internalMP, Batches: []*batch.Batch{malformed}}

	output := batch.NewWithSize(1)
	output.Attrs = []string{"account_id"}
	output.Vecs[0] = vector.NewVec(types.T_uint32.ToType())
	streamCh := make(chan executor.Result, 1)
	streamCh <- result
	close(streamCh)
	state := &subscriptionMetadataState{
		batch:         output,
		config:        &subscriptionTablesConfig,
		outputSources: []int{0},
		streaming:     true,
		streamCh:      streamCh,
		errCh:         make(chan error, 1),
		streamDone:    closedSignal(),
		streamCancel:  func() {},
	}

	err := state.fillBatch(proc)
	require.ErrorContains(t, err, "returned 1 columns")
	state.stopStreaming()
	require.Zero(t, internalMP.CurrNB())
	state.free(nil, proc, false, nil)
}

func TestSubscriptionMetadataStopStreamingCancelsAndDrains(t *testing.T) {
	internalMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(internalMP)
	results := make([]executor.Result, 3)
	for i := range results {
		results[i] = makeSubscriptionTableSourceResult(t, internalMP, []uint32{7}, []uint32{9})
	}
	ctx, cancel := context.WithCancel(context.Background())
	streamCh := make(chan executor.Result, 1)
	done := make(chan struct{})
	producerStarted := make(chan struct{})
	go func() {
		defer close(done)
		defer close(streamCh)
		close(producerStarted)
		for i, result := range results {
			select {
			case streamCh <- result:
			case <-ctx.Done():
				for _, remaining := range results[i:] {
					remaining.Close()
				}
				return
			}
		}
	}()
	<-producerStarted

	state := &subscriptionMetadataState{
		streaming:    true,
		streamCh:     streamCh,
		errCh:        make(chan error, 1),
		streamDone:   done,
		streamCancel: cancel,
	}
	state.stopStreaming()
	require.False(t, state.streaming)
	require.Zero(t, internalMP.CurrNB(), "cancel cleanup must close queued and producer-owned results")
}
