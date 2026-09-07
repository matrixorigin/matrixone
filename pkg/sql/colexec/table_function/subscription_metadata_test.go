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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
	}
	bat.SetRowCount(count)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
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

func TestSubscriptionMetadataQueryBuilders(t *testing.T) {
	candidateSQL := buildSubscriptionCandidateQuery(17, 91)
	require.Contains(t, candidateSQL, "d.account_id = 17")
	require.Contains(t, candidateSQL, "d.dat_id > 91")
	require.Contains(t, candidateSQL, "d.dat_type = "+sqlquote.String("subscription"))
	require.Contains(t, candidateSQL, "s.status = 0")
	require.NotContains(t, candidateSQL, "p.all_table")
	require.Contains(t, candidateSQL, "LIMIT 64")

	candidate := subscriptionCandidate{
		subscriberID:      17,
		localDatabaseID:   99,
		localDatabaseName: "sub'alias",
		localOwner:        8,
		publisherID:       23,
		sourceDatabase:    "source'db",
	}
	tablesSQL := buildSubscriptionTablesQuery(candidate, []string{"orders", "odd'name"})
	require.Contains(t, tablesSQL, "CAST(17 AS INT UNSIGNED) AS account_id")
	require.Contains(t, tablesSQL, "CAST(99 AS BIGINT UNSIGNED) AS reldatabase_id")
	require.Contains(t, tablesSQL, "CAST(8 AS INT UNSIGNED) AS owner")
	require.Contains(t, tablesSQL, "CAST("+sqlquote.String("sub'alias")+" AS VARCHAR(5000))")
	require.Contains(t, tablesSQL, "tbl.reldatabase = "+sqlquote.String("source'db"))
	require.Contains(t, tablesSQL, sqlquote.String("odd'name"))

	columnsSQL := buildSubscriptionColumnsQuery(candidate, []string{"orders"})
	require.Contains(t, columnsSQL, "CAST(17 AS INT UNSIGNED) AS account_id")
	require.Contains(t, columnsSQL, "CAST("+sqlquote.String("sub'alias")+" AS VARCHAR(256))")
	require.Contains(t, columnsSQL, "CAST(mk.key_priority AS BIGINT) AS key_priority")
	require.Contains(t, columnsSQL, "JOIN mo_catalog.mo_tables mt")
	require.Contains(t, columnsSQL, "CAST(8 AS INT UNSIGNED) AS table_owner")
	require.Contains(t, columnsSQL, "mc.att_relname IN ("+sqlquote.String("orders")+")")
	require.Contains(t, columnsSQL, "kt.relname IN ("+sqlquote.String("orders")+")")
	require.Contains(t, columnsSQL, "pt.relname IN ("+sqlquote.String("orders")+")")

	allTablesSQL := buildSubscriptionTablesQuery(candidate, nil)
	require.NotContains(t, allTablesSQL, "tbl.relname IN")
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
	candidates, rows, lastID, err := decodeSubscriptionCandidates(result, 9)
	require.NoError(t, err)
	require.Equal(t, 2, rows)
	require.Equal(t, uint64(11), lastID)
	require.Len(t, candidates, 2)
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
}

func TestSubscriptionMetadataProducerPaginatesAndUsesPublisherIdentity(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	originalRunSQL := subscriptionMetadataRunSQL
	originalRunStreamingSQL := subscriptionMetadataRunStreamingSQL
	t.Cleanup(func() {
		subscriptionMetadataRunSQL = originalRunSQL
		subscriptionMetadataRunStreamingSQL = originalRunStreamingSQL
	})

	firstPage := makeSubscriptionCandidateResult(t, mp, 1, subscriptionCandidatePageSize, "all")
	var mu sync.Mutex
	var candidateQueries []string
	candidateCalls := 0
	subscriptionMetadataRunSQL = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
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
		if sqlProcess.AccountIDOverride == nil || *sqlProcess.AccountIDOverride != 22 {
			return executor.Result{}, fmt.Errorf("unexpected publisher identity")
		}
		if !strings.Contains(sql, "CAST(7 AS INT UNSIGNED) AS account_id") {
			return executor.Result{}, fmt.Errorf("subscriber identity missing from source query")
		}
		publisherCalls++
		return executor.Result{}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sqlContext := sqlexec.NewSqlContext(ctx, "", nil, 7, nil)
	streamCh := make(chan executor.Result, subscriptionResultBufferSize)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go runSubscriptionMetadataProducer(
		ctx, sqlContext, 7, &subscriptionTablesConfig, streamCh, errCh, done)
	for result := range streamCh {
		result.Close()
	}
	<-done

	require.Equal(t, subscriptionCandidatePageSize, publisherCalls)
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
