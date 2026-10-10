// Copyright 2022 Matrix Origin
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

package search

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftplan "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/plan"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// stubLimitSql answers the __DocLen count with a count-only batch, rejects a
// window-function top-k query and any other SQL, and streams rows for the
// streaming query. It records every SQL and the membership filter of the
// streaming query.
type stubLimitSql struct {
	sqls             []string
	streamed         bool
	streamingSQL     string
	streamingFilter  []byte
	streamingResults func(proc *sqlexec.SqlProcess) []*batch.Batch
}

func newStubLimitSql(t *testing.T) *stubLimitSql {
	s := &stubLimitSql{
		streamingResults: func(sqlproc *sqlexec.SqlProcess) []*batch.Batch {
			return []*batch.Batch{makeSmallTextBatchFT(sqlproc.Proc)}
		},
	}
	prevSQL, prevStreaming := RunSql, RunStreamingSql
	t.Cleanup(func() { RunSql, RunStreamingSql = prevSQL, prevStreaming })
	RunSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		s.sqls = append(s.sqls, sql)
		switch {
		case strings.Contains(sql, "COUNT(*) OVER()"):
			return executor.Result{}, moerr.NewInternalError(sqlproc.Proc.Ctx, "top-k SQL must not use a window function")
		case strings.Contains(sql, "word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	RunStreamingSql = func(ctx context.Context, sqlproc *sqlexec.SqlProcess, sql string, ch chan executor.Result, errCh chan error) (executor.Result, error) {
		s.sqls = append(s.sqls, sql)
		s.streamed = true
		s.streamingSQL = sql
		s.streamingFilter = append([]byte(nil), sqlproc.FulltextMembershipFilter...)
		if bats := s.streamingResults(sqlproc); len(bats) > 0 {
			ch <- executor.Result{Mp: sqlproc.Proc.Mp(), Batches: bats}
		}
		return executor.Result{}, nil
	}
	return s
}

// readCounts reads r to its end and returns the row count of every batch.
func readCounts(t *testing.T, r *reader, mp *mpool.MPool, out *batch.Batch) []int {
	t.Helper()
	var counts []int
	for {
		end, err := r.Read(context.Background(), nil, nil, mp, out)
		require.NoError(t, err)
		if end {
			return counts
		}
		counts = append(counts, out.RowCount())
	}
}

// requireCloseReturns fails when Close does not return within a second.
func requireCloseReturns(t *testing.T, close func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not return")
	}
}

func classicReader(t *testing.T, mp *mpool.MPool, algo fulltext.FullTextScoreAlgo, mode int64, req searchplugin.Request) *reader {
	t.Helper()
	proc := newFTTestProcess(t, mp, algo)
	spec := classicSpec(t, ftplan.ScanOptions{SourceTable: "src_table", IndexTable: "index_table", Mode: mode})
	r, err := Hooks{}.NewReader(proc, spec, req)
	require.NoError(t, err)
	return r.(*reader)
}

func docOnlyBatch() *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	return bat
}

// An unlimited search emits full 8192-row batches, then the remainder, for
// either score algorithm and with or without the score column.
func TestClassicReadBatches(t *testing.T) {
	for _, tc := range []struct {
		name string
		algo fulltext.FullTextScoreAlgo
		out  func() *batch.Batch
	}{
		{"tfidf", fulltext.ALGO_TFIDF, docScoreBatch},
		{"tfidf-doc-id-only", fulltext.ALGO_TFIDF, docOnlyBatch},
		{"bm25", fulltext.ALGO_BM25, docScoreBatch},
		{"bm25-doc-id-only", fulltext.ALGO_BM25, docOnlyBatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			stubClassicSql(t)
			r := classicReader(t, mp, tc.algo, 0, searchplugin.Request{QueryPayload: []byte("pattern")})
			require.Equal(t, []int{8192, 8192, 8192, 1}, readCounts(t, r, mp, tc.out()))
			requireCloseReturns(t, r.Close)
		})
	}
}

// LIMIT BY RANK keeps three times the limit, emitted in limit-sized batches.
func TestClassicScanLimitByRank(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := newFTTestProcess(t, mp, fulltext.ALGO_TFIDF)
	stubClassicSql(t)

	scan := NewScan(128)
	scan.ResetRowState(proc)
	scan.SetRanking(true)
	require.NoError(t, scan.Match(proc, "src_table", "index_table", "pattern", 0, "{}", fulltext.ALGO_TFIDF))
	out := docScoreBatch()
	for i := 0; i < 192; i++ {
		result, err := scan.Call(proc, out)
		require.NoError(t, err)
		require.Equal(t, vm.ExecNext, result.Status)
		require.Equal(t, 128, result.Batch.RowCount())
	}
	result, err := scan.Call(proc, out)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 1, result.Batch.RowCount())
	result, err = scan.Call(proc, out)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	scan.Free(proc)
}

// Closing a search before or in the middle of its result returns.
func TestClassicCloseEarly(t *testing.T) {
	mp := mpool.MustNewZero()
	stubClassicSql(t)

	unread := classicReader(t, mp, fulltext.ALGO_TFIDF, 0, searchplugin.Request{QueryPayload: []byte("pattern")})
	requireCloseReturns(t, unread.Close)

	partial := classicReader(t, mp, fulltext.ALGO_TFIDF, 0, searchplugin.Request{QueryPayload: []byte("pattern")})
	end, err := partial.Read(context.Background(), nil, nil, mp, docOnlyBatch())
	require.NoError(t, err)
	require.False(t, end)
	requireCloseReturns(t, partial.Close)
}

// A limited search whose pattern the top-k SQL cannot answer streams instead,
// without a window-function query.
func TestClassicLimitFallsBackToStreaming(t *testing.T) {
	for _, tc := range []struct {
		name    string
		algo    fulltext.FullTextScoreAlgo
		pattern string
		mode    int64
	}{
		{"single-keyword-tfidf-natural-language", fulltext.ALGO_TFIDF, "Matrix", int64(tree.FULLTEXT_NL)},
		{"single-keyword-tfidf-default", fulltext.ALGO_TFIDF, "Matrix", int64(tree.FULLTEXT_DEFAULT)},
		{"single-keyword-bm25-natural-language", fulltext.ALGO_BM25, "Matrix", int64(tree.FULLTEXT_NL)},
		{"single-keyword-bm25-default", fulltext.ALGO_BM25, "Matrix", int64(tree.FULLTEXT_DEFAULT)},
		{"boolean", fulltext.ALGO_TFIDF, "+Matrix", int64(tree.FULLTEXT_BOOLEAN)},
		{"phrase", fulltext.ALGO_TFIDF, "Matrix Origin", 0},
		{"quoted-phrase", fulltext.ALGO_TFIDF, "\"Matrix Origin\"", int64(tree.FULLTEXT_BOOLEAN)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			stub := newStubLimitSql(t)
			r := classicReader(t, mp, tc.algo, tc.mode, searchplugin.Request{QueryPayload: []byte(tc.pattern), ResultLimit: 2})
			require.Equal(t, []int{2}, readCounts(t, r, mp, docScoreBatch()))
			require.True(t, stub.streamed)
			require.NotContains(t, stub.streamingSQL, "COUNT(*) OVER()")
			requireCloseReturns(t, r.Close)
		})
	}
}

// A limited search that matches nothing streams and ends without rows.
func TestClassicLimitZeroMatchStreams(t *testing.T) {
	mp := mpool.MustNewZero()
	stub := newStubLimitSql(t)
	stub.streamingResults = func(*sqlexec.SqlProcess) []*batch.Batch { return nil }
	r := classicReader(t, mp, fulltext.ALGO_TFIDF, int64(tree.FULLTEXT_NL), searchplugin.Request{QueryPayload: []byte("Matrix"), ResultLimit: 2})
	require.Empty(t, readCounts(t, r, mp, docScoreBatch()))
	require.True(t, stub.streamed)
	requireCloseReturns(t, r.Close)
}

// The membership filter of the search reaches the streaming SQL.
func TestClassicLimitPropagatesMembershipFilterToStreamingSQL(t *testing.T) {
	mp := mpool.MustNewZero()
	stub := newStubLimitSql(t)

	keys := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed[int32](keys, 7, false, mp))
	data, err := keys.MarshalBinary()
	require.NoError(t, err)

	r := classicReader(t, mp, fulltext.ALGO_TFIDF, 0, searchplugin.Request{
		QueryPayload: []byte("pattern"), ResultLimit: 2, HasMembershipFilter: true, MembershipFilter: data,
	})
	require.Equal(t, []int{2}, readCounts(t, r, mp, docScoreBatch()))
	require.NotEmpty(t, stub.streamingFilter)
	require.Equal(t, r.scan.MembershipFilter(), stub.streamingFilter)
	requireCloseReturns(t, r.Close)
}

// A runtime filter that is not a membership filter, or an empty one, leaves the
// search unfiltered.
func TestClassicNonMembershipFilterLeavesScanUnfiltered(t *testing.T) {
	for _, req := range []searchplugin.Request{
		{QueryPayload: []byte("apple")},
		{QueryPayload: []byte("apple"), HasMembershipFilter: true},
	} {
		mp := mpool.MustNewZero()
		stubClassicSql(t)
		r := classicReader(t, mp, fulltext.ALGO_TFIDF, int64(tree.FULLTEXT_NL), req)
		_, err := r.Read(context.Background(), nil, nil, mp, docScoreBatch())
		require.NoError(t, err)
		require.Nil(t, r.scan.MembershipFilter())
		requireCloseReturns(t, r.Close)
	}
}

// A Scan reused for a later search starts from a clean state.
func TestClassicScanResetsStateForLaterSearch(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := newFTTestProcess(t, mp, fulltext.ALGO_TFIDF)
	stub := newStubLimitSql(t)

	scan := NewScan(0)
	out := docScoreBatch()
	for _, idx := range []string{"idx0", "idx1"} {
		scan.ResetRowState(proc)
		require.NoError(t, scan.Match(proc, "src", idx, "Matrix", int64(tree.FULLTEXT_NL), "{}", fulltext.ALGO_TFIDF))
		result, err := scan.Call(proc, out)
		require.NoError(t, err)
		require.Equal(t, vm.ExecNext, result.Status)
		result, err = scan.Call(proc, out)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)
	}
	require.Len(t, stub.sqls, 4)
	require.Contains(t, stub.sqls[0], "idx0")
	require.Contains(t, stub.sqls[1], "idx0")
	require.Contains(t, stub.sqls[2], "idx1")
	require.Contains(t, stub.sqls[3], "idx1")
	scan.Free(proc)
}

// An empty or NULL pattern is rejected before any SQL runs and leaves the scan
// idle; a later search still runs.
func TestClassicRejectsInvalidPattern(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    int64
		valid   string
		req     searchplugin.Request
		wantErr string
	}{
		{"boolean-empty", int64(tree.FULLTEXT_BOOLEAN), "+Matrix", searchplugin.Request{}, "must not be empty"},
		{"boolean-null", int64(tree.FULLTEXT_BOOLEAN), "+Matrix", searchplugin.Request{QueryIsNull: true}, "must not be NULL"},
		{"natural-language-empty", int64(tree.FULLTEXT_NL), "Matrix", searchplugin.Request{}, "must not be empty"},
		{"natural-language-null", int64(tree.FULLTEXT_NL), "Matrix", searchplugin.Request{QueryIsNull: true}, "must not be NULL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			stub := newStubLimitSql(t)

			bad := classicReader(t, mp, fulltext.ALGO_TFIDF, tc.mode, tc.req)
			var err error
			require.NotPanics(t, func() {
				_, err = bad.Read(context.Background(), nil, nil, mp, docScoreBatch())
			})
			require.ErrorContains(t, err, tc.wantErr)
			require.True(t, bad.scan.Idle())
			require.Empty(t, stub.sqls)
			requireCloseReturns(t, bad.Close)

			good := classicReader(t, mp, fulltext.ALGO_TFIDF, tc.mode, searchplugin.Request{QueryPayload: []byte(tc.valid)})
			require.Equal(t, []int{2}, readCounts(t, good, mp, docScoreBatch()))
			require.Len(t, stub.sqls, 2)
			requireCloseReturns(t, good.Close)
		})
	}
}

// An unsafe zero-relevance guard refuses a NULL-pattern search before the
// pattern error.
func TestClassicReadGuardPrecedesNullPattern(t *testing.T) {
	mp := mpool.MustNewZero()
	stub := newStubLimitSql(t)
	r := classicReader(t, mp, fulltext.ALGO_TFIDF, 0, searchplugin.Request{
		QueryIsNull: true, AlgoValues: []searchplugin.AlgoValue{boolGuard(true)},
	})
	_, err := r.Read(context.Background(), nil, nil, mp, docScoreBatch())
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "got %v", err)
	require.Empty(t, stub.sqls)
	requireCloseReturns(t, r.Close)
}
