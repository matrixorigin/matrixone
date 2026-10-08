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

package search

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func int64ColumnBuffer(vals ...int64) *vectorindex.ColumnBuffer {
	cb := &vectorindex.ColumnBuffer{Type: types.T_int64}
	var b [8]byte
	for _, v := range vals {
		binary.LittleEndian.PutUint64(b[:], uint64(v))
		cb.Data = append(cb.Data, b[:]...)
	}
	cb.N = len(vals)
	return cb
}

// pkScoreBatch is a [doc_id int64, score float32] output batch.
func pkScoreBatch() *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	return bat
}

func scanSpec(t *testing.T, opts ft2plan.ScanOptions) *plan.IndexSearchScan {
	data, err := ft2plan.EncodeScanOptions(opts)
	require.NoError(t, err)
	return &plan.IndexSearchScan{
		Index:       &plan.IndexDef{IndexAlgo: catalog.MoIndexFullText2Algo.ToString()},
		AlgoOptions: data,
	}
}

func guardRequest(guard *plan.Literal) searchplugin.Request {
	return searchplugin.Request{AlgoValues: []searchplugin.AlgoValue{{Name: fulltext.ZeroRelevanceGuardExpr, Value: guard}}}
}

func TestNewReaderValidatesItsInput(t *testing.T) {
	proc := testutil.NewProc(t)
	_, err := Hooks{}.NewReader(nil, scanSpec(t, ft2plan.ScanOptions{}), searchplugin.Request{})
	require.ErrorContains(t, err, "requires a process")
	_, err = Hooks{}.NewReader(proc, nil, searchplugin.Request{})
	require.ErrorContains(t, err, "missing its specification")
	_, err = Hooks{}.NewReader(proc, &plan.IndexSearchScan{}, searchplugin.Request{})
	require.ErrorContains(t, err, "no scan options")
	_, err = Hooks{}.NewReader(proc, &plan.IndexSearchScan{AlgoOptions: []byte("{")}, searchplugin.Request{})
	require.ErrorContains(t, err, "invalid fulltext2 scan options")

	r, err := Hooks{}.NewReader(proc, scanSpec(t, ft2plan.ScanOptions{Config: "{}"}), searchplugin.Request{ResultLimit: 7})
	require.NoError(t, err)
	u := r.(*reader)
	require.Equal(t, uint64(7), u.limit)
	require.Equal(t, uint64(7), u.plannedLimit)
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
	end, err := r.Read(context.Background(), nil, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, end, "a closed reader is at its end")
}

func TestEmptyScanChecksTheGuard(t *testing.T) {
	proc := testutil.NewProc(t)
	require.NoError(t, Hooks{}.EmptyScan(proc, nil, searchplugin.Request{}))
	require.NoError(t, Hooks{}.EmptyScan(proc, nil, guardRequest(&plan.Literal{Isnull: true})))
	require.NoError(t, Hooks{}.EmptyScan(proc, nil, guardRequest(&plan.Literal{Value: &plan.Literal_Bval{Bval: false}})))
	err := Hooks{}.EmptyScan(proc, nil, guardRequest(&plan.Literal{Value: &plan.Literal_Bval{Bval: true}}))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	err = Hooks{}.EmptyScan(proc, nil, guardRequest(&plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}))
	require.ErrorContains(t, err, "guard must be bool")
}

func TestInitMapsOutputsByName(t *testing.T) {
	proc := testutil.NewProc(t)
	u := &reader{proc: proc}
	require.ErrorContains(t, u.init(nil), "config is empty")
	u.opts.Config = "{bad"
	require.Error(t, u.init(nil))

	u.opts.Config = `{"index":"__idx","include_columns":["Tag","prio"]}`
	attrs := []string{"prio", catalog.FullText2Search_OutCol_Score, "unknown", catalog.FullText2Search_OutCol_DocId}
	require.NoError(t, u.init(attrs))
	require.Equal(t, 3, u.pkVecIdx)
	require.Equal(t, 1, u.scoreVecIdx)
	require.Equal(t, []includeOut{
		{vecIdx: 0, segPos: 1, name: "prio"},
		{vecIdx: 2, segPos: -1, name: "unknown"},
	}, u.includeOut)
	require.Equal(t, []string{"prio", "unknown"}, u.includeNames)
}

func TestApplyMembership(t *testing.T) {
	proc := testutil.NewProc(t)
	keys := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](keys, 5, false, proc.Mp()))
	data, err := keys.MarshalBinary()
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		req        searchplugin.Request
		wantLimit  uint64
		wantFilter bool
	}{
		{"no membership filter keeps the limit", searchplugin.Request{}, 10, false},
		{"exact keys filter inside the search", searchplugin.Request{HasMembershipFilter: true, MembershipFilter: data}, 10, true},
		{"a membership filter without keys streams", searchplugin.Request{HasMembershipFilter: true}, 0, false},
		{"a passed membership filter streams", searchplugin.Request{MembershipFilterPassed: true}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := &reader{proc: proc, req: tc.req, limit: 10}
			require.NoError(t, u.applyMembership())
			require.Equal(t, tc.wantLimit, u.limit)
			require.Equal(t, tc.wantFilter, len(u.filterBytes) > 0)
		})
	}

	u := &reader{proc: proc, req: searchplugin.Request{HasMembershipFilter: true, MembershipFilter: []byte{1}}}
	require.Error(t, u.applyMembership(), "a malformed key payload is rejected")
}

func TestStartRejectsBeforeSearching(t *testing.T) {
	proc := testutil.NewProc(t)
	unsafe := &reader{proc: proc, spec: &plan.IndexSearchScan{},
		req: guardRequest(&plan.Literal{Value: &plan.Literal_Bval{Bval: true}})}
	require.True(t, moerr.IsMoErrCode(unsafe.start(), moerr.ErrNotSupported))

	// A malformed pushed score range is rejected before any index access. The planner only
	// ever emits this option itself, so a bad value means the plan and the engine disagree
	// about the encoding -- fail loudly rather than silently searching without the bound.
	badRange := &reader{proc: proc, spec: &plan.IndexSearchScan{}, opts: ft2plan.ScanOptions{ScoreRange: "{not json"}}
	require.ErrorContains(t, badRange.start(), "invalid score range")
}

func TestReadReportsStartErrors(t *testing.T) {
	proc := testutil.NewProc(t)
	r := &reader{proc: proc, spec: &plan.IndexSearchScan{}}
	_, err := r.Read(context.Background(), nil, nil, proc.Mp(), pkScoreBatch())
	require.ErrorContains(t, err, "config is empty")

	r = &reader{proc: proc, spec: &plan.IndexSearchScan{},
		opts: ft2plan.ScanOptions{Config: "{}", ScoreRange: "{not json"}}
	_, err = r.Read(context.Background(), nil, nil, proc.Mp(), pkScoreBatch())
	require.ErrorContains(t, err, "invalid score range")
}

// TestReadMaterialized drives the non-streaming path: a SearchInto result is paged into
// output batches, then the reader ends.
func TestReadMaterialized(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	u := &reader{
		proc: proc, inited: true, started: true, limit: 10,
		out: &vectorindex.SearchOutput{
			Keys:  int64ColumnBuffer(11, 22, 33),
			Dists: []float32{0.5, 0.4, 0.3},
		},
		pkVecIdx: 0, scoreVecIdx: 1,
	}
	out := pkScoreBatch()
	end, err := u.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, 3, out.RowCount())
	require.Equal(t, int64(11), vector.GetFixedAtWithTypeCheck[int64](out.Vecs[0], 0))
	require.Equal(t, float32(0.3), vector.GetFixedAtWithTypeCheck[float32](out.Vecs[1], 2))

	end, err = u.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.True(t, end)
	require.NoError(t, u.Close())
}

// TestReadMaterializedCovered drives the COVERED non-streaming path: the include column is
// bulk-appended by segPos, NULL-aware.
func TestReadMaterializedCovered(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	// One include col ("prio" int64): [10, NULL, 30].
	prio := &vectorindex.ColumnBuffer{Type: types.T_int64}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 10)
	prio.Data = append(prio.Data, b[:]...)
	prio.Data = append(prio.Data, make([]byte, 8)...) // NULL placeholder
	binary.LittleEndian.PutUint64(b[:], 30)
	prio.Data = append(prio.Data, b[:]...)
	prio.N = 3
	prio.Nulls = []bool{false, true, false}

	u := &reader{
		proc: proc, inited: true, started: true, limit: 10,
		out: &vectorindex.SearchOutput{
			Keys:    int64ColumnBuffer(11, 22, 33),
			Dists:   []float32{0.5, 0.4, 0.3},
			Include: []*vectorindex.ColumnBuffer{prio},
		},
		pkVecIdx: 0, scoreVecIdx: 1,
		includeOut: []includeOut{{vecIdx: 2, segPos: 0, name: "prio"}, {vecIdx: 3, segPos: 5, name: "gone"}},
	}
	out := batch.NewWithSize(4)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	out.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	out.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	out.Vecs[3] = vector.NewVec(types.T_int64.ToType())
	end, err := u.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, 3, out.RowCount())
	require.Equal(t, int64(10), vector.GetFixedAtWithTypeCheck[int64](out.Vecs[2], 0))
	require.True(t, out.Vecs[2].IsNull(1))
	require.Equal(t, int64(30), vector.GetFixedAtWithTypeCheck[int64](out.Vecs[2], 2))
	require.True(t, out.Vecs[3].IsNull(0), "an include without a segment position is NULL")
}

// TestReadWithoutSearchOutputEnds: a reader whose search produced no output ends instead of
// dereferencing it.
func TestReadWithoutSearchOutputEnds(t *testing.T) {
	proc := testutil.NewProc(t)
	u := &reader{proc: proc, inited: true, started: true, limit: 10, pkVecIdx: 0, scoreVecIdx: 1}
	end, err := u.Read(context.Background(), nil, nil, proc.Mp(), pkScoreBatch())
	require.NoError(t, err)
	require.True(t, end)
}

func streamingReader(proc *process.Process) *reader {
	return &reader{
		proc: proc, inited: true, started: true, streaming: true,
		pkVecIdx: 0, scoreVecIdx: 1,
		streamCh: make(chan *vectorindex.SearchOutput, 4),
		errCh:    make(chan error, 1),
	}
}

func TestReadStreaming(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	// happy path: one batch then a clean (nil-error) close.
	u := streamingReader(proc)
	u.streamCh <- &vectorindex.SearchOutput{Keys: int64ColumnBuffer(7, 8), Dists: []float32{1.5, 2.5}}
	close(u.streamCh)
	u.errCh <- nil
	out := pkScoreBatch()
	end, err := u.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, 2, out.RowCount())
	require.Equal(t, int64(8), vector.GetFixedAtWithTypeCheck[int64](out.Vecs[0], 1))
	require.Equal(t, float32(1.5), vector.GetFixedAtWithTypeCheck[float32](out.Vecs[1], 0))

	end, err = u.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.True(t, end)
	require.True(t, u.done)
	// once done, further reads end immediately.
	end, err = u.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.True(t, end)

	// error path: the producer reports a search error on close.
	u = streamingReader(proc)
	close(u.streamCh)
	u.errCh <- moerr.NewInternalErrorNoCtx("search failed")
	_, err = u.Read(context.Background(), nil, nil, mp, out)
	require.ErrorContains(t, err, "search failed")

	// a canceled query stops waiting for the producer.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proc.Ctx = ctx
	u = streamingReader(proc)
	_, err = u.Read(context.Background(), nil, nil, mp, out)
	require.ErrorIs(t, err, context.Canceled)
}

// TestReadStreamingCovered drives the COVERED streaming path: include values arrive
// column-major and are decoded box-free, preserving NULLs.
func TestReadStreamingCovered(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	u := streamingReader(proc)
	u.includeOut = []includeOut{{vecIdx: 2, segPos: 0, name: "tag"}}
	tag := &vectorindex.ColumnBuffer{Type: types.T_varchar}
	tag.Data = append(tag.Data, 1, 0, 0, 0, 'x') // len=1, "x"
	tag.Data = append(tag.Data, 0, 0, 0, 0)      // len=0 (NULL placeholder)
	tag.N = 2
	tag.Nulls = []bool{false, true}
	u.streamCh <- &vectorindex.SearchOutput{Keys: int64ColumnBuffer(7, 8), Dists: []float32{1.5, 2.5}, Include: []*vectorindex.ColumnBuffer{tag}}
	close(u.streamCh)
	u.errCh <- nil

	out := batch.NewWithSize(3)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	out.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	out.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	end, err := u.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, 2, out.RowCount())
	require.Equal(t, "x", out.Vecs[2].GetStringAt(0))
	require.True(t, out.Vecs[2].IsNull(1))
}

// probeTailReader builds a self-completing json-probe reader with a [doc_id int64, score
// float32] output. Defaults are the BEHIND case: searched(100) < bar(2000) <= snap(3000), so
// with a single-schema-version window the reader runs the table_changes tail.
func probeTailReader(proc *process.Process) *reader {
	u := &reader{proc: proc, probeTail: true, pkVecIdx: 0, scoreVecIdx: 1}
	u.tblcfg = fulltext2.TableConfig{
		DbName: "db", SrcTable: "t", PKey: "id",
		ProbeTailWhere: "json_extract_string(`j`, '$.foo') = 'needle'",
		ProbeTailBar:   2000,
	}
	u.tailSearchedBuildTS = 100
	u.tailSnap = timestamp.Timestamp{PhysicalTime: 3000}
	u.tailSp = sqlexec.NewSqlProcess(proc)
	return u
}

// stubTailSpansSchema forces the schema-span check to a fixed answer for a test.
func stubTailSpansSchema(t *testing.T, spans bool) {
	orig := tailSpansSchema
	t.Cleanup(func() { tailSpansSchema = orig })
	tailSpansSchema = func(*reader, int64) bool { return spans }
}

// captureTailSQL replaces the streaming executor with one that records the SQL and streams
// the rows of each pks result.
func captureTailSQL(t *testing.T, mp *mpool.MPool, pks ...[]int64) *string {
	orig := runStreamingSql
	t.Cleanup(func() { runStreamingSql = orig })
	var captured string
	runStreamingSql = func(_ context.Context, _ *sqlexec.SqlProcess, sql string, _ string, streamCh chan executor.Result, _ chan error) (executor.Result, error) {
		captured = sql
		for _, rows := range pks {
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			for _, pk := range rows {
				require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], pk, false, mp))
			}
			bat.SetRowCount(len(rows))
			streamCh <- executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
		}
		return executor.Result{}, nil
	}
	return &captured
}

// TestProbeTailStreams drives the BEHIND, single-schema-version tail: startProbeTail builds the
// aliased table_changes(searched, S] SQL (with the pushed json predicate) and streams it;
// emitProbeTail pages each streamed result into the output as (doc_id=pk, score=0).
func TestProbeTailStreams(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	stubTailSpansSchema(t, false)
	sql := captureTailSQL(t, mp, []int64{}, []int64{42, 43})

	u := probeTailReader(proc)
	u.includeOut = []includeOut{{vecIdx: 2, segPos: 0, name: "tag"}}
	out := batch.NewWithSize(3)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	out.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	out.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	n, err := u.emitProbeTail(mp, out)
	require.NoError(t, err)
	require.Equal(t, 2, n, "an empty streamed result is skipped")
	require.Equal(t, int64(42), vector.GetFixedAtWithTypeCheck[int64](out.Vecs[0], 0))
	require.Equal(t, int64(43), vector.GetFixedAtWithTypeCheck[int64](out.Vecs[0], 1))
	require.Equal(t, float32(0), vector.GetFixedAtWithTypeCheck[float32](out.Vecs[1], 0))
	require.True(t, out.Vecs[2].IsNull(0), "a tail row has no include values")

	require.Contains(t, *sql, "table_changes('db', 't'")
	require.Contains(t, *sql, "'100-0'")
	require.Contains(t, *sql, "AS mo_tc")
	require.Contains(t, *sql, "mo_tc.change_type = 'insert'")
	require.Contains(t, *sql, "AND (json_extract_string(`j`, '$.foo') = 'needle')")

	n, err = u.emitProbeTail(mp, out) // stream closed → end
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, u.Close())
}

// TestProbeTailCaughtUp: searched >= bar (and <= S) means the bulk already reflects every row
// the read sees -- start no stream and run no tail query.
func TestProbeTailCaughtUp(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	sql := captureTailSQL(t, mp)

	u := probeTailReader(proc)
	u.tailSearchedBuildTS = 2500
	n, err := u.emitProbeTail(mp, pkScoreBatch())
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, *sql, "a caught-up generation must not run the tail query")
}

// TestProbeTailLogicalBoundary: a bar of (P, L>0) is NOT covered by a generation at physical
// P, so searched == bar.physical with bar.logical > 0 must run the tail.
func TestProbeTailLogicalBoundary(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	stubTailSpansSchema(t, false)
	sql := captureTailSQL(t, mp)

	u := probeTailReader(proc)
	u.tblcfg.ProbeTailBarLogical = 1
	u.tailSearchedBuildTS = 2000
	_, err := u.emitProbeTail(mp, pkScoreBatch())
	require.NoError(t, err)
	require.Contains(t, *sql, "table_changes")
	require.Contains(t, *sql, "'2000-0'")
}

// TestProbeTailFallbacks: a generation newer than the read, a physical-time tie with it, and a
// schema change inside the gap all fall back to a selective full scan of the source.
func TestProbeTailFallbacks(t *testing.T) {
	const fallback = "SELECT `id` FROM `db`.`t` WHERE json_extract_string(`j`, '$.foo') = 'needle'"
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)

	for _, tc := range []struct {
		name  string
		spans bool
		edit  func(u *reader)
	}{
		{"newer generation", false, func(u *reader) { u.tailSearchedBuildTS = 3500 }},
		{"physical tie", false, func(u *reader) {
			u.tailSearchedBuildTS = 3000
			u.tailSnap = timestamp.Timestamp{PhysicalTime: 3000, LogicalTime: 1}
		}},
		{"schema span", true, func(*reader) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubTailSpansSchema(t, tc.spans)
			sql := captureTailSQL(t, mp)
			u := probeTailReader(proc)
			tc.edit(u)
			_, err := u.emitProbeTail(mp, pkScoreBatch())
			require.NoError(t, err)
			require.Equal(t, fallback, *sql)
		})
	}

	// Without a rendered predicate the fallback scans every pk.
	u := probeTailReader(proc)
	u.tblcfg.ProbeTailWhere = ""
	require.Equal(t, "SELECT `id` FROM `db`.`t`", u.probeFallbackSQL())
}

func TestProbeTailRequiresSourceAndKey(t *testing.T) {
	proc := testutil.NewProc(t)
	u := probeTailReader(proc)
	u.tblcfg.PKey = ""
	_, err := u.emitProbeTail(proc.Mp(), pkScoreBatch())
	require.ErrorContains(t, err, "requires source table and pk")
}

// TestProbeTailStreamError surfaces an error the tail producer reports.
func TestProbeTailStreamError(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	stubTailSpansSchema(t, false)
	orig := runStreamingSql
	t.Cleanup(func() { runStreamingSql = orig })
	runStreamingSql = func(context.Context, *sqlexec.SqlProcess, string, string, chan executor.Result, chan error) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("tail failed")
	}
	u := probeTailReader(proc)
	_, err := u.emitProbeTail(mp, pkScoreBatch())
	require.ErrorContains(t, err, "tail failed")
}

// TestProbeTailSpansSchemaFailsClosed: without an engine the schema check reports a span, so
// the caller takes the safe full-scan fallback.
func TestProbeTailSpansSchemaFailsClosed(t *testing.T) {
	u := probeTailReader(testutil.NewProc(t))
	require.True(t, u.probeTailSpansSchema(100))
}

// TestCloseDrains: Close cancels the search and tail producers and drains their channels, and
// is idempotent.
func TestCloseDrains(t *testing.T) {
	mp := mpool.MustNewZero()
	u := &reader{}
	_, cancel := context.WithCancel(context.Background())
	u.cancel = cancel
	u.streamCh = make(chan *vectorindex.SearchOutput, 4)
	u.streamCh <- &vectorindex.SearchOutput{Keys: int64ColumnBuffer(1), Dists: []float32{1}}
	close(u.streamCh)
	_, tailCancel := context.WithCancel(context.Background())
	u.tailCancel = tailCancel
	u.tailStreamCh = make(chan executor.Result, 4)
	u.tailStreamCh <- executor.Result{Mp: mp}
	close(u.tailStreamCh)

	require.NoError(t, u.Close())
	require.Nil(t, u.cancel)
	require.Nil(t, u.streamCh)
	require.Nil(t, u.tailCancel)
	require.Nil(t, u.tailStreamCh)
	require.NoError(t, u.Close())
}

func TestScoreAlgo(t *testing.T) {
	proc := testutil.NewProc(t)
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return fulltext2.Fulltext2RelevancyAlgo_tfidf, nil
	})
	require.Equal(t, fulltext2.TfIdf, scoreAlgo(proc))
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return nil, moerr.NewInternalErrorNoCtx("no variable")
	})
	require.Equal(t, fulltext2.BM25, scoreAlgo(proc))
}

func TestReaderNoOpSettersAndOptions(t *testing.T) {
	u := &reader{}
	u.SetOrderBy(nil)
	require.Nil(t, u.GetOrderBy())
	u.SetIndexParam(nil)
	u.SetFilterZM(nil)

	opts := ft2plan.ScanOptions{Config: "{}", Mode: 3, IncludePreds: "[]", ScoreRange: "{}"}
	data, err := ft2plan.EncodeScanOptions(opts)
	require.NoError(t, err)
	got, err := ft2plan.DecodeScanOptions(data)
	require.NoError(t, err)
	require.Equal(t, opts, got)
}
