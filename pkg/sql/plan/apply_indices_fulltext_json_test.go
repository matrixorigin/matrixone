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

package plan

import (
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

// fakeCoverageTxn is a non-nil TxnOperator that only needs to answer SnapshotTS();
// indexCoversSnapshot reads no other method on this path. Any other call panics,
// which would surface an unexpected new dependency rather than hide it.
type fakeCoverageTxn struct{ client.TxnOperator }

func (fakeCoverageTxn) SnapshotTS() timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: 1_700_000_000_000_000_000}
}

func jpColExpr(pos int32) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
}

func jpStrLit(s string) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: s}}}}
}

func jpFltLit(f float64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: f}}}}
}

func jpIntLit(i int64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: i}}}}
}

func jpCallExpr(name string, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: name},
		Args: args,
	}}}
}

func jpExtractStr(col int32, path string) *plan.Expr {
	return jpCallExpr("json_extract_string", jpColExpr(col), jpStrLit(path))
}

func jpExtractFloat(col int32, path string) *plan.Expr {
	return jpCallExpr("json_extract_float64", jpColExpr(col), jpStrLit(path))
}

// asyncCoverageBar lowers the snapshot by delay; underflow clamps to the snapshot.
func TestAsyncCoverageBar(t *testing.T) {
	nowNanos := int64(1_700_000_000_000_000_000)
	ts := types.BuildTS(nowNanos, 7)
	delay := 15 * time.Second

	bar := asyncCoverageBar(ts, delay)
	require.Equal(t, nowNanos-int64(delay), bar.Physical())
	require.Equal(t, uint32(7), bar.Logical())

	require.Equal(t, ts, asyncCoverageBar(ts, 0))

	tiny := types.BuildTS(10, 3)
	require.Equal(t, tiny, asyncCoverageBar(tiny, delay))
}

// TestIndexCoversSnapshotReachesCoverageHook drives the async-coverage POSITIVE
// path: a non-nil builder with a real mock process/txn reaches the CoversSnapshot
// lookup under the live top context (the other tests use a nil builder and stop at
// the fail-closed guard). No live ISCP job exists in a unit context, so the hook
// reports not-covered -- the point is exercising the reachable path safely (#27926).
func TestIndexCoversSnapshotReachesCoverageHook(t *testing.T) {
	mockCtx := NewMockCompilerContext(false)
	proc := mockCtx.GetProcess()
	proc.Base.TxnOperator = fakeCoverageTxn{} // the mock proc has no txn otherwise
	b := &QueryBuilder{compCtx: mockCtx}
	idx := jpJSONIndex("j", `{"parser":"json"}`)
	scanNode := jpScanNode("j", idx)
	scanNode.TableDef.TblId = 424242 // must be non-zero to pass the guard

	require.False(t, b.indexCoversSnapshot(scanNode, idx),
		"with no live coverage job, an async index must not be treated as covering")
}

// The headline rewrite from the issue.
func TestJSONProbeStringEquality(t *testing.T) {
	p, ok := jsonExtractProbeFromExpr(
		jpCallExpr("=", jpExtractStr(3, "$.foo"), jpStrLit("bar")))
	require.True(t, ok)
	require.Equal(t, int32(3), p.ColPos)
	require.Equal(t, "foo", p.Tag)
	require.Empty(t, p.Ranges)
	// 'bar' is not numeric, so exactly one term: an exact lookup
	require.Equal(t, []string{fulltext2.JSONStringTerm("foo", "bar")}, p.Terms)
}

// THE correctness property: the probe must be implied by the predicate. For a
// document that satisfies the comparison, the probe's term set must intersect
// the document's indexed terms — otherwise the rewrite drops the row.
func TestJSONProbeTermsArePresentInMatchingDocuments(t *testing.T) {
	intersects := func(p jsonProbe, doc string) bool {
		terms := jpDocTerms(t, doc)
		for _, term := range p.Terms {
			if terms[term] {
				return true
			}
		}
		return false
	}

	// string equality against a string leaf
	p, ok := jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractStr(0, "$.foo"), jpStrLit("bar")))
	require.True(t, ok)
	require.True(t, intersects(p, `{"foo":"bar"}`))
	require.True(t, intersects(p, `{"a":{"foo":"bar"}}`), "leaf-only probe is path agnostic")
	require.False(t, intersects(p, `{"foo":"other"}`))
	require.False(t, intersects(p, `{"zzz":"bar"}`), "a different key must not match")

	// json_extract_string is NULL for a numeric leaf, so `= '3.14'` is true only
	// for the STRING "3.14": one term, and the numeric document must NOT match
	p, ok = jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractStr(0, "$.n"), jpStrLit("3.14")))
	require.True(t, ok)
	require.Len(t, p.Terms, 1, "the two extractors are disjoint on leaf type")
	require.True(t, intersects(p, `{"n":"3.14"}`), "string leaf is reachable")
	require.False(t, intersects(p, `{"n":3.14}`), "numeric leaf is NULL for json_extract_string")

	// float equality reaches an integer leaf, since all numbers normalize
	p, ok = jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractFloat(0, "$.n"), jpIntLit(3)))
	require.True(t, ok)
	require.True(t, intersects(p, `{"n":3}`))
	require.True(t, intersects(p, `{"n":3.0}`))
	require.False(t, intersects(p, `{"n":4}`))
}

// Everything the rule must decline. Declining is always safe — the original
// predicate stands alone — so these guard against a probe that is NOT implied.
func TestJSONProbeDeclines(t *testing.T) {
	for _, tc := range []struct {
		name string
		expr *plan.Expr
	}{
		{"wildcard path", jpCallExpr("=", jpExtractStr(0, "$.a.*"), jpStrLit("x"))},
		{"recursive wildcard", jpCallExpr("=", jpExtractStr(0, "$**.b"), jpStrLit("x"))},
		{"root path has no tag", jpCallExpr("=", jpExtractStr(0, "$"), jpStrLit("x"))},
		{"subscript only has no tag", jpCallExpr("=", jpExtractStr(0, "$[0]"), jpStrLit("x"))},
		{"non-constant rhs", jpCallExpr("=", jpExtractStr(0, "$.a"), jpColExpr(9))},
		{"non-constant path", jpCallExpr("=",
			jpCallExpr("json_extract_string", jpColExpr(0), jpColExpr(1)), jpStrLit("x"))},
		{"not-equal is not a range", jpCallExpr("!=", jpExtractStr(0, "$.a"), jpStrLit("x"))},
		{"not a json_extract", jpCallExpr("=", jpCallExpr("lower", jpColExpr(0)), jpStrLit("x"))},
		{"json_extract on an expression, not a column", jpCallExpr("=",
			jpCallExpr("json_extract_string", jpCallExpr("lower", jpColExpr(0)), jpStrLit("$.a")),
			jpStrLit("x"))},
		// numeric equality with a string constant would change which leaves the
		// probe reaches
		{"float compared to a string constant", jpCallExpr("=", jpExtractFloat(0, "$.n"), jpStrLit("3"))},
		{"unsupported operator", jpCallExpr("<=>", jpExtractStr(0, "$.a"), jpStrLit("x"))},
	} {
		_, ok := jsonExtractProbeFromExpr(tc.expr)
		require.False(t, ok, tc.name)
	}
}

func TestJSONPathTag(t *testing.T) {
	for _, tc := range []struct {
		path, tag string
		ok        bool
	}{
		{"$.foo", "foo", true},
		{"$.a.b.c", "c", true},
		{"$.a[0]", "a", true},      // array elements index under the enclosing key
		{"$.a[0].b", "b", true},    // ...and a key below a subscript is still the tag
		{"$.a[0][1]", "a", true},   // nested subscripts collapse the same way
		{"  $.foo  ", "foo", true}, // surrounding space is not significant
		{"$", "", false},
		{"$[0]", "", false},
		{"$.a.*", "", false},
		{"$**.b", "", false},
		{"foo", "", false}, // not a path
		{"$.", "", false},
	} {
		tag, ok := jsonPathTag(tc.path)
		require.Equal(t, tc.ok, ok, tc.path)
		if tc.ok {
			require.Equal(t, tc.tag, tag, tc.path)
		}
	}
}

// --- index resolution and probe injection ------------------------------------

func jpScanNode(colName string, idx ...*plan.IndexDef) *plan.Node {
	return &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{7},
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: colName, Typ: plan.Type{Id: int32(types.T_json)}},
			},
			Indexes: idx,
		},
	}
}

func jpJSONIndex(col, params string) *plan.IndexDef {
	return &plan.IndexDef{
		IndexName:          "ftj",
		TableExist:         true,
		IndexAlgo:          catalog.MoIndexFullText2Algo.ToString(),
		IndexAlgoTableType: catalog.FullText2Index_TblType_Storage,
		Parts:              []string{col},
		IndexAlgoParams:    params,
	}
}

// The index must be resolved from the COLUMN, and only a json-parser fulltext2
// index that actually holds tuple terms qualifies. Probing anything else finds
// nothing, which would drop every row.
func TestFindJSONTupleIndex(t *testing.T) {
	var b *QueryBuilder
	// findJSONTupleIndex answers SHAPE only — the right column, parser and term
	// options. Freshness is a separate gate (indexCoversSnapshot).
	shaped := jpJSONIndex("j", `{"parser":"json"}`)
	require.NotNil(t, b.findJSONTupleIndex(jpScanNode("j", shaped), 1))

	// wrong column position
	require.Nil(t, b.findJSONTupleIndex(jpScanNode("j", shaped), 0))

	for _, tc := range []struct {
		name string
		idx  *plan.IndexDef
	}{
		{"wrong parser", jpJSONIndex("j", `{"parser":"ngram"}`)},
		{"no parser", jpJSONIndex("j", "")},
		{"include_keys off", jpJSONIndex("j", `{"parser":"json","include_keys":"false"}`)},
		{"different column", jpJSONIndex("other", `{"parser":"json"}`)},
	} {
		require.Nil(t, b.findJSONTupleIndex(jpScanNode("j", tc.idx), 1), tc.name)
	}

	// a metadata def must not be picked up (only the storage def carries Parts)
	meta := jpJSONIndex("j", `{"parser":"json"}`)
	meta.IndexAlgoTableType = catalog.FullText2Index_TblType_Metadata
	require.Nil(t, b.findJSONTupleIndex(jpScanNode("j", meta), 1))

	// a non-materialized index must not be probed
	gone := jpJSONIndex("j", `{"parser":"json"}`)
	gone.TableExist = false
	require.Nil(t, b.findJSONTupleIndex(jpScanNode("j", gone), 1))

	// no indexes at all
	require.Nil(t, b.findJSONTupleIndex(jpScanNode("j"), 1))
}

// The term shape must come from the index, never be assumed: an index built one
// way and probed the other way silently returns nothing.
func TestJSONIndexTermShapeParams(t *testing.T) {
	require.True(t, jsonIndexIncludeKeys(jpJSONIndex("j", `{"parser":"json"}`)), "absent => on")
	require.True(t, jsonIndexIncludeKeys(jpJSONIndex("j", `{"include_keys":"true"}`)))
	require.False(t, jsonIndexIncludeKeys(jpJSONIndex("j", `{"include_keys":"false"}`)))
	require.True(t, jsonIndexIncludeKeys(jpJSONIndex("j", `{bad json`)), "malformed => default on")

	require.Equal(t, "", jsonIndexParam(nil, "include_keys"))
	require.Equal(t, "", jsonIndexParam(jpJSONIndex("j", ""), "include_keys"))
}

// An async index may only back a mandatory filter when its coverage can be
// PROVEN. With no compiler context there is no snapshot to check against, so
// the gate must fail closed and inject nothing.
func TestAddJSONFulltextProbesRefusesAsyncIndex(t *testing.T) {
	var b *QueryBuilder
	node := jpScanNode("j", jpJSONIndex("j", `{"parser":"json"}`))
	node.FilterList = []*plan.Expr{
		jpCallExpr("=", jpExtractStr(1, "$.foo"), jpStrLit("bar")),
	}
	b.addJSONFulltextProbes(node)
	require.Len(t, node.FilterList, 1, "no probe may be added for an async index")
	require.False(t, isJSONProbeMatch(node.FilterList[0]))
}

// makeJSONProbeMatch is still exercised directly: it is what a future
// watermark-gated caller will use, and its shape is what findMatchFullTextIndex
// resolves against.
func TestMakeJSONProbeMatchShape(t *testing.T) {
	var b *QueryBuilder
	node := jpScanNode("j", jpJSONIndex("j", `{"parser":"json"}`))
	probe, ok := jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractStr(1, "$.foo"), jpStrLit("bar")))
	require.True(t, ok)

	m := b.makeJSONProbeMatch(node, 1, probe)
	require.NotNil(t, m)
	require.True(t, isJSONProbeMatch(m))
	fn := m.GetF()
	require.Equal(t, "fulltext_match", fn.Func.ObjName)
	col := fn.Args[2].GetCol()
	require.Equal(t, int32(7), col.RelPos, "must carry the scan binding tag")
	require.Equal(t, "j", col.Name)
	mode := fn.Args[1].GetLit().Value.(*plan.Literal_I64Val).I64Val
	require.Equal(t, fulltext2.JSONProbeMode, mode)
}

// Nothing is added when no index can serve the predicate, or when there is
// nothing to serve. Declining is always safe.
func TestAddJSONFulltextProbesDeclines(t *testing.T) {
	var b *QueryBuilder
	for _, tc := range []struct {
		name string
		node *plan.Node
	}{
		{"no index", func() *plan.Node {
			n := jpScanNode("j")
			n.FilterList = []*plan.Expr{jpCallExpr("=", jpExtractStr(1, "$.foo"), jpStrLit("bar"))}
			return n
		}()},
		{"wrong parser", func() *plan.Node {
			n := jpScanNode("j", jpJSONIndex("j", `{"parser":"ngram"}`))
			n.FilterList = []*plan.Expr{jpCallExpr("=", jpExtractStr(1, "$.foo"), jpStrLit("bar"))}
			return n
		}()},
		{"unprobeable predicate", func() *plan.Node {
			n := jpScanNode("j", jpJSONIndex("j", `{"parser":"json"}`))
			n.FilterList = []*plan.Expr{jpCallExpr("=", jpExtractStr(1, "$.a.*"), jpStrLit("x"))}
			return n
		}()},
		{"no filters", jpScanNode("j", jpJSONIndex("j", `{"parser":"json"}`))},
		{"nil node", nil},
	} {
		before := 0
		if tc.node != nil {
			before = len(tc.node.FilterList)
		}
		b.addJSONFulltextProbes(tc.node)
		if tc.node != nil {
			require.Len(t, tc.node.FilterList, before, tc.name)
		}
	}
}

func TestIsJSONProbeMatch(t *testing.T) {
	require.False(t, isJSONProbeMatch(jpStrLit("x")))
	require.False(t, isJSONProbeMatch(jpCallExpr("lower", jpColExpr(0))))
	// an ordinary MATCH is not a probe
	ordinary := jpCallExpr("fulltext_match", jpStrLit("pattern"), jpIntLit(0), jpColExpr(1))
	require.False(t, isJSONProbeMatch(ordinary))
}

// The freshness gate fails closed on every missing input. A synchronously
// maintained index needs no check at all.
func TestIndexCoversSnapshotFailsClosed(t *testing.T) {
	var b *QueryBuilder
	node := jpScanNode("j", jpJSONIndex("j", `{"parser":"json"}`))

	// always-async + no compiler context ⇒ cannot prove coverage ⇒ decline
	require.False(t, b.indexCoversSnapshot(node, node.TableDef.Indexes[0]))

	// an algorithm that is not always-async is current by construction
	sync := jpJSONIndex("j", `{"parser":"json"}`)
	sync.IndexAlgo = "btree" // registered, not always-async
	require.True(t, b.indexCoversSnapshot(node, sync))

	// an unregistered algo is not always-async either, so it is not gated here;
	// findJSONTupleIndex is what rejects it
	unknown := jpJSONIndex("j", `{"parser":"json"}`)
	unknown.IndexAlgo = "no-such-algo"
	require.True(t, b.indexCoversSnapshot(node, unknown))

	// a scan with no ObjRef cannot name the table for the lookup
	noRef := jpScanNode("j", jpJSONIndex("j", `{"parser":"json"}`))
	noRef.ObjRef = nil
	require.False(t, b.indexCoversSnapshot(noRef, noRef.TableDef.Indexes[0]))
}

// jpDocTerms is the document's indexed tuple terms — what a probe must actually
// intersect for the rewrite to keep the row.
func jpDocTerms(t *testing.T, doc string) map[string]bool {
	t.Helper()
	bj, err := bytejson.ParseFromString(doc)
	require.NoError(t, err)
	m := map[string]bool{}
	for _, term := range fulltext2.JSONTupleTerms(bj, fulltext2.JSONTermOptions{IncludeKeys: true}) {
		m[term] = true
	}
	return m
}

// rangeCovers reports whether any of the document's tuple terms falls inside one
// of the probe's ranges — the necessary condition a range probe asserts.
func rangeCovers(t *testing.T, p jsonProbe, doc string) bool {
	t.Helper()
	for term := range jpDocTerms(t, doc) {
		for _, r := range p.Ranges {
			if r.Lo <= term && term <= r.Hi {
				return true
			}
		}
	}
	return false
}

// The four inequalities become a single term RANGE. The property that matters is
// implication: every document the ORIGINAL predicate accepts must hold a term
// inside the range, or the ANDed probe would drop it.
func TestJSONProbeNumericRanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		op      string
		bound   float64
		accepts []string // the predicate is TRUE for these
		rejects []string // ... and FALSE for these
	}{
		{"greater than", ">", 15,
			[]string{`{"n":20}`, `{"n":30}`, `{"n":15.5}`, `{"n":1e300}`},
			[]string{`{"n":10}`, `{"n":-5}`}},
		{"greater or equal", ">=", 15,
			[]string{`{"n":15}`, `{"n":20}`},
			[]string{`{"n":14.9}`, `{"n":-5}`}},
		{"less than", "<", 15,
			[]string{`{"n":10}`, `{"n":-5}`, `{"n":-1e300}`},
			[]string{`{"n":20}`, `{"n":15.5}`}},
		{"less or equal", "<=", 15,
			[]string{`{"n":15}`, `{"n":10}`},
			[]string{`{"n":15.1}`, `{"n":20}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := jsonExtractProbeFromExpr(
				jpCallExpr(tc.op, jpExtractFloat(0, "$.n"), jpFltLit(tc.bound)))
			require.True(t, ok)
			require.Len(t, p.Ranges, 1)
			require.Empty(t, p.Terms, "a range probe carries no exact term")

			for _, doc := range tc.accepts {
				require.True(t, rangeCovers(t, p, doc),
					"%s must be reachable: dropping it would lose a qualifying row", doc)
			}
			for _, doc := range tc.rejects {
				// the probe MAY be a superset, but on these it should also be
				// tight enough to exclude — except at the strict boundary,
				// checked separately below
				require.False(t, rangeCovers(t, p, doc), "%s should not be selected", doc)
			}

			// a different key never satisfies the range
			require.False(t, rangeCovers(t, p, `{"other":20}`))
			// a STRING leaf under the same key is not in a numeric range
			require.False(t, rangeCovers(t, p, `{"n":"20"}`))
		})
	}
}

// Both ends are inclusive, so a STRICT inequality is a superset by exactly the
// boundary value. That is intentional — the retained predicate removes it — and
// this pins it as a deliberate choice rather than an off-by-one.
func TestJSONProbeStrictInequalityIsSupersetAtTheBoundary(t *testing.T) {
	gt, ok := jsonExtractProbeFromExpr(jpCallExpr(">", jpExtractFloat(0, "$.n"), jpFltLit(15)))
	require.True(t, ok)
	require.True(t, rangeCovers(t, gt, `{"n":15}`),
		"the boundary value is included on purpose; the retained predicate drops it")

	lt, ok := jsonExtractProbeFromExpr(jpCallExpr("<", jpExtractFloat(0, "$.n"), jpFltLit(15)))
	require.True(t, ok)
	require.True(t, rangeCovers(t, lt, `{"n":15}`))
}

// String inequalities range over the STRING encoding only, so they never reach a
// numeric leaf — json_extract_string is NULL for one, so no numeric document can
// satisfy the predicate anyway.
func TestJSONProbeStringRanges(t *testing.T) {
	p, ok := jsonExtractProbeFromExpr(jpCallExpr(">", jpExtractStr(0, "$.a"), jpStrLit("m")))
	require.True(t, ok)
	require.Len(t, p.Ranges, 1)
	require.True(t, rangeCovers(t, p, `{"a":"n"}`))
	require.True(t, rangeCovers(t, p, `{"a":"zzz"}`))
	require.False(t, rangeCovers(t, p, `{"a":"a"}`))
	require.False(t, rangeCovers(t, p, `{"a":20}`), "a numeric leaf is NULL for json_extract_string")

	p, ok = jsonExtractProbeFromExpr(jpCallExpr("<=", jpExtractStr(0, "$.a"), jpStrLit("m")))
	require.True(t, ok)
	require.True(t, rangeCovers(t, p, `{"a":"a"}`))
	require.True(t, rangeCovers(t, p, `{"a":"m"}`))
	require.False(t, rangeCovers(t, p, `{"a":"n"}`))
}

// The operand order flips the operator, so a constant on the left builds the
// mirrored range rather than declining or — worse — the wrong half.
func TestJSONProbeRangeFlipsOperandOrder(t *testing.T) {
	// 15 < n  ==  n > 15
	flipped, ok := jsonExtractProbeFromExpr(
		jpCallExpr("<", jpFltLit(15), jpExtractFloat(0, "$.n")))
	require.True(t, ok)
	direct, ok := jsonExtractProbeFromExpr(
		jpCallExpr(">", jpExtractFloat(0, "$.n"), jpFltLit(15)))
	require.True(t, ok)
	require.Equal(t, direct.Ranges, flipped.Ranges)
}

// A value past the truncation limit must still produce a SUPERSET: truncation is
// monotone, so a long bound can only widen the range, never cut a qualifying row.
func TestJSONProbeRangeWithOverlongBound(t *testing.T) {
	long := strings.Repeat("a", 300)
	p, ok := jsonExtractProbeFromExpr(jpCallExpr(">", jpExtractStr(0, "$.a"), jpStrLit(long)))
	require.True(t, ok)
	// a value greater than the bound and sharing its truncated prefix still lands
	// in range rather than being lost to the cut
	require.True(t, rangeCovers(t, p, `{"a":"`+long+`zzz"}`))
}

// A json probe must never take a pushed candidate LIMIT. It returns a SUPERSET
// that the retained predicate then narrows, so truncating it to k candidates
// yields fewer than k final rows and silently drops qualifying ones.
//
// Both of the gate's paths already decline for a probe — its predicate always
// leaves a residual filter, and its mode is not FULLTEXT_BOOLEAN — but that is
// incidental, and a later widening of conjunctive eligibility would turn it into
// a wrong-results bug. This pins the refusal itself.
func TestCandidateLimitRefusesJSONProbe(t *testing.T) {
	var b QueryBuilder
	limit := makePlan2Uint64ConstExprWithType(10)

	probe := jpCallExpr("fulltext_match",
		jpStrLit("payload"), jpIntLit(fulltext2.JSONProbeMode), jpColExpr(1))
	require.True(t, isJSONProbeMatch(probe))

	// even with NO residual filter and a literal LIMIT — the shape the gate is
	// most willing to push — a probe is refused
	scan := &plan.Node{TableDef: &plan.TableDef{}}
	require.Nil(t, b.buildFullTextCandidateLimit(
		scan, nil, []*plan.Expr{probe}, nil, false, false, limit, nil),
		"a probe must not be truncated, whatever the rest of the shape allows")
}
