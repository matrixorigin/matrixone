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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// fakeCoverageTxn is a non-nil TxnOperator that only needs to answer SnapshotTS();
// indexCoversSnapshot reads no other method on this path. Any other call panics,
// which would surface an unexpected new dependency rather than hide it.
type fakeCoverageTxn struct{ client.TxnOperator }

func (fakeCoverageTxn) SnapshotTS() timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: 1_700_000_000_000_000_000}
}

// Txn answers the read snapshot so EffectiveSnapshotTS can compare a historical read against it
// (a {snapshot=...} TS earlier than this counts as historical).
func (fakeCoverageTxn) Txn() txn.TxnMeta {
	return txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 1_700_000_000_000_000_000}}
}

// CloneSnapshotOp is used to read the source relation as of a historical snapshot; the mocked
// engine ignores the operator, so returning the same fake is enough.
func (f fakeCoverageTxn) CloneSnapshotOp(timestamp.Timestamp) client.TxnOperator { return f }

// indexCoversSnapshot is a test-only convenience over decideJSONProbe: it collapses the 3-way
// decision to the single covered/not-covered bit the older coverage tests assert on.
func (builder *QueryBuilder) indexCoversSnapshot(scanNode *plan.Node, idx *plan.IndexDef) bool {
	kind, _ := builder.decideJSONProbe(scanNode, idx)
	return kind == jsonProbeCovered
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

// A prepared plan carrying an injected json coverage probe must be flagged so it rebuilds every
// EXECUTE: its covered/partial decision reflects the async index's freshness at build time, which
// no schema version tracks, so a reused plan would drop rows committed after it was cached. A user
// MATCH builds the same fulltext2_search node but is freshness-tolerant, so only JSONProbeMode counts.
func TestPreparedPlanDependsOnIndexCoverage(t *testing.T) {
	mkPlan := func(nodes ...*plan.Node) *Plan {
		return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: nodes}}}
	}
	ftNode := func(mode int64) *plan.Node {
		return &plan.Node{
			NodeType: plan.Node_FUNCTION_SCAN,
			TableDef: &plan.TableDef{TblFunc: &plan.TableFunction{Name: fulltext2_search_func_name}},
			TblFuncExprList: []*plan.Expr{
				makePlan2StringConstExprWithType("cfg"),
				jpStrLit("pattern"),
				{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: mode}}}},
			},
		}
	}
	require.False(t, PreparedPlanDependsOnIndexCoverage(nil), "nil plan")
	require.False(t, PreparedPlanDependsOnIndexCoverage(mkPlan()), "no nodes")
	require.False(t, PreparedPlanDependsOnIndexCoverage(mkPlan(&plan.Node{NodeType: plan.Node_TABLE_SCAN})),
		"a plain scan is reusable")
	require.False(t, PreparedPlanDependsOnIndexCoverage(mkPlan(ftNode(0))),
		"a user MATCH (non-JSONProbe mode) is freshness-tolerant and reusable")
	require.True(t, PreparedPlanDependsOnIndexCoverage(mkPlan(ftNode(fulltext2.JSONProbeMode))),
		"an injected json coverage probe must force a rebuild")
	require.True(t, PreparedPlanDependsOnIndexCoverage(mkPlan(&plan.Node{NodeType: plan.Node_TABLE_SCAN}, ftNode(fulltext2.JSONProbeMode))),
		"the probe must be found among other nodes")
}

// rebaseToTableChanges rewrites base-scan column references onto the table_changes output columns
// (matched by name) so the json predicate can filter the tail; a reference to a column the tail
// does not expose makes the whole predicate unpushable (nil), leaving it to the base re-check.
func TestRebaseToTableChanges(t *testing.T) {
	b := &QueryBuilder{}
	const scanTag int32 = 10
	const tcTag int32 = 20
	scanNode := &plan.Node{
		BindingTags: []int32{scanTag},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{Name: "id"}, {Name: "j"}, {Name: "content"},
		}},
	}
	// table_changes prepends four metadata columns before the source columns.
	tcNode := &plan.Node{TableDef: &plan.TableDef{Cols: []*plan.ColDef{
		{Name: "change_type"}, {Name: "commit_ts"}, {Name: "__mo_table_id"}, {Name: "__mo_schema_version"},
		{Name: "id"}, {Name: "j"}, {Name: "content"},
	}}}

	// json_extract(j, ...) = 'x' -- the j reference (scanTag:1) rebinds to tcTag:5.
	colJ := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 1, Name: "j"}}}
	pred := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "="},
		Args: []*plan.Expr{colJ, makePlan2StringConstExprWithType("x")},
	}}}
	out := b.rebaseToTableChanges(pred, scanNode, tcNode, tcTag)
	require.NotNil(t, out)
	got := out.GetF().Args[0].GetCol()
	require.Equal(t, tcTag, got.RelPos)
	require.Equal(t, int32(5), got.ColPos)

	// Name empty -> resolved from scanNode.Cols[ColPos]; "content" is tcTag:6.
	colByPos := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 2}}}
	out2 := b.rebaseToTableChanges(colByPos, scanNode, tcNode, tcTag)
	require.NotNil(t, out2)
	require.Equal(t, tcTag, out2.GetCol().RelPos)
	require.Equal(t, int32(6), out2.GetCol().ColPos)

	// A column the tail does not expose makes the predicate unpushable.
	colMissing := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 0, Name: "hidden_only"}}}
	require.Nil(t, b.rebaseToTableChanges(colMissing, scanNode, tcNode, tcTag))

	// A reference bound to some other node is left untouched.
	colOther := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 99, ColPos: 1}}}
	out3 := b.rebaseToTableChanges(colOther, scanNode, tcNode, tcTag)
	require.NotNil(t, out3)
	require.Equal(t, int32(99), out3.GetCol().RelPos)
	require.Equal(t, int32(1), out3.GetCol().ColPos)
}

// recordJSONPartialProbe stashes the tail's build_ts lower bound and a deep copy of the json
// predicate, keyed by the base-scan node id, for applyJoinFullTextIndices to consume at the splice.
func TestRecordJSONPartialProbe(t *testing.T) {
	b := &QueryBuilder{}
	pred := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 2}}}
	scanNode := &plan.Node{NodeId: 7}
	b.recordJSONPartialProbe(scanNode, pred, types.BuildTS(1234, 5))
	got, ok := b.jsonPartialProbes[7]
	require.True(t, ok)
	require.Equal(t, int64(1234), got.buildTS.Physical())
	require.NotSame(t, pred, got.jsonPred, "predicate must be deep-copied, not aliased")
}

// unionFtWithTail combines the index (bulk) arm and the table_changes tail arm on the (pk, score)
// layout with UNION ALL, projecting through the left (ft) tag, and returns a pk reference through
// the new union tag; the group-by dedup above it then collapses the pks the two arms share.
func TestUnionFtWithTail(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)
	pkType := plan.Type{Id: int32(types.T_int64)}
	scoreType := plan.Type{Id: int32(types.T_float32)}

	ftTag := builder.genNewBindTag()
	ftID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc:   &plan.TableFunction{Name: fulltext2_search_func_name},
			Cols:      []*plan.ColDef{{Name: "__mo_ft_doc_id", Typ: pkType}, {Name: "__mo_ft_score", Typ: scoreType}},
		},
		BindingTags: []int32{ftTag},
	}, ctx)

	tailTag := builder.genNewBindTag()
	tailID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Stats:       &plan.Stats{},
		BindingTags: []int32{tailTag},
		ProjectList: []*plan.Expr{
			{Typ: pkType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tailTag, ColPos: 0}}},
			{Typ: scoreType, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: 0}}}},
		},
	}, ctx)

	unionID, pkcol := builder.unionFtWithTail(ctx, ftID, ftTag, tailID, pkType)
	un := builder.qry.Nodes[unionID]
	require.Equal(t, plan.Node_UNION_ALL, un.NodeType)
	require.Equal(t, []int32{ftID, tailID}, un.Children)
	require.Len(t, un.ProjectList, 2)
	require.Equal(t, ftTag, un.ProjectList[0].GetCol().RelPos, "union projects through the left (ft) tag")
	require.Equal(t, un.BindingTags[0], pkcol.GetCol().RelPos)
	require.Equal(t, int32(0), pkcol.GetCol().ColPos)
}

// buildJSONProbeTail builds the freshness-gap arm: table_changes over (build_ts, snapshot] filtered
// to change_type='insert' AND the json predicate (rebased onto the tail columns), projected to
// (pk, score) so it unions with the bulk arm. This drives the full happy path against a resolvable
// ordinary table.
func TestBuildJSONProbeTail(t *testing.T) {
	mockCtx := newFullTextJoinMockCompilerContext()
	tableDef := makeFullTextJoinTestTableDef("ft", true)
	tableDef.TableType = catalog.SystemOrdinaryRel
	mockCtx.objects["ft"] = &plan.ObjectRef{SchemaName: "test", ObjName: "ft"}
	mockCtx.tables["ft"] = tableDef

	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, true)
	ctx := NewBindContext(builder, nil)
	mockCtx.GetProcess().Base.TxnOperator = fakeCoverageTxn{}

	scanTag := builder.genNewBindTag()
	scanNode := makeFullTextJoinTestScan(tableDef, scanTag, nil)
	pkType := scanNode.TableDef.Cols[0].Typ // id

	// json predicate on "body" (base scan col 3); table_changes re-exposes it by name.
	jsonPred := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "="},
		Args: []*plan.Expr{
			{Typ: tableDef.Cols[3].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 3, Name: "body"}}},
			makePlan2StringConstExprWithType("x"),
		},
	}}}
	p := jsonPartialProbe{buildTS: types.BuildTS(100, 0), jsonPred: jsonPred}

	tailID, ok := builder.buildJSONProbeTail(ctx, scanNode, p, pkType)
	require.True(t, ok)

	proj := builder.qry.Nodes[tailID]
	require.Equal(t, plan.Node_PROJECT, proj.NodeType)
	require.Len(t, proj.ProjectList, 2, "tail projects (pk, score)")

	filter := builder.qry.Nodes[proj.Children[0]]
	require.Equal(t, plan.Node_FILTER, filter.NodeType)
	require.Len(t, filter.FilterList, 2, "change_type='insert' AND the pushed json predicate")

	tc := builder.qry.Nodes[filter.Children[0]]
	require.Equal(t, plan.Node_FUNCTION_SCAN, tc.NodeType)
	require.Equal(t, "table_changes", tc.TableDef.TblFunc.Name)
}

// fakeSourceEngine/fakeSourceRel provide the minimum decideJSONProbe touches on a current read: a
// relation that answers SourceCommitTS. Any other engine/relation call panics on the nil embed,
// surfacing an unexpected dependency rather than hiding it.
type fakeSourceEngine struct{ engine.Engine }

func (fakeSourceEngine) GetRelationById(context.Context, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
	return "", "", fakeSourceRel{}, nil
}

type fakeSourceRel struct{ engine.Relation }

func (fakeSourceRel) SourceCommitTS(context.Context, types.TS) (types.TS, error) {
	return types.BuildTS(50, 0), nil
}

// recordingSourceEngine captures the account id bound on the context GetRelationById is called with,
// so a test can assert a cross-account snapshot's freshness reads resolve under the snapshot's owner.
type recordingSourceEngine struct {
	engine.Engine
	seen *uint32
}

func (e recordingSourceEngine) GetRelationById(ctx context.Context, _ client.TxnOperator, _ uint64) (string, string, engine.Relation, error) {
	if id, err := defines.GetAccountId(ctx); err == nil {
		*e.seen = id
	}
	return "", "", fakeSourceRel{}, nil
}

// A cross-account {snapshot=...} read must resolve its freshness reads (source relation, ISCP log,
// index metadata) under the account that OWNS the data -- the snapshot's tenant -- not the reader's.
// decideJSONProbe binds that account onto the context before GetRelationById; assert it arrives.
func TestDecideJSONProbeBindsSnapshotAccount(t *testing.T) {
	origCovers := coversSnapshotFn
	defer func() { coversSnapshotFn = origCovers }()
	coversSnapshotFn = func(context.Context, string, coverage.Request) (bool, types.TS, error) {
		return true, types.TS{}, nil
	}

	idx := jpJSONIndex("j", `{"parser":"json"}`)
	mockCtx := NewMockCompilerContext(false)
	proc := mockCtx.GetProcess()
	proc.Base.TxnOperator = fakeCoverageTxn{}
	var seen uint32
	proc.Base.SessionInfo.StorageEngine = recordingSourceEngine{seen: &seen}

	scan := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{7},
		ObjRef:      &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
		TableDef: &plan.TableDef{
			TblId:     424242,
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "j", Typ: plan.Type{Id: int32(types.T_json)}},
			},
			Name2ColIndex: map[string]int32{"id": 0, "j": 1},
			Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			Indexes:       []*plan.IndexDef{idx},
		},
		// A historical snapshot (earlier than the txn) owned by account 42.
		ScanSnapshot: &plan.Snapshot{
			TS:     &timestamp.Timestamp{PhysicalTime: 1_600_000_000_000_000_000},
			Tenant: &plan.SnapshotTenant{TenantID: 42},
		},
	}

	b := &QueryBuilder{compCtx: mockCtx}
	b.decideJSONProbe(scan, idx)
	require.Equal(t, uint32(42), seen, "cross-account snapshot freshness reads must bind the snapshot's tenant")
}

// TestDecideJSONProbeMatrix drives the covered / partial / skip decision by stubbing the two runtime
// coverage lookups, so the whole decision surface is exercised without a live index. A fulltext2
// json index is AlwaysAsync, so decideJSONProbe runs the full path rather than short-circuiting.
func TestDecideJSONProbeMatrix(t *testing.T) {
	origCovers := coversSnapshotFn
	defer func() { coversSnapshotFn = origCovers }()

	idx := jpJSONIndex("j", `{"parser":"json"}`)
	newCase := func(withEngine bool) (*QueryBuilder, *plan.Node) {
		mockCtx := NewMockCompilerContext(false)
		proc := mockCtx.GetProcess()
		proc.Base.TxnOperator = fakeCoverageTxn{}
		if withEngine {
			proc.Base.SessionInfo.StorageEngine = fakeSourceEngine{}
		} else {
			proc.Base.SessionInfo.StorageEngine = nil
		}
		scan := &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{7},
			ObjRef:      &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
			TableDef: &plan.TableDef{
				TblId:     424242,
				TableType: catalog.SystemOrdinaryRel,
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
					{Name: "j", Typ: plan.Type{Id: int32(types.T_json)}},
				},
				Name2ColIndex: map[string]int32{"id": 0, "j": 1},
				Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				Indexes:       []*plan.IndexDef{idx},
			},
		}
		return &QueryBuilder{compCtx: mockCtx}, scan
	}
	asSnapshot := func(scan *plan.Node) {
		scan.ScanSnapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1_600_000_000_000_000_000}}
	}
	// covers stubs CoversSnapshot to return the (covered, build_ts, err) the decision keys on.
	covers := func(v bool, bts types.TS, err error) {
		coversSnapshotFn = func(context.Context, string, coverage.Request) (bool, types.TS, error) {
			return v, bts, err
		}
	}

	// current read, behind, build_ts known, table_changes-eligible -> partial
	covers(false, types.BuildTS(100, 0), nil)
	b, scan := newCase(true)
	kind, bts := b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbePartial, kind)
	require.Equal(t, int64(100), bts.Physical())

	// current read, behind, unknown build_ts -> skip
	covers(false, types.TS{}, nil)
	b, scan = newCase(true)
	kind, _ = b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbeSkip, kind)

	// current read, covered -> covered
	covers(true, types.TS{}, nil)
	b, scan = newCase(true)
	kind, _ = b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbeCovered, kind)

	// snapshot read, covered (index caught up as of S) -> covered
	covers(true, types.TS{}, nil)
	b, scan = newCase(true)
	asSnapshot(scan)
	kind, _ = b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbeCovered, kind)

	// snapshot read, behind as of S -> partial (tail up to S), same as a current read
	covers(false, types.BuildTS(100, 0), nil)
	b, scan = newCase(true)
	asSnapshot(scan)
	kind, bts = b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbePartial, kind)
	require.Equal(t, int64(100), bts.Physical())

	// coverage lookup error -> skip (fail closed)
	covers(false, types.TS{}, moerr.NewInternalErrorNoCtx("boom"))
	b, scan = newCase(true)
	kind, _ = b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbeSkip, kind)

	// current read with no storage engine -> skip
	covers(true, types.TS{}, nil)
	b, scan = newCase(false)
	kind, _ = b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbeSkip, kind)

	// composite/hidden primary key -> skip: table_changes drops the hidden pk column, so the tail
	// cannot be anchored; a partial plan would hard-error at the splice, so decline to a full scan.
	covers(false, types.BuildTS(100, 0), nil)
	b, scan = newCase(true)
	scan.TableDef.Cols = append(scan.TableDef.Cols,
		&plan.ColDef{Name: "__mo_cpkey_col", Hidden: true, Typ: plan.Type{Id: int32(types.T_varchar)}})
	scan.TableDef.Name2ColIndex["__mo_cpkey_col"] = 2
	scan.TableDef.Pkey = &plan.PrimaryKeyDef{PkeyColName: "__mo_cpkey_col", Names: []string{"a", "b"}}
	kind, _ = b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbeSkip, kind, "composite/hidden pk cannot anchor the tail -> full scan")

	// build_ts already at/after the read snapshot -> empty (build_ts, snapshot] window -> skip; the
	// index already covers this read and table_changes would reject from >= to.
	covers(false, types.BuildTS(2_000_000_000_000_000_000, 0), nil) // > the fakeCoverageTxn snapshot (1.7e18)
	b, scan = newCase(true)
	kind, _ = b.decideJSONProbe(scan, idx)
	require.Equal(t, jsonProbeSkip, kind, "empty window (build_ts >= snapshot) -> full scan")
}
