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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

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
	opt := fulltext2.JSONTermOptions{IncludeKeys: true}
	docTerms := func(doc string) map[string]bool {
		bj, err := bytejson.ParseFromString(doc)
		require.NoError(t, err)
		m := map[string]bool{}
		for _, term := range fulltext2.JSONTupleTerms(bj, opt) {
			m[term] = true
		}
		return m
	}
	intersects := func(p jsonProbe, doc string) bool {
		terms := docTerms(doc)
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
		{"unsupported operator", jpCallExpr("!=", jpExtractStr(0, "$.a"), jpStrLit("x"))},
		{"not a json_extract", jpCallExpr("=", jpCallExpr("lower", jpColExpr(0)), jpStrLit("x"))},
		{"json_extract on an expression, not a column", jpCallExpr("=",
			jpCallExpr("json_extract_string", jpCallExpr("lower", jpColExpr(0)), jpStrLit("$.a")),
			jpStrLit("x"))},
		// numeric equality with a string constant would change which leaves the
		// probe reaches
		{"float compared to a string constant", jpCallExpr("=", jpExtractFloat(0, "$.n"), jpStrLit("3"))},
		// v1 accelerates equality only; a range would have to enumerate the
		// key's vocabulary (see jsonRangeProbe)
		{"greater than", jpCallExpr(">", jpExtractFloat(0, "$.n"), jpFltLit(1))},
		{"greater or equal", jpCallExpr(">=", jpExtractFloat(0, "$.n"), jpFltLit(1))},
		{"less than", jpCallExpr("<", jpExtractFloat(0, "$.n"), jpFltLit(1))},
		{"less or equal", jpCallExpr("<=", jpExtractFloat(0, "$.n"), jpFltLit(1))},
		{"string inequality", jpCallExpr(">", jpExtractStr(0, "$.a"), jpStrLit("m"))},
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
	// fulltext2 is ALWAYS ASYNC, so even a correctly-shaped json index is
	// refused: its postings trail the base table, and an ANDed probe would drop
	// rows written inside the ISCP lag.
	shaped := jpJSONIndex("j", `{"parser":"json"}`)
	require.Nil(t, b.findJSONTupleIndex(jpScanNode("j", shaped), 1),
		"an always-async index must not back a mandatory filter")

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

// No probe is injected today: every fulltext2 index is always-async, and the
// gate in findJSONTupleIndex refuses those. The filter list must come back
// untouched — the query is answered by the retained predicate alone.
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
