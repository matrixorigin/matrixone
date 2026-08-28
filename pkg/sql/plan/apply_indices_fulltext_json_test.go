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

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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
	p, ok := jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractStr(3, "$.foo"), jpStrLit("bar")), false)
	require.True(t, ok)
	require.Equal(t, int32(3), p.ColPos)
	require.Equal(t, "foo", p.Tag)
	require.Empty(t, p.Ranges)
	// 'bar' is not numeric, so exactly one term: an exact lookup
	require.Equal(t, []string{fulltext2.JSONStringTerm("foo", "bar", "", false)}, p.Terms)
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
	p, ok := jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractStr(0, "$.foo"), jpStrLit("bar")), false)
	require.True(t, ok)
	require.True(t, intersects(p, `{"foo":"bar"}`))
	require.True(t, intersects(p, `{"a":{"foo":"bar"}}`), "leaf-only probe is path agnostic")
	require.False(t, intersects(p, `{"foo":"other"}`))
	require.False(t, intersects(p, `{"zzz":"bar"}`), "a different key must not match")

	// json_extract_string('$.n') = '3.14' is TRUE for a NUMERIC leaf; the
	// OR-of-encodings is what keeps that row
	p, ok = jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractStr(0, "$.n"), jpStrLit("3.14")), false)
	require.True(t, ok)
	require.Len(t, p.Terms, 2)
	require.True(t, intersects(p, `{"n":3.14}`), "numeric leaf must stay reachable")
	require.True(t, intersects(p, `{"n":"3.14"}`), "string leaf must stay reachable")

	// float equality reaches an integer leaf, since all numbers normalize
	p, ok = jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractFloat(0, "$.n"), jpIntLit(3)), false)
	require.True(t, ok)
	require.True(t, intersects(p, `{"n":3}`))
	require.True(t, intersects(p, `{"n":3.0}`))
	require.False(t, intersects(p, `{"n":4}`))
}

// Ranges: the bound must sit on the correct side. Both ends are inclusive, so
// > and >= produce the SAME range (and < and <=) — the strict form just returns
// the boundary term too, which the retained predicate removes.
func TestJSONProbeRanges(t *testing.T) {
	at := fulltext2.JSONFloatTerm("n", 3.14, "", false)
	lo, hi := fulltext2.JSONNumericTermBounds("n")

	for _, tc := range []struct {
		op             string
		wantLo, wantHi string
	}{
		{">", at, hi},
		{">=", at, hi},
		{"<", lo, at},
		{"<=", lo, at},
	} {
		p, ok := jsonExtractProbeFromExpr(
			jpCallExpr(tc.op, jpExtractFloat(1, "$.n"), jpFltLit(3.14)), false)
		require.True(t, ok, tc.op)
		require.Empty(t, p.Terms, tc.op)
		require.Len(t, p.Ranges, 1, "a float range needs only the numeric encoding")
		require.Equal(t, jsonTermRange{Lo: tc.wantLo, Hi: tc.wantHi}, p.Ranges[0], tc.op)
		require.Less(t, p.Ranges[0].Lo, p.Ranges[0].Hi, "range must be non-empty for %s", tc.op)
	}
}

// json_extract_string inequality: the leaf may be stored as a string OR as a
// number (json_extract_string renders numbers), and the two orders disagree, so
// the probe unions the ordered string range with EVERY numeric term. Wider, but
// a superset — and the retained predicate re-checks each row.
func TestJSONProbeStringRangeUnionsBothEncodings(t *testing.T) {
	p, ok := jsonExtractProbeFromExpr(
		jpCallExpr(">", jpExtractStr(0, "$.n"), jpStrLit("m")), false)
	require.True(t, ok)
	require.Len(t, p.Ranges, 2, "string side + numeric side")

	slo, shi := fulltext2.JSONStringTermBounds("n")
	nlo, nhi := fulltext2.JSONNumericTermBounds("n")
	require.Equal(t, fulltext2.JSONStringTerm("n", "m", "", false), p.Ranges[0].Lo)
	require.Equal(t, shi, p.Ranges[0].Hi)
	require.Equal(t, jsonTermRange{Lo: nlo, Hi: nhi}, p.Ranges[1],
		"the numeric side must stay untightened")
	require.Less(t, slo, shi)

	// every leaf that satisfies the predicate must land in one of the ranges
	opt := fulltext2.JSONTermOptions{IncludeKeys: true}
	covered := func(doc string) bool {
		bj, err := bytejson.ParseFromString(doc)
		require.NoError(t, err)
		for _, term := range fulltext2.JSONTupleTerms(bj, opt) {
			for _, r := range p.Ranges {
				if inTermRange(r, term) {
					return true
				}
			}
		}
		return false
	}
	require.True(t, covered(`{"n":"zebra"}`), "string leaf above the bound")
	require.True(t, covered(`{"n":9}`), "numeric leaf: rendering is compared, so it must be covered")
	require.True(t, covered(`{"n":1e300}`))
	require.True(t, covered(`{"n":-4.5}`))
}

// inTermRange mirrors the scan's own test: both ends inclusive.
func inTermRange(r jsonTermRange, term string) bool {
	return term >= r.Lo && term <= r.Hi
}

// A range must actually bracket the terms of documents that satisfy it.
func TestJSONProbeRangeBracketsMatchingDocuments(t *testing.T) {
	opt := fulltext2.JSONTermOptions{IncludeKeys: true}
	termOf := func(doc string) string {
		bj, err := bytejson.ParseFromString(doc)
		require.NoError(t, err)
		ts := fulltext2.JSONTupleTerms(bj, opt)
		require.Len(t, ts, 1)
		return ts[0]
	}
	inRange := func(p jsonProbe, term string) bool {
		require.Len(t, p.Ranges, 1)
		return inTermRange(p.Ranges[0], term)
	}

	p, ok := jsonExtractProbeFromExpr(jpCallExpr(">", jpExtractFloat(0, "$.n"), jpFltLit(3.0)), false)
	require.True(t, ok)
	require.True(t, inRange(p, termOf(`{"n":3.5}`)))
	require.True(t, inRange(p, termOf(`{"n":1e9}`)))
	require.True(t, inRange(p, termOf(`{"n":3.0}`)),
		"the bound itself is returned: a superset the retained predicate filters")
	require.False(t, inRange(p, termOf(`{"n":-5}`)), "below the bound is still excluded")

	p, ok = jsonExtractProbeFromExpr(jpCallExpr(">=", jpExtractFloat(0, "$.n"), jpFltLit(3.0)), false)
	require.True(t, ok)
	require.True(t, inRange(p, termOf(`{"n":3.0}`)), ">= includes the bound")
	require.True(t, inRange(p, termOf(`{"n":3}`)), "integer leaf normalizes into the same range")
}

// Reversed operand order is the same predicate with the operator flipped.
func TestJSONProbeFlipsReversedOperands(t *testing.T) {
	fwd, ok := jsonExtractProbeFromExpr(jpCallExpr("<", jpExtractFloat(0, "$.n"), jpFltLit(7)), false)
	require.True(t, ok)
	rev, ok := jsonExtractProbeFromExpr(jpCallExpr(">", jpFltLit(7), jpExtractFloat(0, "$.n")), false)
	require.True(t, ok)
	require.Equal(t, fwd, rev, "7 > x must probe the same range as x < 7")
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
	} {
		_, ok := jsonExtractProbeFromExpr(tc.expr, false)
		require.False(t, ok, tc.name)
	}
}

// A full-path index changes the term shape, so the probe must follow it — and
// ranges are declined there because the path sorts after the value.
func TestJSONProbeRespectsIndexTermShape(t *testing.T) {
	leaf, ok := jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractStr(0, "$.foo"), jpStrLit("bar")), false)
	require.True(t, ok)
	full, ok := jsonExtractProbeFromExpr(jpCallExpr("=", jpExtractStr(0, "$.foo"), jpStrLit("bar")), true)
	require.True(t, ok)
	require.NotEqual(t, leaf.Terms, full.Terms, "term shape must follow the index")

	_, ok = jsonExtractProbeFromExpr(jpCallExpr(">", jpExtractFloat(0, "$.n"), jpFltLit(1)), true)
	require.False(t, ok, "a value range is not contiguous once the path is appended")
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
