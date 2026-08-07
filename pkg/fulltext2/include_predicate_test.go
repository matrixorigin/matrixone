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

package fulltext2

import (
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// incIdx builds a 5-doc index where every doc contains the term "x" (so a disjunctive query
// "x" matches all) plus two INCLUDE columns: col0 status(varchar), col1 prio(int64).
func incIdx(t *testing.T) *Index {
	docs := []TokenizedDoc{
		{Pk: int64(1), Terms: []string{"x"}, Positions: []int32{0}, Include: []any{[]byte("active"), int64(10)}},
		{Pk: int64(2), Terms: []string{"x"}, Positions: []int32{0}, Include: []any{[]byte("inactive"), int64(20)}},
		{Pk: int64(3), Terms: []string{"x"}, Positions: []int32{0}, Include: []any{[]byte("active"), int64(30)}},
		{Pk: int64(4), Terms: []string{"x"}, Positions: []int32{0}, Include: []any{nil, int64(40)}}, // NULL status
		{Pk: int64(5), Terms: []string{"x"}, Positions: []int32{0}, Include: []any{[]byte("archived"), int64(5)}},
	}
	incTypes := []int32{int32(types.T_varchar), int32(types.T_int64)}
	seg, err := BuildSegmentFromTokenized("inc", int32(types.T_int64), docs, WithIncludeTypes(incTypes))
	require.NoError(t, err)
	return NewIndex([]*Segment{seg}, nil)
}

// runPrefilter compiles specJSON and runs a disjunctive "x" search with the resulting
// INCLUDE prefilter, returning the matched pk set.
func runPrefilter(t *testing.T, idx *Index, specJSON string) []int64 {
	preds, err := compileIncludePredicates([]byte(specJSON), idx.includeTypes(), idx.pkType())
	require.NoError(t, err)
	res, err := idx.SearchQuery([]byte("x"), true, ParserDefault, BM25, 100, &prefilter{include: preds})
	require.NoError(t, err)
	ids := resultIDs(res)
	out := make([]int64, len(ids))
	for i, v := range ids {
		out[i] = v.(int64)
	}
	return out
}

// TestIncludePrefilterOps exercises the in-index INCLUDE prefilter end-to-end (compile →
// membership → search) across the op set, including string prefix, ranges, IN, and 3-valued
// NULL logic. status: 1=active 2=inactive 3=active 4=NULL 5=archived; prio: 1=10..5=5.
func TestIncludePrefilterOps(t *testing.T) {
	idx := incIdx(t)

	require.ElementsMatch(t, []int64{1, 3}, runPrefilter(t, idx, `[{"col":0,"op":"=","val":"active"}]`))
	require.ElementsMatch(t, []int64{1, 3, 5}, runPrefilter(t, idx, `[{"col":0,"op":"prefix","val":"a"}]`)) // active/archived
	require.ElementsMatch(t, []int64{1, 3, 5}, runPrefilter(t, idx, `[{"col":0,"op":"in","vals":["active","archived"]}]`))
	require.ElementsMatch(t, []int64{2, 5}, runPrefilter(t, idx, `[{"col":0,"op":"!=","val":"active"}]`)) // inactive/archived (NULL excluded)
	require.ElementsMatch(t, []int64{4}, runPrefilter(t, idx, `[{"col":0,"op":"is_null"}]`))
	require.ElementsMatch(t, []int64{1, 2, 3, 5}, runPrefilter(t, idx, `[{"col":0,"op":"is_not_null"}]`))

	// int column: range / comparisons (bare JSON numbers).
	require.ElementsMatch(t, []int64{1, 2, 3}, runPrefilter(t, idx, `[{"col":1,"op":"between","lo":10,"hi":30}]`))
	require.ElementsMatch(t, []int64{4}, runPrefilter(t, idx, `[{"col":1,"op":">","val":30}]`))
	require.Empty(t, runPrefilter(t, idx, `[{"col":1,"op":">","val":100}]`))

	// conjunction: status=active AND prio>=30 → only pk3.
	require.ElementsMatch(t, []int64{3}, runPrefilter(t, idx,
		`[{"col":0,"op":"=","val":"active"},{"col":1,"op":">=","val":30}]`))

	// empty array → all 5.
	require.Len(t, runPrefilter(t, idx, `[]`), 5)

	// PRIMARY KEY predicates (col -1, the pk sentinel) — evaluated inline against the stored
	// pk, no docfilter second-scan.
	require.ElementsMatch(t, []int64{2, 3, 4}, runPrefilter(t, idx, `[{"col":-1,"op":"between","lo":2,"hi":4}]`))
	require.ElementsMatch(t, []int64{1, 5}, runPrefilter(t, idx, `[{"col":-1,"op":"in","vals":[1,5]}]`))
	require.ElementsMatch(t, []int64{4, 5}, runPrefilter(t, idx, `[{"col":-1,"op":">","val":3}]`))
	// pk predicate ANDed with an INCLUDE predicate: id>2 AND status=active → {3}.
	require.ElementsMatch(t, []int64{3}, runPrefilter(t, idx,
		`[{"col":-1,"op":">","val":2},{"col":0,"op":"=","val":"active"}]`))
}

// TestIncludePredicateUnit covers the compiled-predicate evaluator directly, incl. the
// NULL 3-valued rule (a value comparison against NULL is UNKNOWN ⇒ not admitted).
func TestIncludePredicateUnit(t *testing.T) {
	// varchar eq
	p := compiledIncludePred{col: 0, kind: incEq, isStr: true, strs: [][]byte{[]byte("active")}}
	require.True(t, p.test([]byte("active"), false))
	require.False(t, p.test([]byte("other"), false))
	require.False(t, p.test(nil, true)) // NULL vs value ⇒ excluded

	// int between
	pb := compiledIncludePred{col: 1, kind: incBetween, ints: []int64{10, 20}}
	require.True(t, pb.test(int64(15), false))
	require.True(t, pb.test(int32(10), false)) // narrower int type normalized
	require.False(t, pb.test(int64(21), false))
	require.False(t, pb.test(nil, true))

	// isnull / isnotnull
	require.True(t, (&compiledIncludePred{kind: incIsNull}).test(nil, true))
	require.False(t, (&compiledIncludePred{kind: incIsNull}).test([]byte("x"), false))
	require.True(t, (&compiledIncludePred{kind: incIsNotNull}).test([]byte("x"), false))
}

// TestIncludePredicateUint64 pins the unsigned path: a uint64 column with values ABOVE
// MaxInt64 must compare as uint64. The old int64 path wrapped such a stored value to
// negative (and saturated the operand), silently dropping a matching row.
func TestIncludePredicateUint64(t *testing.T) {
	const big = uint64(math.MaxInt64) + 100 // 9223372036854775907, > MaxInt64

	// compile a pk predicate (col=-1) on a T_uint64 pk with a huge operand: unsigned path,
	// operand parsed into uints (not wrapped/saturated).
	preds, err := compileIncludePredicates(
		[]byte(fmt.Sprintf(`[{"col":-1,"op":"=","val":%d}]`, big)), nil, int32(types.T_uint64))
	require.NoError(t, err)
	require.Len(t, preds, 1)
	require.True(t, preds[0].isUnsigned)
	require.Equal(t, []uint64{big}, preds[0].uints)
	require.True(t, preds[0].test(big, false)) // exact match (int64 path would miss it)
	require.False(t, preds[0].test(big-1, false))
	require.False(t, preds[0].test(uint64(1), false))

	// '>' at the MaxInt64 boundary: big > MaxInt64 must be TRUE (int64 wrap would say false).
	gt := compiledIncludePred{col: 0, kind: incGt, isUnsigned: true, uints: []uint64{uint64(math.MaxInt64)}}
	require.True(t, gt.test(big, false))
	require.False(t, gt.test(uint64(5), false))

	// IN including a huge value.
	in := compiledIncludePred{col: 0, kind: incIn, isUnsigned: true, uints: []uint64{1, big}}
	require.True(t, in.test(big, false))
	require.True(t, in.test(uint64(1), false))
	require.False(t, in.test(uint64(2), false))
}

// TestCompileIncludePredicatesErrors: bad op / out-of-range col / bad int literal are rejected.
func TestCompileIncludePredicatesErrors(t *testing.T) {
	it := []int32{int32(types.T_varchar), int32(types.T_int64)}
	pk := int32(types.T_int64)
	_, err := compileIncludePredicates([]byte(`[{"col":0,"op":"bogus","val":"a"}]`), it, pk)
	require.Error(t, err)
	_, err = compileIncludePredicates([]byte(`[{"col":9,"op":"=","val":"a"}]`), it, pk)
	require.Error(t, err)
	_, err = compileIncludePredicates([]byte(`[{"col":1,"op":"=","val":"notanint"}]`), it, pk)
	require.Error(t, err)
	// a pk predicate on a non-comparable pk type (decimal128) is rejected (planner falls back).
	_, err = compileIncludePredicates([]byte(`[{"col":-1,"op":"=","val":1}]`), it, int32(types.T_decimal128))
	require.Error(t, err)
	// empty spec → nil, no error.
	preds, err := compileIncludePredicates(nil, it, pk)
	require.NoError(t, err)
	require.Nil(t, preds)
}

// TestResultCarriesInclude pins the covering primitive: a top-k search Result carries the
// doc's INCLUDE column values (in index order, NULL-aware) across the WAND/boolean path
// (heapToResults) and the NL phrase path (boundedTopK) — so a covering query is served from
// the index. Uses the incIdx fixture (col0 status varchar, col1 prio int64).
func TestResultCarriesInclude(t *testing.T) {
	idx := incIdx(t)

	byPk := func(res []Result) map[int64][]any {
		m := map[int64][]any{}
		for _, r := range res {
			m[r.Pk.(int64)] = r.Include
		}
		return m
	}

	// Boolean/WAND path (disjunctive "x").
	wand, err := idx.SearchQuery([]byte("x"), true, ParserDefault, BM25, 100, nil)
	require.NoError(t, err)
	mw := byPk(wand)
	require.Equal(t, []any{[]byte("active"), int64(10)}, mw[1])
	require.Equal(t, []any{nil, int64(40)}, mw[4]) // NULL status preserved

	// NL phrase path (boundedTopK).
	nl, err := idx.SearchQuery([]byte("x"), false, ParserDefault, BM25, 100, nil)
	require.NoError(t, err)
	mn := byPk(nl)
	require.Equal(t, []any{[]byte("archived"), int64(5)}, mn[5])

	// An index WITHOUT include columns → Result.Include is nil.
	seg, err := BuildSegmentFromTokenized("noinc", int32(types.T_int64),
		[]TokenizedDoc{{Pk: int64(1), Terms: []string{"x"}, Positions: []int32{0}}})
	require.NoError(t, err)
	res, err := NewIndex([]*Segment{seg}, nil).SearchQuery([]byte("x"), true, ParserDefault, BM25, 10, nil)
	require.NoError(t, err)
	require.Nil(t, res[0].Include)
}
