// Copyright 2021 Matrix Origin
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

package window

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// add unit tests for cases
type winTestCase struct {
	arg  *Window
	proc *process.Process
}

func makeTestCases(t *testing.T) []winTestCase {
	return []winTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Window{
				WinSpecList: []*plan.Expr{makeWindowSpec()},
				Types:       []types.Type{types.T_int32.ToType()},
				Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestWin(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *Window, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func makeWindowSpec() *plan.Expr {
	f := &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type:      plan.FrameBound_PRECEDING,
			UnBounded: true,
		},
		End: &plan.FrameBound{
			Type:      plan.FrameBound_FOLLOWING,
			UnBounded: true,
		},
	}
	return &plan.Expr{
		Typ: plan.Type{},
		Expr: &plan.Expr_W{
			W: &plan.WindowSpec{
				//OrderBy:    []*plan.OrderBySpec{&plan.OrderBySpec{Expr: newColExpr(0)}},
				WindowFunc: newFunExpr(),
				Frame:      f,
			},
		},
	}
}

func newColExpr(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newAggExpr() aggexec.AggFuncExecExpression {
	e, _ := function.GetFunctionByName(context.Background(), "sum", []types.Type{types.T_int32.ToType()})
	id := e.GetEncodedOverloadID()
	return aggexec.MakeAggFunctionExpression(id, false, []*plan.Expr{newColExpr(0)}, nil)
}

func newFunExpr() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: "sum",
				},
			},
		},
	}
}

func TestSearchLeftUnsupportedType(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType())
	err := vector.AppendBytes(vec, []byte("abc"), false, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	_, err = searchLeft(0, 1, 0, vec, nil, false, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}

func TestSearchLeftWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	// Simulate sorted order with ASC NULLS FIRST: [NULL, NULL, 1, 2, 2, 4]
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{0, 0, 1, 2, 2, 4}
	nullRows := []bool{true, true, false, false, false, false}

	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// NULL rows should be treated as peers
	// For rowIdx=0 (NULL), searchLeft should return 0 (start of NULL peer group)
	left, err := searchLeft(0, 6, 0, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, left, "NULL row at idx 0: all NULL peers should share the same left boundary")

	// For rowIdx=1 (NULL), searchLeft should also return 0 (peer with row 0)
	left, err = searchLeft(0, 6, 1, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, left, "NULL row at idx 1: should return start of NULL peer group, not its own index")

	// For non-NULL row (k=1 at idx=2), searchLeft with 1 PRECEDING should NOT include NULL rows
	// Target = 1 - 1 = 0, but NULL rows' raw value=0 should NOT match
	left, err = searchLeft(0, 6, 2, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 1},
			},
		},
	}, false, false)
	require.NoError(t, err)
	require.Equal(t, 2, left, "k=1 with 1 PRECEDING: should start at first non-NULL (idx 2), not include NULLs")
}

func TestSearchRightWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	// Simulate sorted order with ASC NULLS FIRST: [NULL, NULL, 1, 2, 2, 4]
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{0, 0, 1, 2, 2, 4}
	nullRows := []bool{true, true, false, false, false, false}

	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// NULL rows should be treated as peers
	// For rowIdx=0 (NULL), searchRight should return 2 (end of NULL peer group, exclusive)
	right, err := searchRight(0, 6, 0, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 2, right, "NULL row at idx 0: should return end of NULL peer group (idx 2)")

	// For rowIdx=1 (NULL), searchRight should also return 2
	right, err = searchRight(0, 6, 1, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 2, right, "NULL row at idx 1: should return end of NULL peer group (idx 2)")
}

// TestSearchLeftWithNullsDesc covers DESC NULLS LAST ordering.
// Raw values are [4, 2, 1, 0, 0] — NOT monotonically sorted!
// P2 must confine binary search to the non-NULL subrange [0, 3).
func TestSearchLeftWithNullsDesc(t *testing.T) {
	mp := mpool.MustNewZero()
	// DESC NULLS LAST: raw values = [4, 2, 1, 0, 0], nulls at positions 3, 4
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{4, 2, 1, 0, 0}
	nullRows := []bool{false, false, false, true, true}

	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// NULL rows should be treated as peers (DESC NULLS LAST)
	left, err := searchLeft(0, 5, 3, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 3, left, "NULL row at idx 3: all NULL peers share same left boundary (start of NULL group)")

	left, err = searchLeft(0, 5, 4, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 3, left, "NULL row at idx 4: should return start of NULL peer group (idx 3)")

	// Verify P2 correctly identifies the non-NULL data range [0, 3) when NULLs are at end.
	// After P2 trimming, start=0, end=3 for this [4, 2, 1, NULL, NULL] data.
	// This is validated indirectly: if P2 failed to trim, binary search would operate
	// on the full unsorted [4, 2, 1, 0, 0] and produce garbage results.
	// (Explicit CURRENT ROW search on non-NULL row not tested here because
	// genericSearchLeft assumes ascending order and is not DESC-aware.)
}

// TestSearchRightWithNullsDesc covers DESC NULLS LAST ordering.
// Raw values are [4, 2, 1, 0, 0] — NOT monotonically sorted!
func TestSearchRightWithNullsDesc(t *testing.T) {
	mp := mpool.MustNewZero()
	// DESC NULLS LAST: raw values = [4, 2, 1, 0, 0], nulls at positions 3, 4
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{4, 2, 1, 0, 0}
	nullRows := []bool{false, false, false, true, true}

	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// NULL rows are peers
	right, err := searchRight(0, 5, 3, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 5, right, "NULL row at idx 3: should return end of NULL peer group (idx 5)")

	right, err = searchRight(0, 5, 4, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 5, right, "NULL row at idx 4: should return end of NULL peer group (idx 5)")

	// Verify P2 correctly identifies the non-NULL data range [0, 3) when NULLs are at end.
	// After P2 trimming, start=0, end=3 for this [4, 2, 1, NULL, NULL] data.
	// (Explicit CURRENT ROW search on non-NULL row not tested here because
	// genericSearchEqualRight assumes ascending order and is not DESC-aware.)
}

// TestSearchLeftAllNulls verifies NULL peer grouping when all values are NULL.
func TestSearchLeftAllNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < 5; i++ {
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
	}
	defer vec.Free(mp)

	// All rows are NULL peers — every row should return 0 (start of the NULL group)
	left, err := searchLeft(0, 5, 0, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, left, "all NULL: row 0 should start at 0")

	left, err = searchLeft(0, 5, 4, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, left, "all NULL: row 4 should start at 0 (all peers)")
}

func TestSearchRightUnsupportedType(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType())
	err := vector.AppendBytes(vec, []byte("abc"), false, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	_, err = searchRight(0, 1, 0, vec, nil, false, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}

// TestSearchLeftDescRange verifies searchLeft with desc=true.
// DESC NULLS LAST ordering: raw values [4, 2, 2, 1, NULL, NULL].
func TestSearchLeftDescRange(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{4, 2, 2, 1, 0, 0}
	nullRows := []bool{false, false, false, false, true, true}
	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// CURRENT ROW (desc=true): find first equal to 2
	left, err := searchLeft(0, 4, 1, vec, nil, false, true)
	require.NoError(t, err)
	require.Equal(t, 1, left, "DESC CURRENT ROW k=2: should find first peer at idx 1")

	// 1 PRECEDING from k=2 (desc): target = 2+1 = 3, find first <= 3
	left, err = searchLeft(0, 4, 1, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, false, true)
	require.NoError(t, err)
	require.Equal(t, 1, left, "DESC k=2 1 PRECEDING: should find first <= 3 (idx 1, value 2)")

	// 1 PRECEDING from k=4 (desc): target = 4+1 = 5, find first <= 5
	left, err = searchLeft(0, 4, 0, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, false, true)
	require.NoError(t, err)
	require.Equal(t, 0, left, "DESC k=4 1 PRECEDING: should find first <= 5 (idx 0, value 4)")

	// 1 FOLLOWING from k=2 (desc): target = 2-1 = 1, find first <= 1
	left, err = searchLeft(0, 4, 1, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, true, true)
	require.NoError(t, err)
	require.Equal(t, 3, left, "DESC k=2 1 FOLLOWING: should find first <= 1 (idx 3, value 1)")
}

// TestSearchRightDescRange verifies searchRight with desc=true.
func TestSearchRightDescRange(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{4, 2, 2, 1, 0, 0}
	nullRows := []bool{false, false, false, false, true, true}
	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// CURRENT ROW (desc=true): find last equal to 2
	right, err := searchRight(0, 4, 1, vec, nil, false, true)
	require.NoError(t, err)
	require.Equal(t, 3, right, "DESC CURRENT ROW k=2: should find exclusive end after last peer (idx 3)")

	// 1 FOLLOWING from k=2 (desc): target = 2-1 = 1, find last >= 1
	right, err = searchRight(0, 4, 1, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, false, true)
	require.NoError(t, err)
	require.Equal(t, 4, right, "DESC k=2 1 FOLLOWING: should include idx 3 (value 1), exclusive end = 4")

	// 1 PRECEDING from k=1 (desc): target = 1+1 = 2, find last >= 2
	right, err = searchRight(0, 4, 3, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, true, true)
	require.NoError(t, err)
	require.Equal(t, 3, right, "DESC k=1 1 PRECEDING: should include up to idx 2 (value 2), exclusive end = 3")
}

// TestBuildRangeIntervalEmptyDesc verifies buildRangeInterval does not panic
// when ctr.desc is empty (RANGE frame without an ORDER BY spec).
func TestBuildRangeIntervalEmptyDesc(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{1, 2, 2, 4}
	for _, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	defer vec.Free(mp)

	ctr := &container{}
	ctr.orderVecs = make([]colexec.ExprEvalVector, 1)
	ctr.orderVecs[0].Vec = []*vector.Vector{vec}
	// ctr.desc intentionally left empty (no ORDER BY spec).

	// RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
	frame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}

	start, end, err := ctr.buildRangeInterval(3, 0, 4, frame)
	require.NoError(t, err)
	require.Equal(t, 3, start, "empty desc: 1 PRECEDING from value 4 starts at idx 3")
	require.Equal(t, 4, end, "empty desc: CURRENT ROW ends after last value 4")

	start, end, err = ctr.buildRangeInterval(1, 0, 4, frame)
	require.NoError(t, err)
	require.Equal(t, 0, start, "empty desc: 1 PRECEDING from value 2 reaches idx 0 (value 1)")
	require.Equal(t, 3, end, "empty desc: CURRENT ROW ends after last value 2")
}

// TestBuildRangeIntervalEmptyDescUnbounded verifies the UNBOUNDED branches
// also tolerate an empty ctr.desc.
func TestBuildRangeIntervalEmptyDescUnbounded(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{1, 2, 2, 4}
	for _, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	defer vec.Free(mp)

	ctr := &container{}
	ctr.orderVecs = make([]colexec.ExprEvalVector, 1)
	ctr.orderVecs[0].Vec = []*vector.Vector{vec}
	// ctr.desc intentionally left empty.

	frame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}

	start, end, err := ctr.buildRangeInterval(1, 0, 4, frame)
	require.NoError(t, err)
	require.Equal(t, 0, start, "empty desc: UNBOUNDED PRECEDING keeps start at 0")
	require.Equal(t, 3, end, "empty desc: CURRENT ROW ends after last value 2")
}

// ---------------------------------------------------------------------------
// All-type coverage tests — each type case in searchLeft/searchRight must be
// exercised at least once by a test to satisfy the 75% diff-coverage gate.
// Data layout: sorted ascending [1, 2, 2, 4] (no NULLs).
//   - ASC CURRENT ROW at idx 1 (value 2):    left=1, right=3
//   - DESC CURRENT ROW at idx 1 (value 2):   left=1, right=3 (equal ignores order)
//   - ASC 1 PRECEDING  at idx 1 (value 2):   left=0 (first >= 1)
//   - ASC 1 FOLLOWING  at idx 1 (value 2):   right=4 (last  <= 3 is idx 3, excl=4)
// ---------------------------------------------------------------------------

// helper to build a simple fixed-type vector without nulls.
func makeFixedVec[T types.OrderedT](t *testing.T, mp *mpool.MPool, oid types.T, values []T) *vector.Vector {
	vec := vector.NewVec(oid.ToType())
	for _, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

// Type-specific literal helpers — each searchLeft/searchRight type case casts to
// a distinct proto literal type, so a generic I64Val would panic on e.g. int8.
func i8Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 1}}}}
}
func i16Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 1}}}}
}
func i32Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 1}}}}
}
func i64Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
}
func u8Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 1}}}}
}
func u16Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 1}}}}
}
func u32Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 1}}}}
}
func u64Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}}}
}
func f32Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: 1}}}}
}
func f64Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 1}}}}
}

// testSearchLeftRightNumeric covers each type's case block in both searchLeft and
// searchRight. ASC tests use [1,2,2,4] (ascending order). DESC tests use [4,2,2,1]
// (descending order) so the binary-search comparators are exercised correctly.
func testSearchLeftRightNumeric[T types.OrderedT](t *testing.T, mp *mpool.MPool,
	oid types.T, ascValues, descValues []T, litOffset func() *plan.Expr,
) {
	// ── ASC (data in ascending order) ──
	vec := makeFixedVec[T](t, mp, oid, ascValues)
	defer vec.Free(mp)
	n := len(ascValues)

	l, err := searchLeft(0, n, 1, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 1, l, "ASC CURRENT ROW left")

	r, err := searchRight(0, n, 1, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 3, r, "ASC CURRENT ROW right")

	l, err = searchLeft(0, n, 1, vec, litOffset(), false, false)
	require.NoError(t, err)
	require.Equal(t, 0, l, "ASC 1 PRECEDING left")

	// 1 FOLLOWING at idx 1 (value 2): target 3, last <= 3 is idx 2 → right = 3
	r, err = searchRight(0, n, 1, vec, litOffset(), false, false)
	require.NoError(t, err)
	require.Equal(t, 3, r, "ASC 1 FOLLOWING right")

	vec.Free(mp)

	// ── DESC (data in descending order) ──
	vec2 := makeFixedVec[T](t, mp, oid, descValues)
	defer vec2.Free(mp)

	l, err = searchLeft(0, n, 0, vec2, nil, false, true)
	require.NoError(t, err)
	require.Equal(t, 0, l, "DESC CURRENT ROW left (value 4)")

	r, err = searchRight(0, n, 0, vec2, nil, false, true)
	require.NoError(t, err)
	require.Equal(t, 1, r, "DESC CURRENT ROW right (value 4)")
}

// TestSearchLeftRightAllIntTypes covers int8/16/32/64 + bit.
func TestSearchLeftRightAllIntTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("int8", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_int8,
			[]int8{1, 2, 2, 4}, []int8{4, 2, 2, 1}, i8Lit)
	})
	t.Run("int16", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_int16,
			[]int16{1, 2, 2, 4}, []int16{4, 2, 2, 1}, i16Lit)
	})
	t.Run("int32", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_int32,
			[]int32{1, 2, 2, 4}, []int32{4, 2, 2, 1}, i32Lit)
	})
	t.Run("int64", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_int64,
			[]int64{1, 2, 2, 4}, []int64{4, 2, 2, 1}, i64Lit)
	})
	t.Run("bit", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_bit,
			[]uint64{1, 2, 2, 4}, []uint64{4, 2, 2, 1}, u64Lit)
	})
}

// TestSearchLeftRightAllUintTypes covers uint8/16/32/64.
func TestSearchLeftRightAllUintTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("uint8", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_uint8,
			[]uint8{1, 2, 2, 4}, []uint8{4, 2, 2, 1}, u8Lit)
	})
	t.Run("uint16", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_uint16,
			[]uint16{1, 2, 2, 4}, []uint16{4, 2, 2, 1}, u16Lit)
	})
	t.Run("uint32", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_uint32,
			[]uint32{1, 2, 2, 4}, []uint32{4, 2, 2, 1}, u32Lit)
	})
	t.Run("uint64", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_uint64,
			[]uint64{1, 2, 2, 4}, []uint64{4, 2, 2, 1}, u64Lit)
	})
}

// TestSearchLeftRightAllFloatTypes covers float32/64.
func TestSearchLeftRightAllFloatTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("float32", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_float32,
			[]float32{1, 2, 2, 4}, []float32{4, 2, 2, 1}, f32Lit)
	})
	t.Run("float64", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_float64,
			[]float64{1, 2, 2, 4}, []float64{4, 2, 2, 1}, f64Lit)
	})
}

// TestSearchLeftRightDecimalTypes covers decimal64/128.
func TestSearchLeftRightDecimalTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("decimal64", func(t *testing.T) {
		// ASC
		vec := testutil.NewDecimal64Vector(0, types.T_decimal64.ToType(), mp, false, nil,
			[]types.Decimal64{1, 2, 2, 4},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		// DESC: descending data [4, 2, 2, 1]
		vec2 := testutil.NewDecimal64Vector(0, types.T_decimal64.ToType(), mp, false, nil,
			[]types.Decimal64{4, 2, 2, 1},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})

	t.Run("decimal128", func(t *testing.T) {
		vec := testutil.NewDecimal128Vector(0, types.T_decimal128.ToType(), mp, false, nil,
			[]types.Decimal128{
				{B0_63: 1, B64_127: 0}, {B0_63: 2, B64_127: 0},
				{B0_63: 2, B64_127: 0}, {B0_63: 4, B64_127: 0},
			},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewDecimal128Vector(0, types.T_decimal128.ToType(), mp, false, nil,
			[]types.Decimal128{
				{B0_63: 4, B64_127: 0}, {B0_63: 2, B64_127: 0},
				{B0_63: 2, B64_127: 0}, {B0_63: 1, B64_127: 0},
			},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}

// TestSearchLeftRightDateTimeTypes covers date/datetime/time/timestamp (CURRENT ROW only).
func TestSearchLeftRightDateTimeTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("date", func(t *testing.T) {
		vec := testutil.NewDateVector(0, types.T_date.ToType(), mp, false, nil,
			[]string{"2024-01-01", "2024-01-02", "2024-01-02", "2024-01-04"},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewDateVector(0, types.T_date.ToType(), mp, false, nil,
			[]string{"2024-01-04", "2024-01-02", "2024-01-02", "2024-01-01"},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})

	t.Run("datetime", func(t *testing.T) {
		vec := testutil.NewDatetimeVector(0, types.T_datetime.ToType(), mp, false, nil,
			[]string{"2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-04 10:00:00"},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewDatetimeVector(0, types.T_datetime.ToType(), mp, false, nil,
			[]string{"2024-01-04 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-01 10:00:00"},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})

	t.Run("time", func(t *testing.T) {
		vec := testutil.NewTimeVector(0, types.T_time.ToType(), mp, false, nil,
			[]string{"10:00:00", "12:00:00", "12:00:00", "14:00:00"},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewTimeVector(0, types.T_time.ToType(), mp, false, nil,
			[]string{"14:00:00", "12:00:00", "12:00:00", "10:00:00"},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})

	t.Run("timestamp", func(t *testing.T) {
		vec := testutil.NewTimestampVector(0, types.T_timestamp.ToType(), mp, false, nil,
			[]string{"2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-04 10:00:00"},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewTimestampVector(0, types.T_timestamp.ToType(), mp, false, nil,
			[]string{"2024-01-04 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-01 10:00:00"},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}
