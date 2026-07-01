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
