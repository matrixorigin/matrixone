// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// issue #30729 public-SQL binder/plan regression: inside IF, an unsized
// CAST(<real column> AS BINARY) next to an untyped NULL must keep the
// unsized binary projection width -1 so that later execution preserves the
// cast bytes ("hello-long" stays 10 bytes) instead of widening to
// BINARY(65535) and padding them. IFNULL/CASE/COALESCE/direct-unsized forms
// already resolve through binaryStringCommonType; they are pinned here as
// unchanged controls, and explicit BINARY(3) keeps the fixed-width
// truncate/pad semantics.
func TestQueryIfUnsizedBinaryNullProjection(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		width int32
	}{
		{name: "if null then cast else",
			sql:   `select if(n_nationkey = 1, null, cast(n_name as binary)) from nation`,
			width: -1},
		{name: "if cast then null else",
			sql:   `select if(n_nationkey = 1, cast(n_name as binary), null) from nation`,
			width: -1},
		{name: "if explicit binary(3)",
			sql:   `select if(n_nationkey = 1, null, cast(n_name as binary(3))) from nation`,
			width: 3},
		{name: "direct unsized control",
			sql:   `select cast(n_name as binary) from nation`,
			width: -1},
		{name: "ifnull control",
			sql:   `select ifnull(cast(n_name as binary), null) from nation`,
			width: -1},
		{name: "searched case control",
			sql:   `select case when n_nationkey = 1 then null else cast(n_name as binary) end from nation`,
			width: -1},
		{name: "coalesce control",
			sql:   `select coalesce(cast(n_name as binary), null) from nation`,
			width: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmts, err := mysql.Parse(mock.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			query, err := NewBaseOptimizer(mock).Optimize(stmts[0], false, nil)
			require.NoError(t, err)
			node := query.Nodes[query.Steps[0]]
			require.Equal(t, planpb.Node_PROJECT, node.NodeType, "root of a plain select must be PROJECT")
			require.NotEmpty(t, node.ProjectList)
			typ := node.ProjectList[0].Typ
			require.Equal(t, int32(types.T_binary), typ.Id,
				"the selected cast branch must drive a binary projection")
			require.Equal(t, tc.width, typ.Width,
				"projected width must preserve the unsized/fixed binary target")
		})
	}
}

// TestIfUnsizedBinaryNullBoundEval binds the public IF expression through the
// same binder path SQL statements use (eq on a column predicate that cannot
// const-fold, explicit user CAST(v AS BINARY), NULL constant branch), then
// runs the bound expression on a real src fixture
// (id INT, v VARCHAR(20), 'hello-long') through the real expression executor.
// It asserts the exact selected bytes and NULL row: 'hello-long' must remain
// 10 bytes (0x68656C6C6F2D6C6F6E67) in both NULL orders, and BINARY(3) keeps
// truncating to 'hel'. This is the operator-consumer proof for issue #30729:
// no 65535 clamp may leak into the inserted branch CASTs.
func TestIfUnsizedBinaryNullBoundEval(t *testing.T) {
	ctx := context.Background()
	idTyp := types.T_int32.ToType()
	vTyp := types.New(types.T_varchar, 20, 0)

	// Non-foldable column predicate, like the issue's `IF(id = 0, ...)`.
	// 'eq' is an internal overload resolved by the binder itself, so compose
	// the comparison node the way the binder emits it: the resolver's encoded
	// overload id plus the raw column/constant operands.
	idCol := &Expr{
		Typ: idTyp.PlanType(),
		Expr: &planpb.Expr_Col{
			Col: &ColRef{RelPos: 0, ColPos: 0, Name: "id"},
		},
	}
	// `=` is the registered function-id name for equality (function_id.go);
	// the composed Function node mirrors what the binder emits for `id = 0`.
	eqGet, err := function.GetFunctionByName(ctx, "=", []types.Type{idTyp, idTyp})
	require.NoError(t, err)
	condExpr := &Expr{
		Typ: eqGet.GetReturnType().PlanType(),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: eqGet.GetEncodedOverloadID(), ObjName: "eq"},
			Args: []*Expr{idCol, makePlan2Int32ConstExprWithType(0)},
		}},
	}

	newSrcBatch := func(t *testing.T, mp *mpool.MPool) *batch.Batch {
		t.Helper()
		idVec := vector.NewVec(idTyp)
		require.NoError(t, vector.AppendFixedList(idVec, []int32{1, 0}, nil, mp))
		vVec := testutil.MakeVarlenaVector(
			[][]byte{[]byte("hello-long"), []byte("hello-long")}, nil, vTyp, mp)
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = idVec
		bat.Vecs[1] = vVec
		bat.SetRowCount(2)
		return bat
	}

	runCase := func(t *testing.T, binaryWidth int32, nullLast bool, wantBytes []byte, wantNullRow0 bool) {
		t.Helper()
		eproc := testutil.NewProcess(t)
		castExpr, err := makePlan2CastExpr(ctx, &Expr{
			Typ: vTyp.PlanType(),
			Expr: &planpb.Expr_Col{
				Col: &ColRef{RelPos: 0, ColPos: 1, Name: "v"},
			},
		}, Type{Id: int32(types.T_binary), Width: binaryWidth, Scale: -1})
		require.NoError(t, err)
		nullExpr := makePlan2NullConstExprWithType()

		var ifExpr *Expr
		if nullLast {
			ifExpr, err = BindFuncExprImplByPlanExpr(ctx, "if",
				[]*Expr{condExpr, castExpr, nullExpr})
		} else {
			ifExpr, err = BindFuncExprImplByPlanExpr(ctx, "if",
				[]*Expr{condExpr, nullExpr, castExpr})
		}
		require.NoError(t, err)
		require.Equal(t, binaryWidth, ifExpr.Typ.Width,
			"bound IF width must keep the cast target width")

		srcBat := newSrcBatch(t, eproc.Mp())
		defer srcBat.Clean(eproc.Mp()) // released after the executor on every path
		executor, err := colexec.NewExpressionExecutor(eproc, ifExpr)
		require.NoError(t, err)
		defer executor.Free() // result aliases go before the caller-owned batch
		res, err := executor.Eval(eproc, []*batch.Batch{srcBat}, nil)
		require.NoError(t, err)
		require.Equal(t, types.T_binary, res.GetType().Oid)
		require.Equal(t, binaryWidth, res.GetType().Width,
			"evaluated vector width must keep the unsized/fixed binary target")

		// id = 0 is FALSE for row 0 (id=1) and TRUE for row 1 (id=0): with
		// NULL in the then branch (nullLast=false, the issue SQL shape) row 0
		// selects the cast bytes and row 1 the NULL; with NULL as the else
		// branch (nullLast=true) the two rows swap.
		if wantNullRow0 {
			require.True(t, res.IsNull(0))
			require.False(t, res.IsNull(1))
			require.Equal(t, wantBytes, res.GetBytesAt(1))
		} else {
			require.False(t, res.IsNull(0))
			require.True(t, res.IsNull(1))
			require.Equal(t, wantBytes, res.GetBytesAt(0))
		}
	}

	t.Run("null then cast else keeps bytes", func(t *testing.T) {
		runCase(t, -1, false, []byte("hello-long"), false)
	})
	t.Run("cast then null else keeps bytes", func(t *testing.T) {
		runCase(t, -1, true, []byte("hello-long"), true)
	})
	t.Run("binary(3) truncation control", func(t *testing.T) {
		runCase(t, 3, false, []byte("hel"), false)
	})
}
