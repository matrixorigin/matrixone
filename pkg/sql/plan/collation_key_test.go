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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCollationProbeExpressionMatchesStoredTuple(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	col := pb.Type{
		Id:               int32(types.T_varchar),
		Width:            5,
		Charset:          uint32(types.CharsetUTF8),
		CollationVersion: uint32(types.CollationVersionV1),
	}
	for _, value := range []string{"Alpha", "alpha ", "a\x00", "a \x00", "a b", "a value longer than the column width", "中😀"} {
		input := makePlan2StringConstExprWithType(value)
		key, err := MakeCollationKeyExpr(proc.Ctx, input, col, types.PADSpaceKeyV1)
		require.NoError(t, err)
		require.Equal(t, uint32(types.CharsetBinary), key.Typ.Charset)
		again, err := MakeCollationKeyExpr(proc.Ctx, key, col, types.PADSpaceKeyV1)
		require.NoError(t, err)
		require.Same(t, key, again)
		serial, err := BindFuncExprImplByPlanExpr(proc.Ctx, "serial", []*pb.Expr{makePlan2Int64ConstExprWithType(42), key})
		require.NoError(t, err)
		// Exercise the persisted plan transport and the real vector executor.
		encoded, err := serial.Marshal()
		require.NoError(t, err)
		var restored pb.Expr
		require.NoError(t, restored.Unmarshal(encoded))
		exec, err := colexec.NewExpressionExecutor(proc, &restored)
		require.NoError(t, err)
		vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		require.NoError(t, err)
		part, err := types.ResolveStringKeyPart(types.NewWithCharset(types.T_varchar, 5, 0, types.CharsetUTF8), types.PADSpaceKeyV1)
		require.NoError(t, err)
		p := types.NewPacker()
		p.EncodeInt64(42)
		_, err = part.Encode(p, nil, []byte(value))
		require.NoError(t, err)
		require.Equal(t, p.GetBuf(), vec.GetBytesAt(0), "probe must not be truncated to column width")
		p.Close()
		exec.Free()
		legacy, err := MakeCollationKeyExpr(proc.Ctx, input, col, types.LegacyKeyFormat)
		require.NoError(t, err)
		require.Same(t, input, legacy)
	}
}

func TestCollationKeyRejectsUnknownMetadataBeforeNarrowing(t *testing.T) {
	input := makePlan2StringConstExprWithType("Alpha")
	_, err := MakeCollationKeyExpr(context.Background(), input, pb.Type{
		Id:               int32(types.T_varchar),
		Charset:          uint32(types.CharsetUTF8),
		CollationVersion: 256,
	}, types.PADSpaceKeyV1)
	require.ErrorContains(t, err, "unsupported collation semantic version")

	_, err = MakeCollationKeyExpr(context.Background(), input, pb.Type{
		Id:      int32(types.T_varchar),
		Charset: 256,
	}, types.PADSpaceKeyV1)
	require.ErrorContains(t, err, "unsupported collation identity")
}

func TestNative0900ComparisonPromotesUntypedLiteralToComparisonKey(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	nativeType := pb.Type{
		Id:      int32(types.T_varchar),
		Width:   types.MaxVarcharLen,
		Charset: uint32(types.CharsetUTF8MB40900AI),
	}
	column := &pb.Expr{
		Typ:  nativeType,
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 1, ColPos: 0, Name: "name"}},
	}
	literal := makePlan2StringConstExprWithType("Alpha")
	comparison, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*pb.Expr{column, literal})
	require.NoError(t, err)
	require.NotNil(t, comparison.GetF())
	require.Len(t, comparison.GetF().Args, 2)
	for _, arg := range comparison.GetF().Args {
		require.NotNil(t, arg.GetF(), "both operands must use the native comparison identity: %#v", arg)
		require.Equal(t, "internal_collation_key", arg.GetF().Func.ObjName)
	}
}

func TestExplicitNative0900CollateSetsExpressionIdentity(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select 'Alpha' collate utf8mb4_0900_ai_ci", 1)
	require.NoError(t, err)
	defer stmt.Free()
	selectStmt := stmt.(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	binder := NewDefaultBinder(context.Background(), nil, nil, pb.Type{}, nil)
	bound, err := binder.BindExpr(selectClause.Exprs[0].Expr, 0, false)
	require.NoError(t, err)
	require.Equal(t, uint32(types.CharsetUTF8MB40900AI), bound.Typ.Charset)
	require.Equal(t, int32(types.T_varchar), bound.Typ.Id)
	require.NotNil(t, bound.GetF())
	require.True(t, bound.GetF().ExplicitCollation)

	serialized, err := bound.Marshal()
	require.NoError(t, err)
	var restored pb.Expr
	require.NoError(t, restored.Unmarshal(serialized))
	require.True(t, restored.GetF().ExplicitCollation)
	require.True(t, restored.Typ.CollationCoercibilitySet)
	require.Equal(t, uint32(0), restored.Typ.CollationCoercibility)
}

func TestExplicitCollationWinsOverColumnIdentity(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select 'Alpha' collate utf8mb4_0900_ai_ci", 1)
	require.NoError(t, err)
	defer stmt.Free()
	selectStmt := stmt.(*tree.Select)
	binder := NewDefaultBinder(context.Background(), nil, nil, pb.Type{}, nil)
	explicit, err := binder.BindExpr(selectStmt.Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
	require.NoError(t, err)
	column := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB4Bin)},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 1, ColPos: 0, Name: "name"}},
	}
	comparison, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*pb.Expr{explicit, column})
	require.NoError(t, err)
	require.Len(t, comparison.GetF().Args, 2)
	for _, arg := range comparison.GetF().Args {
		require.Equal(t, "internal_collation_key", arg.GetF().Func.ObjName)
	}
	require.Equal(t, uint32(types.CharsetUTF8MB40900AI), comparison.GetF().Args[1].GetF().Args[0].Typ.Charset)
}

func TestExplicitCollationWinsOverColumnIdentityInList(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select 'Alpha' collate utf8mb4_0900_ai_ci", 1)
	require.NoError(t, err)
	defer stmt.Free()
	selectStmt := stmt.(*tree.Select)
	binder := NewDefaultBinder(context.Background(), nil, nil, pb.Type{}, nil)
	explicit, err := binder.BindExpr(selectStmt.Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
	require.NoError(t, err)
	nativeType := pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)}
	left := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB4Bin)},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 1, ColPos: 0, Name: "name"}},
	}
	second := makePlan2StringConstExprWithType("Beta")
	second.Typ = nativeType
	right := &pb.Expr{
		Typ:  nativeType,
		Expr: &pb.Expr_List{List: &pb.ExprList{List: []*pb.Expr{explicit, second}}},
	}
	bound, err := BindFuncExprImplByPlanExpr(context.Background(), "in", []*pb.Expr{left, right})
	require.NoError(t, err)
	require.True(t, exprContainsFuncName(bound, "internal_collation_key"))
}

func TestRowValueINDerivesCollationPerField(t *testing.T) {
	ai := pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)}
	bin := pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900Bin)}
	value := func(text string, typ pb.Type) *pb.Expr {
		expr := makePlan2StringConstExprWithType(text)
		expr.Typ = typ
		return expr
	}
	left := &pb.Expr{Expr: &pb.Expr_List{List: &pb.ExprList{List: []*pb.Expr{
		value("a", ai),
		value("b", bin),
	}}}}
	right := &pb.Expr{Expr: &pb.Expr_List{List: &pb.ExprList{List: []*pb.Expr{
		{Expr: &pb.Expr_List{List: &pb.ExprList{List: []*pb.Expr{
			value("a", ai),
			value("b", bin),
		}}}},
	}}}}
	bound, err := BindFuncExprImplByPlanExpr(context.Background(), "in", []*pb.Expr{left, right})
	require.NoError(t, err)
	require.NotNil(t, bound)
}

func TestOrdinaryStringFunctionInheritsColumnCoercibility(t *testing.T) {
	column := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0, Name: "name"}},
	}
	lower := &pb.Expr{
		Typ: pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "lower"},
			Args: []*pb.Expr{column},
		}},
	}
	candidate, ok, err := collationCandidateForExpr(lower)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint8(2), candidate.rank)
	require.Equal(t, uint8(types.CharsetUTF8MB40900AI), candidate.charset)
}

func TestConcatPreservesExplicitCollationConflict(t *testing.T) {
	leftLiteral := makePlan2StringConstExprWithType("a")
	left := &pb.Expr{
		Typ: pb.Type{
			Id:                       int32(types.T_varchar),
			Charset:                  uint32(types.CharsetUTF8MB40900AI),
			CollationCoercibility:    0,
			CollationCoercibilitySet: true,
		},
		Expr: leftLiteral.Expr,
	}
	concat := &pb.Expr{
		Typ: pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "concat"},
			Args: []*pb.Expr{left},
		}},
	}
	right := makePlan2StringConstExprWithType("a")
	right.Typ = pb.Type{
		Id:                       int32(types.T_varchar),
		Charset:                  uint32(types.CharsetUTF8MB40900Bin),
		CollationCoercibility:    0,
		CollationCoercibilitySet: true,
	}
	err := normalizeCollationCoercibilityArgs(context.Background(), "=", []*pb.Expr{concat, right})
	require.ErrorContains(t, err, "illegal mix of collations")
}

func TestNativeCollationMetadataSurvivesCopyFoldAndWire(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	native := pb.Type{
		Id:                       int32(types.T_varchar),
		Charset:                  uint32(types.CharsetUTF8MB40900AI),
		CollationCoercibility:    4,
		CollationCoercibilitySet: true,
	}
	literal := makePlan2StringConstExprWithType("Alpha")
	literal.Typ = native
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "lower", []*pb.Expr{literal})
	require.NoError(t, err)
	require.Equal(t, native.Charset, expr.Typ.Charset)
	require.Equal(t, uint32(4), expr.Typ.CollationCoercibility)
	require.True(t, expr.Typ.CollationCoercibilitySet)

	cloned := DeepCopyExpr(expr)
	require.Equal(t, expr.Typ.CollationCoercibility, cloned.Typ.CollationCoercibility)
	require.Equal(t, expr.Typ.CollationCoercibilitySet, cloned.Typ.CollationCoercibilitySet)

	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(expr), proc, false, true)
	require.NoError(t, err)
	require.Equal(t, native.Charset, folded.Typ.Charset)
	require.Equal(t, uint32(4), folded.Typ.CollationCoercibility)
	require.True(t, folded.Typ.CollationCoercibilitySet)

	wire, err := expr.Marshal()
	require.NoError(t, err)
	var restored pb.Expr
	require.NoError(t, restored.Unmarshal(wire))
	require.Equal(t, expr.Typ.CollationCoercibility, restored.Typ.CollationCoercibility)
	require.Equal(t, expr.Typ.CollationCoercibilitySet, restored.Typ.CollationCoercibilitySet)
}

func TestEqualRankNonBinaryCollationsAreRejectedForComparison(t *testing.T) {
	left := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0, Name: "left_name"}},
	}
	right := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 1, Name: "right_name"}},
	}
	err := normalizeCollationCoercibilityArgs(context.Background(), "=", []*pb.Expr{left, right})
	require.ErrorContains(t, err, "illegal mix of collations")
}

func TestConflictingExplicitCollationsAreRejected(t *testing.T) {
	bind := func(sql string) *pb.Expr {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		selectStmt := stmt.(*tree.Select)
		binder := NewDefaultBinder(context.Background(), nil, nil, pb.Type{}, nil)
		expr, err := binder.BindExpr(selectStmt.Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
		require.NoError(t, err)
		return expr
	}
	left := bind("select 'a' collate utf8mb4_0900_ai_ci")
	right := bind("select 'a' collate utf8mb4_0900_bin")
	_, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*pb.Expr{left, right})
	require.Error(t, err)
	require.Contains(t, err.Error(), "illegal mix of collations")
}

func TestExplicitCollationWinsAfterLowerPriorityConflict(t *testing.T) {
	bind := func(sql string) *pb.Expr {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		selectStmt := stmt.(*tree.Select)
		binder := NewDefaultBinder(context.Background(), nil, nil, pb.Type{}, nil)
		expr, err := binder.BindExpr(selectStmt.Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
		require.NoError(t, err)
		return expr
	}
	legacyColumn := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 1, ColPos: 0, Name: "left_name"}},
	}
	nativeColumn := &pb.Expr{
		Typ:  pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 2, ColPos: 0, Name: "right_name"}},
	}
	explicit := bind("select 'a' collate utf8mb4_0900_ai_ci")
	// The first two operands have equal, weaker coercibility but different
	// collations. The later explicit operand must win before that tie is merged.
	args := []*pb.Expr{legacyColumn, nativeColumn, explicit}
	err := normalizeCollationCoercibilityArgs(context.Background(), "=", args)
	require.NoError(t, err)
	require.Equal(t, uint32(types.CharsetUTF8MB40900AI), args[0].Typ.Charset)
	require.Equal(t, uint32(types.CharsetUTF8), legacyColumn.Typ.Charset,
		"normalization must not mutate a shared operand")
}

func TestDerivedStringFunctionUsesItsDeclaredSource(t *testing.T) {
	native := func(name string, pos int32) *pb.Expr {
		return &pb.Expr{
			Typ: pb.Type{
				Id:      int32(types.T_varchar),
				Charset: uint32(types.CharsetUTF8MB40900AI),
			},
			Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos, Name: name}},
		}
	}
	legacy := func(name string, pos int32) *pb.Expr {
		return &pb.Expr{
			Typ:  pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)},
			Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: pos, Name: name}},
		}
	}

	// TRIM's value being trimmed is its third argument. The first two
	// arguments describe the trim operation and must not determine the result
	// collation. This catches the old "first string child" shortcut.
	trim := &pb.Expr{
		Typ: pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "trim"},
			Args: []*pb.Expr{legacy("direction", 0), legacy("remove", 1), native("value", 2)},
		}},
	}
	candidate, ok, err := collationCandidateForExpr(trim)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint8(types.CharsetUTF8MB40900AI), candidate.charset)

	// SUBSTRING and LOWER preserve their source identity even though they
	// return a newly allocated VARCHAR value.
	substring := &pb.Expr{
		Typ: pb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)},
		Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "substring"},
			Args: []*pb.Expr{native("value", 0), makePlan2Int64ConstExprWithType(1)},
		}},
	}
	candidate, ok, err = collationCandidateForExpr(substring)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint8(types.CharsetUTF8MB40900AI), candidate.charset)
}

func TestVersionedGeneralAndBinaryIdentitiesAreExplicit(t *testing.T) {
	legacyGeneral := types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetUTF8)
	versionedGeneral := types.NewWithCharsetVersion(types.T_varchar, 16, 0, types.CharsetUTF8, types.CollationVersionV1)
	legacyBin := types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetUTF8MB4Bin)
	versionedBin := types.NewWithCharsetVersion(types.T_varchar, 16, 0, types.CharsetUTF8MB4Bin, types.CollationVersionV1)

	require.False(t, types.NeedsCollationKey(legacyGeneral, types.PADSpaceKeyV1))
	require.True(t, types.NeedsCollationKey(versionedGeneral, types.PADSpaceKeyV1))
	require.False(t, types.NeedsCollationKey(legacyBin, types.PADSpaceKeyV1))
	require.True(t, types.NeedsCollationKey(versionedBin, types.PADSpaceKeyV1))
	require.Equal(t, 0, types.CompareStringValues(versionedGeneral, []byte("Alpha"), []byte("alpha")))
	require.NotEqual(t, 0, types.CompareStringValues(legacyGeneral, []byte("Alpha"), []byte("alpha")))
}
