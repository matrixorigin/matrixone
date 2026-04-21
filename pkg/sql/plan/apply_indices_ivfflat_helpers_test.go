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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeIvfHelperColExpr(relPos, colPos int32, tableDef *TableDef) *Expr {
	colDef := tableDef.Cols[colPos]
	return &Expr{
		Typ: colDef.Typ,
		Expr: &planpb.Expr_Col{
			Col: &ColRef{
				RelPos: relPos,
				ColPos: colPos,
				Name:   colDef.Name,
			},
		},
	}
}

func makeIvfHelperFnExpr(name string, typ Type, args ...*Expr) *Expr {
	return &Expr{
		Typ: typ,
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &ObjectRef{ObjName: name},
				Args: args,
			},
		},
	}
}

func TestIvfIncludeHelperColumnCollection(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	childTag := int32(777)

	childNode := &Node{
		BindingTags: []int32{childTag},
		ProjectList: []*Expr{
			makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
			makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		},
	}
	projNode := &Node{
		ProjectList: []*Expr{
			makeIvfHelperColExpr(childTag, 0, scanNode.TableDef),
			makeIvfHelperColExpr(childTag, 1, scanNode.TableDef),
		},
	}
	orderExpr := makeIvfHelperColExpr(childTag, 1, scanNode.TableDef)
	scanNode.FilterList = []*Expr{
		makeIvfHelperFnExpr(
			"=",
			Type{Id: int32(types.T_bool)},
			makeIvfHelperColExpr(scanTag, 4, scanNode.TableDef),
			makePlan2StringConstExprWithType("memo"),
		),
		makeIvfHelperFnExpr(
			"<=",
			Type{Id: int32(types.T_bool)},
			makeIvfHelperFnExpr(
				"l2_distance",
				Type{Id: int32(types.T_float64)},
				makeIvfHelperColExpr(scanTag, 1, scanNode.TableDef),
				&Expr{
					Typ: Type{Id: int32(types.T_array_float32)},
					Expr: &planpb.Expr_Lit{
						Lit: &planpb.Literal{Value: &planpb.Literal_VecVal{VecVal: "[1,1,1]"}},
					},
				},
			),
			MakePlan2Float64ConstExprWithType(0.5),
		),
	}

	childMap := buildIvfChildProjectionMap(childNode)
	require.Len(t, childMap, 2)
	require.NotSame(t, childNode.ProjectList[0], childMap[[2]int32{childTag, 0}])
	require.Nil(t, buildIvfChildProjectionMap(nil))

	required := collectRequiredColumns(projNode, childNode, scanNode, orderExpr, 1)
	assert.Contains(t, required, "title")
	assert.Contains(t, required, "category")
	assert.Contains(t, required, "note")
	assert.NotContains(t, required, "embedding")

	projected := collectProjectedColumns(projNode, childNode, scanNode, 1)
	assert.Equal(t, map[string]struct{}{
		"title":    {},
		"category": {},
	}, projected)
}

func TestIvfIncludeHelperFilterCoverageAndSerialization(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	covered := map[string]struct{}{
		"id":       {},
		"title":    {},
		"category": {},
	}

	categoryEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(20),
	)
	noteEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 4, scanNode.TableDef),
		makePlan2StringConstExprWithType("memo"),
	)
	distFilter := makeIvfHelperFnExpr(
		"<",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperFnExpr(
			"l2_distance",
			Type{Id: int32(types.T_float64)},
			makeIvfHelperColExpr(scanTag, 1, scanNode.TableDef),
			&Expr{
				Typ: Type{Id: int32(types.T_array_float32)},
				Expr: &planpb.Expr_Lit{
					Lit: &planpb.Literal{Value: &planpb.Literal_VecVal{VecVal: "[1,1,1]"}},
				},
			},
		),
		MakePlan2Float64ConstExprWithType(0.5),
	)

	assert.True(t, exprRefsOnlyCoveredColumns(categoryEq, scanTag, 1, scanNode.TableDef, covered))
	assert.False(t, exprRefsOnlyCoveredColumns(noteEq, scanTag, 1, scanNode.TableDef, covered))
	assert.False(t, exprRefsOnlyCoveredColumns(distFilter, scanTag, 1, scanNode.TableDef, covered))
	assert.True(t, exprRefsOnlyCoveredColumns(&Expr{Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}, scanTag, 1, scanNode.TableDef, covered))
	assert.True(t, exprRefsOnlyCoveredColumns(
		&Expr{
			Expr: &planpb.Expr_List{
				List: &planpb.ExprList{List: []*Expr{MakePlan2Int32ConstExprWithType(1), MakePlan2Int32ConstExprWithType(2)}},
			},
		},
		scanTag, 1, scanNode.TableDef, covered,
	))
	assert.False(t, exprRefsOnlyCoveredColumns(
		&Expr{
			Typ: scanNode.TableDef.Cols[0].Typ,
			Expr: &planpb.Expr_Col{
				Col: &ColRef{RelPos: scanTag, ColPos: 99, Name: "unknown"},
			},
		},
		scanTag, 1, scanNode.TableDef, covered,
	))

	pushdown, remaining := splitFiltersByIncludeColumns([]*Expr{categoryEq, noteEq, distFilter}, scanNode, []string{"title", "category"}, 1)
	require.Len(t, pushdown, 1)
	require.Len(t, remaining, 2)
	assert.Same(t, categoryEq, pushdown[0])

	betweenFilter := makeIvfHelperFnExpr(
		"between",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(10),
		MakePlan2Int32ConstExprWithType(30),
	)
	inFilter := makeIvfHelperFnExpr(
		"in",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(10),
		MakePlan2Int32ConstExprWithType(20),
	)
	isNotNullFilter := makeIvfHelperFnExpr(
		"is_not_null",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
	)
	bitAndFilter := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperFnExpr(
			"&",
			Type{Id: int32(types.T_int32)},
			makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
			MakePlan2Int32ConstExprWithType(1),
		),
		MakePlan2Int32ConstExprWithType(0),
	)
	unaryFilter := makeIvfHelperFnExpr(
		"<",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperFnExpr(
			"unary_minus",
			Type{Id: int32(types.T_int32)},
			makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		),
		&Expr{
			Typ: Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: -5}},
			},
		},
	)
	caseFilter := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperFnExpr(
			"case",
			Type{Id: int32(types.T_varchar)},
			categoryEq,
			makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
			makePlan2StringConstExprWithType("fallback"),
		),
		makePlan2StringConstExprWithType("beta"),
	)
	defaultFnFilter := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperFnExpr(
			"lower",
			Type{Id: int32(types.T_varchar)},
			makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
		),
		makePlan2StringConstExprWithType("beta"),
	)

	sql, serializedPushdown, serializedRemaining, err := serializeFiltersToSQL(
		[]*Expr{betweenFilter, inFilter, isNotNullFilter, bitAndFilter, unaryFilter, caseFilter, defaultFnFilter},
		scanNode,
		[]string{"title", "category"},
		1,
	)
	require.NoError(t, err)
	require.Len(t, serializedPushdown, 7)
	require.Empty(t, serializedRemaining)
	assert.Contains(t, sql, "between 10 and 30")
	assert.Contains(t, sql, "in (10, 20)")
	assert.Contains(t, sql, "is not null")
	assert.Contains(t, sql, "& 1")
	assert.Contains(t, sql, "-`"+catalog.SystemSI_IVFFLAT_IncludeColPrefix+"category`")
	assert.Contains(t, sql, "case when")
	assert.Contains(t, sql, "lower(")
}

func TestIvfOperatorAndTypeMappingHelpers(t *testing.T) {
	for _, fnName := range []string{"=", "!=", "<>", "<", "<=", ">", ">=", "like", "not_like", "ilike", "not_ilike", "reg_match", "not_reg_match", "<=>"} {
		_, ok := ivfComparisonOpForFunc(fnName)
		assert.True(t, ok, fnName)
	}
	_, ok := ivfComparisonOpForFunc("bad_cmp")
	assert.False(t, ok)

	for _, fnName := range []string{"+", "-", "*", "/", "div", "%", "&", "|", "^", "<<", ">>"} {
		_, ok := ivfBinaryOpForFunc(fnName)
		assert.True(t, ok, fnName)
	}
	_, ok = ivfBinaryOpForFunc("bad_bin")
	assert.False(t, ok)

	for _, fnName := range []string{"unary_minus", "unary_plus", "unary_tilde", "unary_mark"} {
		_, ok := ivfUnaryOpForFunc(fnName)
		assert.True(t, ok, fnName)
	}
	_, ok = ivfUnaryOpForFunc("bad_unary")
	assert.False(t, ok)

	assert.True(t, ivfCanFormatAsFunctionName("lower"))
	assert.True(t, ivfCanFormatAsFunctionName("fn_1"))
	assert.False(t, ivfCanFormatAsFunctionName(""))
	assert.False(t, ivfCanFormatAsFunctionName("1bad"))
	assert.False(t, ivfCanFormatAsFunctionName("bad-name"))

	assert.Equal(t, int32(12), ivfStringTypeDisplayWidth(12))
	assert.Equal(t, int32(-1), ivfStringTypeDisplayWidth(0))
}

func TestIvfSerializeFilterAdditionalBranches(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	covered := map[string]struct{}{
		"id":       {},
		"title":    {},
		"category": {},
	}

	categoryEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(20),
	)
	titleEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
		makePlan2StringConstExprWithType("beta"),
	)

	tests := []struct {
		name     string
		expr     *Expr
		contains string
	}{
		{
			name:     "and",
			expr:     makeIvfHelperFnExpr("and", Type{Id: int32(types.T_bool)}, categoryEq, titleEq),
			contains: " and ",
		},
		{
			name:     "or",
			expr:     makeIvfHelperFnExpr("or", Type{Id: int32(types.T_bool)}, categoryEq, titleEq),
			contains: " or ",
		},
		{
			name:     "xor",
			expr:     makeIvfHelperFnExpr("xor", Type{Id: int32(types.T_bool)}, categoryEq, titleEq),
			contains: " xor ",
		},
		{
			name:     "not",
			expr:     makeIvfHelperFnExpr("not", Type{Id: int32(types.T_bool)}, categoryEq),
			contains: "not ",
		},
		{
			name: "not_in_tuple_arg",
			expr: makeIvfHelperFnExpr(
				"not_in",
				Type{Id: int32(types.T_bool)},
				makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
				&Expr{
					Expr: &planpb.Expr_List{
						List: &planpb.ExprList{
							List: []*Expr{MakePlan2Int32ConstExprWithType(10), MakePlan2Int32ConstExprWithType(20)},
						},
					},
				},
			),
			contains: "not in",
		},
		{
			name:     "is_null",
			expr:     makeIvfHelperFnExpr("is_null", Type{Id: int32(types.T_bool)}, makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef)),
			contains: "is null",
		},
		{
			name:     "is_unknown",
			expr:     makeIvfHelperFnExpr("is_unknown", Type{Id: int32(types.T_bool)}, categoryEq),
			contains: "is unknown",
		},
		{
			name:     "is_not_unknown",
			expr:     makeIvfHelperFnExpr("is_not_unknown", Type{Id: int32(types.T_bool)}, categoryEq),
			contains: "is not unknown",
		},
		{
			name:     "is_true",
			expr:     makeIvfHelperFnExpr("is_true", Type{Id: int32(types.T_bool)}, categoryEq),
			contains: "is true",
		},
		{
			name:     "is_not_true",
			expr:     makeIvfHelperFnExpr("is_not_true", Type{Id: int32(types.T_bool)}, categoryEq),
			contains: "is not true",
		},
		{
			name:     "is_false",
			expr:     makeIvfHelperFnExpr("is_false", Type{Id: int32(types.T_bool)}, categoryEq),
			contains: "is false",
		},
		{
			name:     "is_not_false",
			expr:     makeIvfHelperFnExpr("is_not_false", Type{Id: int32(types.T_bool)}, categoryEq),
			contains: "is not false",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ast, ok, err := serializeFilterExprToAST(tc.expr, scanNode, scanTag, 1, covered)
			require.NoError(t, err)
			require.True(t, ok)
			sql := tree.StringWithOpts(ast, dialect.MYSQL, tree.WithQuoteString(true))
			assert.Contains(t, sql, tc.contains)
		})
	}

	ast, ok, err := serializeFilterExprToAST(
		makeIvfHelperFnExpr("bad-name", Type{Id: int32(types.T_varchar)}, makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef)),
		scanNode, scanTag, 1, covered,
	)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, ast)

	caseAst, ok, err := serializeFilterCaseExprToAST([]*Expr{categoryEq, makePlan2StringConstExprWithType("beta")}, scanNode, scanTag, 1, covered)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, caseAst)
}

func TestIvfSerializeFilterInvalidShapes(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	covered := map[string]struct{}{
		"id":       {},
		"title":    {},
		"category": {},
	}

	ast, ok, err := serializeFilterExprToAST(&Expr{}, scanNode, scanTag, 1, covered)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, ast)

	ast, ok, err = serializeFilterExprToAST(makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef), scanNode, scanTag, 1, covered)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Contains(t, tree.StringWithOpts(ast, dialect.MYSQL, tree.WithQuoteString(true)), catalog.SystemSI_IVFFLAT_IncludeColPrefix+"title")

	ast, ok, err = serializeFilterExprToAST(makePlan2StringConstExprWithType("beta"), scanNode, scanTag, 1, covered)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Contains(t, tree.StringWithOpts(ast, dialect.MYSQL, tree.WithQuoteString(true)), "beta")

	ast, ok, err = serializeFilterExprToAST(
		&Expr{
			Expr: &planpb.Expr_List{
				List: &planpb.ExprList{List: []*Expr{MakePlan2Int32ConstExprWithType(1), MakePlan2Int32ConstExprWithType(2)}},
			},
		},
		scanNode, scanTag, 1, covered,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Contains(t, tree.StringWithOpts(ast, dialect.MYSQL, tree.WithQuoteString(true)), "(1, 2)")

	ast, ok, err = serializeFilterFuncToAST(nil, nil, scanNode, scanTag, 1, covered)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, ast)

	ast, ok, err = serializeFilterFuncToAST(
		nil,
		&planpb.Function{Func: &ObjectRef{ObjName: "and"}, Args: []*Expr{makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef)}},
		scanNode, scanTag, 1, covered,
	)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, ast)

	ast, ok, err = serializeFilterIsExprToAST(nil, scanNode, scanTag, 1, covered, func(expr tree.Expr) tree.Expr {
		return tree.NewIsNullExpr(expr)
	})
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, ast)

	ast, ok, err = serializeInListToAST([]*Expr{MakePlan2Int32ConstExprWithType(1)}, scanNode, scanTag, 1, covered)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, ast)

	invalidFns := []struct {
		name string
		fn   *planpb.Function
	}{
		{name: "not_bad_arity", fn: &planpb.Function{Func: &ObjectRef{ObjName: "not"}}},
		{name: "compare_bad_arity", fn: &planpb.Function{Func: &ObjectRef{ObjName: "="}, Args: []*Expr{makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef)}}},
		{name: "between_bad_arity", fn: &planpb.Function{Func: &ObjectRef{ObjName: "between"}, Args: []*Expr{makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef), MakePlan2Int32ConstExprWithType(1)}}},
		{name: "in_bad_arity", fn: &planpb.Function{Func: &ObjectRef{ObjName: "in"}, Args: []*Expr{makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef)}}},
		{name: "binary_bad_arity", fn: &planpb.Function{Func: &ObjectRef{ObjName: "+"}, Args: []*Expr{makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef)}}},
		{name: "unary_bad_arity", fn: &planpb.Function{Func: &ObjectRef{ObjName: "unary_minus"}}},
		{name: "cast_bad_arity", fn: &planpb.Function{Func: &ObjectRef{ObjName: "cast"}}},
	}

	for _, tc := range invalidFns {
		t.Run(tc.name, func(t *testing.T) {
			ast, ok, err := serializeFilterFuncToAST(&Expr{Typ: Type{Id: int32(types.T_int64)}}, tc.fn, scanNode, scanTag, 1, covered)
			require.NoError(t, err)
			assert.False(t, ok)
			assert.Nil(t, ast)
		})
	}

	cols := map[string]struct{}{}
	collectScanColumnsFromExpr(nil, scanTag, 1, scanNode.TableDef, cols)
	collectScanColumnsFromExpr(makeIvfHelperColExpr(scanTag+1, 2, scanNode.TableDef), scanTag, 1, scanNode.TableDef, cols)
	collectScanColumnsFromExpr(
		&Expr{Typ: Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &ColRef{RelPos: scanTag, ColPos: 99, Name: "missing"}}},
		scanTag, 1, scanNode.TableDef, cols,
	)
	collectScanColumnsFromExpr(
		&Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*Expr{
			makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
			makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		}}}},
		scanTag, 1, scanNode.TableDef, cols,
	)
	assert.Equal(t, map[string]struct{}{
		"title":    {},
		"category": {},
	}, cols)
}

func TestIvfLiteralAndPlanTypeConversionHelpers(t *testing.T) {
	literalCases := []struct {
		name     string
		lit      *planpb.Literal
		typ      Type
		contains string
	}{
		{
			name:     "i8",
			lit:      &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 8}},
			typ:      Type{Id: int32(types.T_int8)},
			contains: "8",
		},
		{
			name:     "i16",
			lit:      &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: 16}},
			typ:      Type{Id: int32(types.T_int16)},
			contains: "16",
		},
		{
			name:     "i32",
			lit:      &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 32}},
			typ:      Type{Id: int32(types.T_int32)},
			contains: "32",
		},
		{
			name:     "i64",
			lit:      &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 64}},
			typ:      Type{Id: int32(types.T_int64)},
			contains: "64",
		},
		{
			name:     "u8",
			lit:      &planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: 8}},
			typ:      Type{Id: int32(types.T_uint8)},
			contains: "8",
		},
		{
			name:     "u16",
			lit:      &planpb.Literal{Value: &planpb.Literal_U16Val{U16Val: 16}},
			typ:      Type{Id: int32(types.T_uint16)},
			contains: "16",
		},
		{
			name:     "u32",
			lit:      &planpb.Literal{Value: &planpb.Literal_U32Val{U32Val: 32}},
			typ:      Type{Id: int32(types.T_uint32)},
			contains: "32",
		},
		{
			name:     "u64",
			lit:      &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 64}},
			typ:      Type{Id: int32(types.T_uint64)},
			contains: "64",
		},
		{
			name:     "f32",
			lit:      &planpb.Literal{Value: &planpb.Literal_Fval{Fval: 1.25}},
			typ:      Type{Id: int32(types.T_float32)},
			contains: "1.25",
		},
		{
			name:     "f64",
			lit:      &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 2.5}},
			typ:      Type{Id: int32(types.T_float64)},
			contains: "2.5",
		},
		{
			name:     "bool",
			lit:      &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}},
			typ:      Type{Id: int32(types.T_bool)},
			contains: "true",
		},
		{
			name:     "date",
			lit:      &planpb.Literal{Value: &planpb.Literal_Dateval{Dateval: int32(types.Date(1))}},
			typ:      Type{Id: int32(types.T_date)},
			contains: types.Date(1).String(),
		},
		{
			name:     "time",
			lit:      &planpb.Literal{Value: &planpb.Literal_Timeval{Timeval: int64(types.Time(0))}},
			typ:      Type{Id: int32(types.T_time), Scale: 3},
			contains: types.Time(0).String2(3),
		},
		{
			name:     "datetime",
			lit:      &planpb.Literal{Value: &planpb.Literal_Datetimeval{Datetimeval: int64(types.Datetime(0))}},
			typ:      Type{Id: int32(types.T_datetime), Scale: 3},
			contains: types.Datetime(0).String2(3),
		},
		{
			name:     "timestamp",
			lit:      &planpb.Literal{Value: &planpb.Literal_Timestampval{Timestampval: int64(types.Timestamp(0))}},
			typ:      Type{Id: int32(types.T_timestamp), Scale: 3},
			contains: types.Timestamp(0).String2(time.UTC, 3),
		},
		{
			name:     "decimal64",
			lit:      &planpb.Literal{Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: 1234}}},
			typ:      Type{Id: int32(types.T_decimal64), Scale: 2},
			contains: types.Decimal64(1234).Format(2),
		},
		{
			name: "decimal128",
			lit: &planpb.Literal{Value: &planpb.Literal_Decimal128Val{
				Decimal128Val: &planpb.Decimal128{A: 1234, B: 0},
			}},
			typ:      Type{Id: int32(types.T_decimal128), Scale: 2},
			contains: (types.Decimal128{B0_63: 1234, B64_127: 0}).Format(2),
		},
		{
			name:     "json",
			lit:      &planpb.Literal{Value: &planpb.Literal_Jsonval{Jsonval: `{"k":1}`}},
			typ:      Type{Id: int32(types.T_json)},
			contains: `\"k\"`,
		},
		{
			name:     "enum",
			lit:      &planpb.Literal{Value: &planpb.Literal_EnumVal{EnumVal: 7}},
			typ:      Type{Id: int32(types.T_enum)},
			contains: "7",
		},
		{
			name:     "vec",
			lit:      &planpb.Literal{Value: &planpb.Literal_VecVal{VecVal: "[1,2,3]"}},
			typ:      Type{Id: int32(types.T_array_float32)},
			contains: "[1,2,3]",
		},
	}

	for _, tc := range literalCases {
		t.Run("literal_"+tc.name, func(t *testing.T) {
			ast, err := ivfLiteralToAST(tc.lit, tc.typ)
			require.NoError(t, err)
			sql := tree.StringWithOpts(ast, dialect.MYSQL, tree.WithQuoteString(true))
			assert.Contains(t, sql, tc.contains)
		})
	}

	typeCases := []struct {
		name     string
		typ      Type
		family   string
		unsigned bool
		binary   bool
		display  int32
		enumVals []string
	}{
		{name: "bool", typ: Type{Id: int32(types.T_bool)}, family: "bool"},
		{name: "bit", typ: Type{Id: int32(types.T_bit)}, family: "bit", display: 1},
		{name: "int8", typ: Type{Id: int32(types.T_int8)}, family: "tinyint"},
		{name: "uint8", typ: Type{Id: int32(types.T_uint8)}, family: "tinyint", unsigned: true},
		{name: "int16", typ: Type{Id: int32(types.T_int16)}, family: "smallint"},
		{name: "uint16", typ: Type{Id: int32(types.T_uint16)}, family: "smallint", unsigned: true},
		{name: "int32", typ: Type{Id: int32(types.T_int32)}, family: "int"},
		{name: "uint32", typ: Type{Id: int32(types.T_uint32)}, family: "int", unsigned: true},
		{name: "int64", typ: Type{Id: int32(types.T_int64)}, family: "bigint"},
		{name: "float32", typ: Type{Id: int32(types.T_float32)}, family: "float"},
		{name: "uint64", typ: Type{Id: int32(types.T_uint64)}, family: "bigint", unsigned: true},
		{name: "float64", typ: Type{Id: int32(types.T_float64)}, family: "double"},
		{name: "decimal64", typ: Type{Id: int32(types.T_decimal64), Width: 12, Scale: 3}, family: "decimal", display: 12},
		{name: "date", typ: Type{Id: int32(types.T_date)}, family: "date"},
		{name: "time", typ: Type{Id: int32(types.T_time), Scale: 4}, family: "time", display: 4},
		{name: "datetime", typ: Type{Id: int32(types.T_datetime), Scale: 6}, family: "datetime", display: 6},
		{name: "timestamp", typ: Type{Id: int32(types.T_timestamp), Scale: 2}, family: "timestamp", display: 2},
		{name: "char", typ: Type{Id: int32(types.T_char), Width: 8}, family: "char", display: 8},
		{name: "varchar_default_width", typ: Type{Id: int32(types.T_varchar)}, family: "varchar", display: -1},
		{name: "binary", typ: Type{Id: int32(types.T_binary), Width: 16}, family: "binary", binary: true, display: 16},
		{name: "varbinary", typ: Type{Id: int32(types.T_varbinary), Width: 16}, family: "varbinary", binary: true, display: 16},
		{name: "text", typ: Type{Id: int32(types.T_text)}, family: "text"},
		{name: "blob", typ: Type{Id: int32(types.T_blob)}, family: "blob"},
		{name: "json", typ: Type{Id: int32(types.T_json)}, family: "json"},
		{name: "uuid", typ: Type{Id: int32(types.T_uuid)}, family: "uuid"},
		{name: "enum", typ: Type{Id: int32(types.T_enum), Enumvalues: "a,b"}, family: "enum", enumVals: []string{"a", "b"}},
		{name: "vecf32", typ: Type{Id: int32(types.T_array_float32), Width: 64}, family: "vecf32", display: 64},
		{name: "vecf64", typ: Type{Id: int32(types.T_array_float64), Width: 32}, family: "vecf64", display: 32},
	}

	for _, tc := range typeCases {
		t.Run("type_"+tc.name, func(t *testing.T) {
			treeType, err := ivfPlanTypeToTreeType(tc.typ)
			require.NoError(t, err)
			require.NotNil(t, treeType)
			assert.Equal(t, tc.family, treeType.InternalType.FamilyString)
			assert.Equal(t, tc.unsigned, treeType.InternalType.Unsigned)
			assert.Equal(t, tc.binary, treeType.InternalType.Binary)
			if tc.display != 0 {
				assert.Equal(t, tc.display, treeType.InternalType.DisplayWith)
			}
			if tc.enumVals != nil {
				assert.Equal(t, tc.enumVals, treeType.InternalType.EnumValues)
			}
		})
	}

	_, err := ivfPlanTypeToTreeType(Type{Id: int32(types.T_any)})
	require.Error(t, err)
}

func TestIvfFilterColumnAndDistanceRangeHelpers(t *testing.T) {
	builder, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]

	_, err := ivfFilterColumnToAST(&ColRef{ColPos: 0}, nil)
	require.Error(t, err)
	_, err = ivfFilterColumnToAST(&ColRef{ColPos: 99}, scanNode)
	require.Error(t, err)

	pkExpr, err := ivfFilterColumnToAST(&ColRef{RelPos: scanTag, ColPos: 0, Name: "id"}, scanNode)
	require.NoError(t, err)
	assert.Contains(t, tree.StringWithOpts(pkExpr, dialect.MYSQL, tree.WithQuoteString(true)), catalog.SystemSI_IVFFLAT_TblCol_Entries_pk)

	includeExpr, err := ivfFilterColumnToAST(&ColRef{RelPos: scanTag, ColPos: 2, Name: "title"}, scanNode)
	require.NoError(t, err)
	assert.Contains(t, tree.StringWithOpts(includeExpr, dialect.MYSQL, tree.WithQuoteString(true)), catalog.SystemSI_IVFFLAT_IncludeColPrefix+"title")

	ivfCtx := &ivfIndexContext{
		partPos:         1,
		origFuncName:    "l2_distance",
		vecLitArg:       &Expr{Typ: Type{Id: int32(types.T_array_float32)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_VecVal{VecVal: "[1,1,1]"}}}},
		pushdownEnabled: false,
	}
	validVecExpr := makeIvfHelperFnExpr(
		"l2_distance",
		Type{Id: int32(types.T_float64)},
		makeIvfHelperColExpr(scanTag, 1, scanNode.TableDef),
		&Expr{Typ: Type{Id: int32(types.T_array_float32)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_VecVal{VecVal: "[1,1,1]"}}}},
	)
	invalidVecExpr := makeIvfHelperFnExpr(
		"l2_distance",
		Type{Id: int32(types.T_float64)},
		makeIvfHelperColExpr(scanTag, 1, scanNode.TableDef),
		&Expr{Typ: Type{Id: int32(types.T_array_float32)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_VecVal{VecVal: "[9,9,9]"}}}},
	)
	remaining, distRange := builder.getDistRangeFromFilters(
		[]*Expr{
			makeIvfHelperFnExpr(">=", Type{Id: int32(types.T_bool)}, validVecExpr, MakePlan2Float64ConstExprWithType(0.1)),
			makeIvfHelperFnExpr("<", Type{Id: int32(types.T_bool)}, validVecExpr, MakePlan2Float64ConstExprWithType(0.9)),
			makeIvfHelperFnExpr("<=", Type{Id: int32(types.T_bool)}, invalidVecExpr, MakePlan2Float64ConstExprWithType(0.3)),
			makeIvfHelperFnExpr("=", Type{Id: int32(types.T_bool)}, makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef), MakePlan2Int32ConstExprWithType(20)),
		},
		ivfCtx,
	)
	require.NotNil(t, distRange)
	assert.Equal(t, planpb.BoundType_INCLUSIVE, distRange.LowerBoundType)
	assert.Equal(t, planpb.BoundType_EXCLUSIVE, distRange.UpperBoundType)
	require.Len(t, remaining, 2)
	assert.Equal(t, "<=", remaining[0].GetF().Func.ObjName)
	assert.Equal(t, "=", remaining[1].GetF().Func.ObjName)
}

func TestSkipPkDedupCoverage(t *testing.T) {
	tests := []struct {
		name string
		old  *TableDef
		new  *TableDef
		want bool
	}{
		{
			name: "new table without primary key skips",
			old:  &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}},
			new:  &TableDef{Pkey: nil},
			want: true,
		},
		{
			name: "adding a new primary key requires dedup",
			old:  &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName}},
			new:  &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}},
			want: false,
		},
		{
			name: "same primary key names skip dedup",
			old:  &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}},
			new:  &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}},
			want: true,
		},
		{
			name: "changed primary key names do not skip",
			old:  &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}},
			new:  &TableDef{Pkey: &PrimaryKeyDef{PkeyColName: "headline", Names: []string{"headline"}}},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, skipPkDedup(tc.old, tc.new))
		})
	}
}

func TestOrderByColumnAllowsKnownColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	origin := makeAlterCoverageTableDef()
	copyTable := DeepCopyTableDef(origin, true)
	alterCtx := initAlterTableContext(origin, copyTable, origin.DbName)
	alterPlan := &planpb.AlterTable{
		Database:     origin.DbName,
		TableDef:     origin,
		CopyTableDef: copyTable,
	}
	spec := mustParseOrderByClause(
		t,
		mock.CurrentContext(),
		"alter table t1 order by id, title",
	)

	err := OrderByColumn(mock.CurrentContext(), alterPlan, spec, alterCtx)
	require.NoError(t, err)
}
