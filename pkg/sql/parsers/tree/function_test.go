// Copyright 2023 Matrix Origin
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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func Test_Function(t *testing.T) {
	// sql
	// 1. create function sql_sum (a int, b int) returns int language sql as '$1 + $2';
	// 2. drop function sql_sum (a int, b int);

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))

	intType := &T{
		InternalType: InternalType{
			Family:       IntFamily,
			FamilyString: "int",
			Width:        32,
			Oid:          3,
		},
	}

	// FunctionArg
	arg1 := &FunctionArgDecl{
		Name: NewUnresolvedColName("a"),
		Type: intType,
	}

	arg2 := &FunctionArgDecl{
		Name: NewUnresolvedColName("b"),
		Type: intType,
	}

	ctx.Reset()
	arg1.Format(ctx)
	require.Equal(t, "a int", ctx.String())

	ctx.Reset()
	arg1.GetName(ctx)
	require.Equal(t, "a", ctx.String())

	ctx.Reset()
	arg1.GetType(ctx)
	require.Equal(t, "int", ctx.String())

	// ReturnType
	ret := NewReturnType(intType)

	ctx.Reset()
	ret.Format(ctx)
	require.Equal(t, "int", ctx.String())

	// FunctionName
	name := NewFuncName("sql_sum", ObjectNamePrefix{})

	ctx.Reset()
	name.Format(ctx)
	require.Equal(t, "sql_sum", ctx.String())

	// CreateFunction
	create := &CreateFunction{
		Name:       name,
		Args:       []FunctionArg{arg1, arg2},
		ReturnType: ret,
		Body:       "$1 + $2",
		Language:   "sql",
	}

	ctx.Reset()
	create.Format(ctx)
	require.Equal(t, "create function sql_sum (a int, b int) returns int language sql as '$1 + $2'", ctx.String())

	// DropFunction
	drop := &DropFunction{
		Name: name,
		Args: []FunctionArg{arg1, arg2},
	}

	ctx.Reset()
	drop.Format(ctx)
	require.Equal(t, "drop function sql_sum (a int, b int)", ctx.String())

	dropIfExists := NewDropFunction(name, []FunctionArg{arg1, arg2})
	dropIfExists.IfExists = true
	require.True(t, dropIfExists.IfExists)
	ctx.Reset()
	dropIfExists.Format(ctx)
	require.Equal(t, "drop function if exists sql_sum (a int, b int)", ctx.String())
}

func TestViewDefinitionNameConstFormattingIsScoped(t *testing.T) {
	for _, tc := range []struct {
		name      string
		nameType  P_TYPE
		valueType P_TYPE
		want      string
	}{
		{"text literal", P_char, P_char, "(cast(0x615c62 as varchar)) as `c`"},
		{"binary value keeps function", P_char, P_ScoreBinary, "name_const('c', _binary 0x615c62)"},
		{"invalid name keeps function", P_null, P_char, "name_const(null, cast(0x615c62 as varchar))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := NewUnresolvedColName("name_const")
			value := NewNumVal(`a\b`, `a\b`, false, tc.valueType)
			expr := &FuncExpr{Func: FuncName2ResolvableFunctionReference(name), FuncName: NewCStr("name_const", 1),
				Exprs: Exprs{NewNumVal("c", "c", false, tc.nameType), value}}
			projection := &SelectExpr{Expr: expr}
			require.Equal(t, tc.want, StringWithOpts(projection, dialect.MYSQL, WithViewDefinition()))
			require.Contains(t, StringWithOpts(projection, dialect.MYSQL, WithSingleQuoteString(), WithModeIndependentStringLiterals()), "name_const(")
			require.Same(t, value, expr.Exprs[1], "formatting does not rewrite the source AST")
			require.Nil(t, projection.As)
		})
	}
}
