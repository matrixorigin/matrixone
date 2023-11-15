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
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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
		Name: &UnresolvedName{
			NumParts: 1,
			Star:     false,
			Parts:    NameParts([]string{"a", "", "", ""}),
		},
		Type: intType,
	}

	arg2 := &FunctionArgDecl{
		Name: &UnresolvedName{
			NumParts: 1,
			Star:     false,
			Parts:    NameParts([]string{"b", "", "", ""}),
		},
		Type: intType,
	}

	ctx.Reset()
	arg1.Format(ctx)
	fmt.Println(ctx.String())
	require.Equal(t, ctx.String(), "a int")

	ctx.Reset()
	arg1.GetName(ctx)
	fmt.Println(ctx.String())
	require.Equal(t, ctx.String(), "a")

	ctx.Reset()
	arg1.GetType(ctx)
	fmt.Println(ctx.String())
	require.Equal(t, ctx.String(), "int")

	// ReturnType
	ret := NewReturnType(intType)

	ctx.Reset()
	ret.Format(ctx)
	fmt.Println(ctx.String())
	require.Equal(t, ctx.String(), "int")

	// FunctionName
	name := NewFuncName("sql_sum", ObjectNamePrefix{})

	ctx.Reset()
	name.Format(ctx)
	fmt.Println(ctx.String())
	require.Equal(t, ctx.String(), "sql_sum")

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
	fmt.Println(ctx.String())
	require.Equal(t, ctx.String(), "create function sql_sum (a int, b int) returns int language sql as '$1 + $2'")

	// DropFunction
	drop := &DropFunction{
		Name: name,
		Args: []FunctionArg{arg1, arg2},
	}

	ctx.Reset()
	drop.Format(ctx)
	fmt.Println(ctx.String())
	require.Equal(t, ctx.String(), "drop function sql_sum (a int, b int)")
}
