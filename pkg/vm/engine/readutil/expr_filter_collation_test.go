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

package readutil

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestCompileFilterExprNative0900VisibleColumnFailsOpen(t *testing.T) {
	tableDef := &plan.TableDef{
		Name:          "native_filter",
		Name2ColIndex: map[string]int32{"value": 0},
		Cols: []*plan.ColDef{{
			Name: "value",
			Typ: plan.Type{
				Id:      int32(types.T_varchar),
				Charset: uint32(types.CharsetUTF8MB40900AI),
			},
		}},
	}
	column := MakeColExprForTest(0, types.T_varchar, "value")
	column.Typ.Charset = uint32(types.CharsetUTF8MB40900AI)
	constant := plan2.MakePlan2StringConstExprWithType("alpha")
	for _, name := range []string{"=", "<", ">", "between"} {
		args := []*plan.Expr{column, constant}
		if name == "between" {
			args = append(args, plan2.MakePlan2StringConstExprWithType("omega"))
		}
		expr := MakeFunctionExprForTest(name, args)
		_, _, _, _, _, canCompile, _ := CompileFilterExpr(expr, tableDef, nil)
		require.False(t, canCompile, "raw %s pruning must fail open for native 0900", name)
	}
}
