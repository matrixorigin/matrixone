// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalWarningFreeBinding(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, name := range []string{"date", "date_format"} {
			for _, input := range []string{"2024-02-29", "bad", "0000-00-00"} {
				t.Run(fmt.Sprintf("%s/%s/prepare=%t", name, input, prepared), func(t *testing.T) {
					proc := testutil.NewProcess(t)
					defer proc.Free()
					sink := &floatCastWarningSink{}
					proc.WarningSink = sink
					ctx := NewMockCompilerContext(true, proc)
					ctx.SetSqlModeOverride("")
					query := "select " + name + "('" + input + "'"
					if name == "date_format" {
						query += ",'%Y-%m-%d'"
					}
					stmt, err := mysql.ParseOne(proc.Ctx, query+")", 1)
					require.NoError(t, err)
					defer stmt.Free()
					ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
					binder := NewProjectionBinder(NewQueryBuilder(planpb.Query_SELECT, ctx, prepared, false), NewBindContext(nil, nil), nil)
					expr, err := binder.BindExpr(ast, 0, true)
					require.NoError(t, err)
					require.Empty(t, sink.codes, "binding must not publish speculative warnings")
					if !prepared && input == "2024-02-29" {
						require.NotNil(t, expr.GetLit(), "safe temporal constants must reach planning consumers as literals")
						require.False(t, expr.GetLit().Isnull)
					} else if prepared || input == "bad" {
						require.NotNil(t, expr.GetF(), "retain runtime diagnostics and EXECUTE-time SQL mode")
					}
				})
			}
		}
	}
}
