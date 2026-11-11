// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Bind real column expressions, not handcrafted IF/cast nodes: all input rows
// fit their declarations, but CONCAT's selected value exceeds VARBINARY's limit.
func TestConditionalBinaryCoercionPreservesSelectedBytes(t *testing.T) {
	long := strings.Repeat("😀", 20000)
	for _, mode := range []string{"STRICT_TRANS_TABLES", ""} {
		t.Run("sql_mode="+mode, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			t.Cleanup(proc.Free)
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			t.Cleanup(func() {
				if hadPrevious {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
				}
			})
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return mode, nil })
			warnings := &process.WarningProbe{}
			proc.WarningSink = warnings
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, proc), false, true)
			ctx := NewBindContext(builder, nil)
			flagType := types.T_bool.ToType().PlanType()
			textType := types.New(types.T_varchar, 10000, 0).PlanType()
			binaryType := types.New(types.T_binary, 1, 0).PlanType()
			binding := NewBinding(0, 0, "", "conditional_input", 0, []string{"flag", "c", "b"},
				[]bool{false, false, false}, []*planpb.Type{&flagType, &textType, &binaryType}, false, []string{"", "", ""})
			ctx.bindings = append(ctx.bindings, binding)
			ctx.bindingByTable[binding.table] = binding
			ctx.bindingByTag[binding.tag] = binding
			for _, col := range binding.cols {
				ctx.bindingByCol[col] = binding
			}
			binder := NewProjectionBinder(builder, ctx, nil)
			ctx.binder = binder
			input := batch.NewWithSize(3)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
			input.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 10000, 0))
			input.Vecs[2] = vector.NewVec(types.New(types.T_binary, 1, 0))
			for row := 0; row < 4; row++ {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], row == 0 || row == 3, row == 2, proc.Mp()))
				require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("😀", 10000)), row == 3, proc.Mp()))
				require.NoError(t, vector.AppendBytes(input.Vecs[2], []byte("A"), false, proc.Mp()))
			}
			input.SetRowCount(4)
			for _, tc := range []struct {
				sql  string
				want []string
				null int
			}{
				{"concat(c,c)", []string{long, long, long, ""}, 3},
				{"if(flag,concat(c,c),b)", []string{long, "A", "A", ""}, 3},
				{"case when flag then concat(c,c) else b end", []string{long, "A", "A", ""}, 3},
				{"if(flag,b,concat(c,c))", []string{"A", long, long, "A"}, -1},
			} {
				t.Run(tc.sql, func(t *testing.T) {
					stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select "+tc.sql, 1)
					require.NoError(t, err)
					defer stmt.Free()
					expr, err := binder.BindExpr(stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
					require.NoError(t, err)
					value, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{input})
					if free != nil {
						defer free()
					}
					require.NoError(t, err)
					for row, want := range tc.want {
						require.Equal(t, row == tc.null, value.IsNull(uint64(row)))
						if row != tc.null {
							require.Equal(t, []byte(want), value.GetBytesAt(row))
						}
					}
					require.False(t, warnings.Warned(), "expression coercion must not emit assignment truncation warnings")
					if tc.sql == "concat(c,c)" {
						return
					}
					// The expression stays lossless; the actual destination still
					// rejects in strict mode and truncates/warns in non-strict/IGNORE.
					for _, ignore := range []bool{false, true} {
						func() {
							assigned, err := builder.forceAssignmentCastExpr(DeepCopyExpr(expr), types.New(types.T_varbinary, types.MaxVarBinaryLen, 0).PlanType(), ignore)
							require.NoError(t, err)
							probe := &process.WarningProbe{}
							proc.WarningSink = probe
							defer func() { proc.WarningSink = warnings }()
							out, release, err := colexec.GetReadonlyResultFromExpression(proc, assigned, []*batch.Batch{input})
							if release != nil {
								defer release()
							}
							if mode != "" && !ignore {
								require.True(t, moerr.IsMoErrCode(err, moerr.ErrCastWidthExceeded), "%v", err)
								return
							}
							require.NoError(t, err)
							require.True(t, probe.Warned())
							for row, want := range tc.want {
								if row != tc.null {
									require.Equal(t, []byte(want[:min(len(want), 65535)]), out.GetBytesAt(row))
								}
							}
						}()
					}
				})
			}
		})
	}
}
