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
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPersistedIntegerArgumentGeneratedAndCheck(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name, sqlType, argument string
		decimal                 bool
		want                    []string
		overflow                bool
	}{
		{name: "real column", sqlType: "double", argument: "a", want: []string{"a.b", "a.b"}},
		{name: "explicit real cast", sqlType: "double", argument: "cast(a as double)", want: []string{"a", "a.b"}},
		{name: "selected source", sqlType: "double", argument: "case when a<2 then cast(a as double) else a end", want: []string{"a", "a.b"}},
		{name: "exact decimal", sqlType: "decimal(38,1)", argument: "a", decimal: true, want: []string{"a.b", "a.b.c"}},
		{name: "overflow remains error", sqlType: "decimal(38,1)", argument: "a", decimal: true, overflow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call := "substring_index('a.b.c.d','.'," + tc.argument + ")"
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, fmt.Sprintf("create table t(a %s,g varchar(64) generated always as (%s) stored,check(length(%s)>=0))", tc.sqlType, call, call), 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			original := built.GetDdl().GetCreateTable().GetTableDef()
			require.NotNil(t, original)
			wire, err := proto.Marshal(original)
			require.NoError(t, err)
			var loaded planpb.TableDef
			require.NoError(t, proto.Unmarshal(wire, &loaded))
			require.Len(t, loaded.Checks, 1)
			var generated *planpb.GeneratedCol
			for _, col := range loaded.Cols {
				if col.Name == "g" {
					generated = col.GeneratedCol
				}
			}
			require.NotNil(t, generated)

			// Restore from the original SQL independently of the serialized execution
			// expression. The catalog stores AST spelling, not formatted private CASTs.
			bindSource := func(sql string) *planpb.Expr {
				parsed, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+sql, 1)
				require.NoError(t, err)
				defer parsed.Free()
				ast := parsed.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
				typ := types.T_float64.ToType()
				if tc.decimal {
					typ = types.New(types.T_decimal128, 38, 1)
				}
				result, err := NewGeneratedColBinder(proc.Ctx, []string{"a"}, []planpb.Type{makePlan2Type(&typ)}).BindExpr(ast, 0, false)
				require.NoError(t, err)
				return result
			}
			sqlGenerated := bindSource(generated.OriginString)
			sqlCheck := bindSource(loaded.Checks[0].OriginSql)
			input := batch.NewWithSize(1)
			defer input.Clean(proc.Mp())
			if tc.decimal {
				input.Vecs[0] = vector.NewVec(types.New(types.T_decimal128, 38, 1))
				values := []string{"1.5", "2.5"}
				if tc.overflow {
					values = []string{"9223372036854775808.0"}
				}
				for _, s := range values {
					value, err := types.ParseDecimal128(s, 38, 1)
					require.NoError(t, err)
					require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
				}
				input.SetRowCount(len(values))
			} else {
				input.Vecs[0] = vector.NewVec(types.T_float64.ToType())
				require.NoError(t, vector.AppendFixedList(input.Vecs[0], []float64{1.5, 2.5}, nil, proc.Mp()))
				input.SetRowCount(2)
			}
			for _, candidate := range []struct {
				name  string
				expr  *planpb.Expr
				check bool
			}{
				{"protobuf generated", generated.Expr, false}, {"SQL generated", sqlGenerated, false},
				{"protobuf check", loaded.Checks[0].Check, true}, {"SQL check", sqlCheck, true},
			} {
				t.Run(candidate.name, func(t *testing.T) {
					executor, err := colexec.NewExpressionExecutor(proc, candidate.expr)
					require.NoError(t, err)
					defer executor.Free()
					result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
					if tc.overflow {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					for i, want := range tc.want {
						if candidate.check {
							require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, i))
						} else {
							require.Equal(t, want, result.GetStringAt(i))
						}
					}
				})
			}
		})
	}
}

func TestPersistedIntegerArgumentDefaultOrigin(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	proc := testutil.NewProcess(t)
	for _, tc := range []struct{ source, want string }{
		{"1.5e0", "a.b"}, {"cast(1.5 as double)", "a"}, {"2.5", "a.b.c"},
	} {
		t.Run(tc.source, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table t(a varchar(64) default (substring_index('a.b.c.d','.',"+tc.source+")))", 1)
			require.NoError(t, err)
			defer stmt.Free()
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			var def *planpb.Default
			for _, col := range built.GetDdl().GetCreateTable().GetTableDef().Cols {
				if col.Name == "a" {
					def = col.Default
				}
			}
			require.NotNil(t, def)
			wire, err := proto.Marshal(def)
			require.NoError(t, err)
			var restored planpb.Default
			require.NoError(t, proto.Unmarshal(wire, &restored))
			stmt2, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+restored.OriginString, 1)
			require.NoError(t, err)
			defer stmt2.Free()
			ast := stmt2.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			rebound, err := NewDefaultBinder(proc.Ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			for _, expr := range []*planpb.Expr{restored.Expr, rebound} {
				func() {
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					defer free()
					require.Equal(t, tc.want, result.GetStringAt(0))
				}()
			}
		})
	}
}
