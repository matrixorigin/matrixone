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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestPreparedCommonValueAggregatesPeerDomains(t *testing.T) {
	for _, expression := range []string{
		"greatest(cast(1 as decimal(65,30)),coalesce(?,?),cast(1 as decimal(38,0)))",
		"greatest(cast(1 as decimal(38,0)),coalesce(?,?),cast(1 as decimal(65,30)))",
		"greatest(cast(1 as decimal(65,30)),?,?,cast(1 as decimal(38,0)))",
		"greatest(cast(1 as decimal(38,0)),?,?,cast(1 as decimal(65,30)))",
	} {
		for _, binary := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/binary=%t", expression, binary), func(t *testing.T) {
				mock := NewMockOptimizer(false)
				proc := mock.ctxt.GetProcess()
				params := vector.NewVec(types.T_text.ToType())
				defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
				for i := 0; i < 2; i++ {
					require.NoError(t, vector.AppendBytes(params, []byte("1"), false, proc.Mp()))
				}
				proc.SetPrepareParams(params)
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select "+expression, 1)
				require.NoError(t, err)
				defer stmt.Free()
				bindings := []PreparedSourceBinding{
					{Position: 0, Type: types.T_varchar.ToType()},
					{Position: 1, Type: types.T_varchar.ToType()},
				}
				for _, value := range []string{strings.Repeat("9", 65), strings.Repeat("9", 47), strings.Repeat("9", 46), "1", strings.Repeat("9", 65), "1"} {
					values := make([]any, 2)
					for i := range values {
						require.NoError(t, vector.SetStringAt(params, i, value, proc.Mp()))
						values[i] = ParamValue{Value: value, SourceType: types.T_varchar.ToType(),
							HasSourceType: true, EnableNumericPrefix: true, IsBinaryProtocol: binary}
					}
					bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
					if len(value) > 46 {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "all fixed scales must constrain the marker: %v", err)
						continue
					}
					require.NoError(t, err)
					q := bound.Plan.GetQuery()
					expr := q.Nodes[q.Steps[0]].ProjectList[0]
					require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
					require.Equal(t, int32(max(68, len(value)+30)), expr.Typ.Width)
					require.Equal(t, int32(30), expr.Typ.Scale)
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					if free != nil {
						defer free()
					}
					require.NoError(t, err)
					require.Equal(t, value+"."+strings.Repeat("0", 30), vector.GetFixedAtWithTypeCheck[types.Decimal256](result, 0).Format(30))
				}
			})
		}
	}
}

func TestIssue26879PreparedExecutionCommonValue(t *testing.T) {
	for _, domain := range []struct {
		name, prefix string
		width, scale int
	}{
		{"decimal128", "9007199254740992.000000000", 38, 10},
		{"decimal64", "9007199254740992.0", 18, 2},
	} {
		for _, tc := range []struct {
			name, expression string
			want             [3]bool
		}{
			{"marker-only child", "greatest(@peer@,coalesce(?,?))", [3]bool{false, true, true}},
			{"marker-only child reversed", "greatest(coalesce(?,?),@peer@)", [3]bool{false, true, true}},
			{"marker-only least", "least(@peer@,coalesce(?,?))", [3]bool{true, true, false}},
			{"deep marker-only child", "greatest(@peer@,coalesce(?,coalesce(?,?)))", [3]bool{false, true, true}},
			{"marker-only ifnull", "greatest(@peer@,ifnull(?,?))", [3]bool{false, true, true}},
			{"real string peer", "greatest(@peer@,coalesce(?,'0'))", [3]bool{true, true, true}},
			{"explicit string boundary", "greatest(@peer@,cast(coalesce(?,?) as char))", [3]bool{true, true, true}},
			{"explicit float boundary", "greatest(@peer@,cast(coalesce(?,?) as double))", [3]bool{true, true, true}},
			{"standalone unresolved", "coalesce(?,?)", [3]bool{true, true, true}},
			{"marker-derived peer", "greatest(abs(?),coalesce(?,?))", [3]bool{false, true, false}},
			{"fixed before marker-derived peer", "greatest(@peer@,abs(?),coalesce(?,?))", [3]bool{false, true, true}},
			{"ifnull child", "greatest(?,ifnull(?,@peer@))", [3]bool{false, true, false}},
			{"null-first peer", "coalesce(?,NULL,@peer@)", [3]bool{true, true, true}},
			{"null-last peer", "coalesce(?,@peer@,NULL)", [3]bool{false, true, false}},
			{"typed-null peer", "coalesce(?,cast(NULL as decimal(38,10)),@peer@)", [3]bool{false, true, false}},
		} {
			for _, binary := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/binary=%t", domain.name, tc.name, binary), func(t *testing.T) {
					for row := 0; row < 3; row++ {
						mock := NewMockOptimizer(false)
						proc := mock.ctxt.GetProcess()
						params := vector.NewVec(types.T_text.ToType())
						defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
						count := strings.Count(tc.expression, "?")
						bindings := make([]PreparedSourceBinding, count)
						values := make([]any, count)
						for i := range bindings {
							bindings[i] = PreparedSourceBinding{Position: int32(i), Type: types.T_varchar.ToType()}
							values[i] = ParamValue{Value: domain.prefix + "2", SourceType: types.T_varchar.ToType(),
								HasSourceType: true, EnableNumericPrefix: true, IsBinaryProtocol: binary}
							require.NoError(t, vector.AppendBytes(params, []byte(domain.prefix+"2"), false, proc.Mp()))
						}
						proc.SetPrepareParams(params)
						decimal := fmt.Sprintf("cast(%s%d as decimal(%d,%d))", domain.prefix, row+1, domain.width, domain.scale)
						sql := "select " + strings.ReplaceAll(tc.expression, "@peer@", decimal) + "=" + decimal
						stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
						require.NoError(t, err)
						defer stmt.Free()
						bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
						require.NoError(t, err)
						if tc.name == "marker-derived peer" {
							require.True(t, bound.ValueDependent, "runtime-derived DECIMAL must not enter the type-only cache")
						}
						q := bound.Plan.GetQuery()
						expr := q.Nodes[q.Steps[0]].ProjectList[0]
						result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
						if free != nil {
							defer free()
						}
						require.NoError(t, err)
						require.False(t, result.IsNull(0))
						require.Equal(t, tc.want[row], vector.GetFixedAtWithTypeCheck[bool](result, 0), "row %d: %s", row+1, expr.String())
					}
				})
			}
		}
	}
}
