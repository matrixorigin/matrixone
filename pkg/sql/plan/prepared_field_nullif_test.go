// Copyright 2021 - 2026 Matrix Origin
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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldSparseReturnMarker(t *testing.T) {
	for _, query := range []string{"select ?,field(nullif(?,1),?)", "select ?,?,field(nullif(?,cast(null as signed)),?)", "select ?,?,?,?,?,?,?,field(nullif(?,1),?)"} {
		t.Run(query, func(t *testing.T) {
			prefix := strings.Count(query, "?") - 2
			var values []any
			var bindings []PreparedSourceBinding
			for pos := 0; pos < prefix; pos++ {
				values = append(values, ParamValue{Value: int64(0), SourceType: types.T_int64.ToType(), HasSourceType: true})
				bindings = append(bindings, PreparedSourceBinding{Position: int32(pos), Type: types.T_int64.ToType()})
			}
			text := types.T_varchar.ToType()
			values = append(values, ParamValue{Value: "A", SourceType: text, HasSourceType: true}, ParamValue{Value: "a", SourceType: text, HasSourceType: true})
			bindings = append(bindings, PreparedSourceBinding{Position: int32(prefix), Type: text}, PreparedSourceBinding{Position: int32(prefix + 1), Type: text})
			for _, mode := range []string{"cached", "source bound", "execution plan"} {
				t.Run(mode, func(t *testing.T) {
					mock := NewMockOptimizer(false, newPlanTestProcess(t))
					var template *Plan
					switch mode {
					case "execution plan":
						stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, query, 1)
						require.NoError(t, err)
						t.Cleanup(stmt.Free)
						bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
						require.NoError(t, err)
						template = bound.Plan
					case "source bound":
						mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), bindings, values))
						built, err := runOneStmt(mock, t, query)
						require.NoError(t, err)
						template = built
					default:
						built, err := runOneStmt(mock, t, "prepare p from '"+query+"'")
						require.NoError(t, err)
						template = built.GetDcl().GetPrepare().Plan
					}
					before := template.String()
					filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), template, values)
					require.NoError(t, err)
					require.Equal(t, before, template.String())
					consumer := findPlanFunctionExpr(filled, "field")
					require.NotNil(t, consumer)
					proc := testutil.NewProcess(t)
					executor, err := colexec.NewExpressionExecutor(proc, consumer)
					require.NoError(t, err)
					defer executor.Free()
					out, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					require.EqualValues(t, 1, vector.GetFixedAtWithTypeCheck[int64](out, 0), consumer.String())
				})
			}
		})
	}
}
