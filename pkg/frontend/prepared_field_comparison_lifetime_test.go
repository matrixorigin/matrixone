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

package frontend

import (
	"maps"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldFixedBinaryNumericComparisonLifetime(t *testing.T) {
	type step struct {
		typ                types.T
		subject, candidate any
		want               int64
	}
	for _, numericFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "numeric-first", false: "text-first"}[numericFirst], func(t *testing.T) {
			ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 1, "select field(coalesce(?,cast('A' as binary)),?)")
			defer func() { cw.proc.SetPrepareParams(nil); prepared.Close(); require.Nil(t, prepared.fieldCaseDomains) }()
			execCtx.input.isBinaryProtExecute = false
			args := []*plan.Expr{{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "subject"}}}, {Expr: &plan.Expr_V{V: &plan.VarRef{Name: "candidate"}}}}
			steps := []step{
				{types.T_int64, int64(42), int64(42), 1},
				{types.T_varchar, "A", "a", 1},
				{types.T_varbinary, "A", "a", 1},
				{types.T_varchar, "A", "a", 1},
			}
			if !numericFirst {
				steps = steps[1:3]
				for i := range steps {
					steps[i].want = 0
				}
				steps = append(steps, step{types.T_int64, int64(42), int64(42), 1}, step{types.T_varchar, "A", "a", 1})
			}
			template := prepared.PreparePlan.String()
			for _, step := range steps {
				func() {
					typ := plan.Type{Id: int32(step.typ)}
					if step.typ == types.T_varbinary {
						typ.Charset = uint32(types.CharsetBinary)
					}
					if step.typ == types.T_varchar {
						typ.Charset = uint32(types.CharsetUTF8)
					}
					require.NoError(t, ses.setUserDefinedVarWithType("subject", step.subject, "", false, typ))
					require.NoError(t, ses.setUserDefinedVarWithType("candidate", step.candidate, "", false, typ))
					_, runtime, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, &plan.Execute{Name: prepared.Name, Args: args}, "")
					require.NoError(t, err)
					if owned && stmt != nil {
						defer stmt.Free()
					}
					query := runtime.GetQuery()
					project := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
					executor, err := colexec.NewExpressionExecutor(cw.proc, project)
					require.NoError(t, err)
					defer executor.Free()
					result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					require.Equal(t, step.want, vector.GetFixedAtWithTypeCheck[int64](result, 0), project.String())
					require.Equal(t, template, prepared.PreparePlan.String())
					if cw.runtimeCachePlan != nil {
						prepared.installRuntimeSpecializationCache(cw.runtimeCacheKey, cw.runtimeCachePlan, nil, cw.runtimeCacheDiagnostics, cw.runtimeCacheColDefData)
						cw.discardRuntimeCacheCandidate()
					}
				}()
			}
			require.Len(t, prepared.fieldCaseDomains, 1)
			require.Contains(t, prepared.fieldCaseDomains, int32(-1), "comparison is owned by FIELD, not a returned marker")
			published, revision := maps.Clone(prepared.fieldCaseDomains), prepared.fieldCaseRevision
			retry := cw.preparedExecutionRetry()
			require.Equal(t, published, retry.fieldDomains)
			retryPlan, err := buildPlanForCompileRetry(execCtx.reqCtx, ses, ses.GetTxnCompileCtx(), prepared.PrepareStmt, false, retry)
			require.NoError(t, err)
			query := retryPlan.GetQuery()
			project := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(cw.proc, project)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, int64(1), vector.GetFixedAtWithTypeCheck[int64](result, 0), "retry retains the numeric FIELD consumer contract")
			require.Equal(t, published, prepared.fieldCaseDomains)
			_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, &plan.Execute{Name: prepared.Name, Args: args[:1]}, "")
			require.Error(t, err)
			require.Equal(t, published, prepared.fieldCaseDomains)
			require.Equal(t, revision, prepared.fieldCaseRevision)
		})
	}
}
