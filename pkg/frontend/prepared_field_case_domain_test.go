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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestPreparedSQLFieldCaseDomainLifetime(t *testing.T) {
	for _, initial := range []types.T{types.T_varchar, types.T_varbinary} {
		t.Run(initial.String(), func(t *testing.T) {
			ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 1, "select field(case when ? then null else ? end,?)")
			defer func() {
				cw.proc.SetPrepareParams(nil)
				prepared.Close()
				require.Nil(t, prepared.fieldCaseDomains)
				require.Zero(t, prepared.fieldCaseRevision)
			}()
			before := prepared.PreparePlan.String()
			execCtx.input.isBinaryProtExecute = false
			cw.binaryPrepare = false
			args := make([]*plan.Expr, 0, 3)
			for _, name := range []string{"field_condition", "field_subject", "field_candidate"} {
				args = append(args, &plan.Expr{Expr: &plan.Expr_V{V: &plan.VarRef{Name: name}}})
			}
			// Rejected argument admission must not resolve a domain.
			_, _, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, &plan.Execute{Name: prepared.Name, Args: args[:2]}, "")
			require.Error(t, err)
			require.Nil(t, prepared.fieldCaseDomains)
			opposite := types.T_varbinary
			if initial == types.T_varbinary {
				opposite = types.T_varchar
			}
			cacheHits := 0
			for _, step := range []struct {
				source             types.T
				subject, candidate any
				rebound            bool
			}{
				{initial, "A", "a", false}, {opposite, "A", "a", false}, {initial, "A", "a", false},
				{types.T_int64, int64(42), int64(42), true},
				{types.T_varchar, "A", "a", true}, {types.T_varbinary, "A", "a", true},
				{types.T_varchar, "01", "1", true},
				{types.T_float64, 1.5, 1.5, true}, {types.T_varchar, "1.5x", "1.5", true},
				{types.T_int64, int64(42), int64(42), true}, {types.T_varchar, "1.5x", "1.5", true},
			} {
				source := step.source
				for _, condition := range []int64{0, 1, 0} {
					func() {
						require.NoError(t, ses.SetUserDefinedVar("field_condition", condition, ""))
						typ := plan.Type{Id: int32(source), Charset: uint32(types.CharsetUTF8)}
						if source == types.T_varbinary {
							typ.Charset = uint32(types.CharsetBinary)
						}
						if source.ToType().IsNumeric() {
							typ.Charset = 0
						}
						require.NoError(t, ses.setUserDefinedVarWithType("field_subject", step.subject, "", false, typ))
						require.NoError(t, ses.setUserDefinedVarWithType("field_candidate", step.candidate, "", false, typ))
						cached := prepared.runtimePlan
						var cachedBefore string
						if cached != nil {
							cachedBefore = cached.String()
						}
						_, runtime, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, &plan.Execute{Name: prepared.Name, Args: args}, "")
						require.NoError(t, err)
						if owned && stmt != nil {
							defer stmt.Free()
						}
						require.Len(t, prepared.fieldCaseDomains, 1)
						require.Equal(t, before, prepared.PreparePlan.String())
						if prepared.runtimePlan != nil && runtime == prepared.runtimePlan {
							cacheHits++
						}
						query := runtime.GetQuery()
						project := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
						executor, err := colexec.NewExpressionExecutor(cw.proc, project)
						require.NoError(t, err)
						defer executor.Free()
						result, err := executor.Eval(cw.proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
						if err != nil {
							t.Logf("source=%v domains=%v project=%s", source, prepared.fieldCaseDomains, project.String())
						}
						require.NoError(t, err)
						want := int64(1 - condition)
						if initial == types.T_varbinary && !step.rebound {
							want = 0
						}
						require.Equal(t, want, vector.GetFixedAtWithTypeCheck[int64](result, 0))
						if cached != nil {
							require.Equal(t, cachedBefore, cached.String(), "execution must not mutate a published cache plan")
						}
						// AP execution caches only its logical plan. Use the same
						// install path without an operator to exercise subsequent hits.
						if cw.runtimeCachePlan != nil {
							prepared.installRuntimeSpecializationCache(cw.runtimeCacheKey, cw.runtimeCachePlan, nil, cw.runtimeCacheDiagnostics)
							cw.discardRuntimeCacheCandidate()
						}
					}()
				}
			}
			require.Positive(t, cacheHits, "exercise logical cache hits, not only fresh bindings")
			require.GreaterOrEqual(t, prepared.fieldCaseRevision, uint64(3))
			published, revision := prepared.fieldCaseDomains, prepared.fieldCaseRevision
			_, _, _, _, _, err = initExecuteStmtParam(execCtx, ses, cw, &plan.Execute{Name: prepared.Name, Args: args[:2]}, "")
			require.Error(t, err)
			require.Equal(t, published, prepared.fieldCaseDomains)
			require.Equal(t, revision, prepared.fieldCaseRevision)
		})
	}
}
