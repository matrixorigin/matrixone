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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedDecimalFloatSingleSlotCache(t *testing.T) {
	const wide = "1152921504606846976"
	for _, first := range []string{"7", wide} {
		t.Run(first, func(t *testing.T) {
			base := plan2.NewMockCompilerContext(false, newPlanTestProcess(t))
			_, table, err := base.Resolve("tpch", "nation", nil)
			require.NoError(t, err)
			for _, col := range table.Cols {
				if col.Name == "n_nationkey" {
					col.Typ.Id, col.Typ.Width, col.Typ.Scale = int32(types.T_decimal128), 18, 2
				}
			}
			ctx := &preparedStatsTestCompiler{preparedTestCompiler: &preparedTestCompiler{CompilerContext: base, proc: base.GetProcess()}, table: table}
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQLWithCompilerContext(
				t, 29661, "select n_name from nation where n_nationkey=cast(? as double)", ctx)
			ctx.proc = cw.proc
			closed := false
			closePrepared := func() {
				if closed {
					return
				}
				closed = true
				cw.releaseRuntimeCacheRetiredCompiles()
				cw.proc.SetPrepareParams(nil)
				prepared.Close()
			}
			t.Cleanup(closePrepared)
			bind := func(value string, null bool, source defines.MysqlType) {
				t.Helper()
				cw.proc.SetPrepareParams(nil)
				if prepared.params != nil {
					prepared.params.Free(cw.proc.Mp())
				}
				prepared.params = vector.NewVec(types.T_text.ToType())
				require.NoError(t, vector.AppendBytes(prepared.params, []byte(value), null, cw.proc.Mp()))
				prepared.ParamTypes = []byte{byte(source), 0}
			}
			execute := func() (*compile.Compile, *plan.Plan) {
				t.Helper()
				cw.discardRuntimeCacheCandidate()
				comp, p, stmt, _, owned, err := initExecuteStmtParamWithResolverInSession(ec, ses, ses, cw, nil, prepared.Name, ctx.Resolve, ctx)
				if owned && stmt != nil {
					stmt.Free()
				}
				require.NoError(t, err)
				return comp, p
			}
			install := func(p *plan.Plan) *compile.Compile {
				t.Helper()
				require.Same(t, p, cw.runtimeCachePlan)
				require.NotEmpty(t, cw.runtimeCacheDecimalFloatProofs)
				comp := compile.NewCompile("", "", prepared.Sql, "", "", nil, cw.proc, prepared.PrepareStmt, false, nil, time.Now())
				installed := false
				t.Cleanup(func() {
					if !installed {
						comp.FreeOperator()
						comp.Release()
					}
				})
				installed = cw.completeRuntimeCacheCandidate(comp, nil)
				require.True(t, installed)
				require.NotEmpty(t, prepared.runtimeDecimalFloatProofs)
				require.Nil(t, cw.runtimeCacheDecimalFloatProofs)
				return comp
			}
			bind(first, false, defines.MYSQL_TYPE_LONGLONG)
			_, cachedPlan := execute()
			cached := install(cachedPlan)
			other := wide
			if first == wide {
				other = "7"
			}
			firstWide := first == wide
			cachedWide := firstWide
			firstSibling, otherSibling := "9", "1152921504606846977"
			if firstWide {
				firstSibling, otherSibling = otherSibling, firstSibling
			}
			for _, step := range []struct {
				value string
				wide  bool
			}{{firstSibling, firstWide}, {other, !firstWide}, {otherSibling, !firstWide}, {first, firstWide}, {firstSibling, firstWide}} {
				bind(step.value, false, defines.MYSQL_TYPE_LONGLONG)
				comp, p := execute()
				if step.wide == cachedWide {
					require.Same(t, cachedPlan, p, "same proof class must not replan: %s", step.value)
					require.Same(t, cached, comp, "repeated values in either class must reuse the compile")
					require.Nil(t, cw.runtimeCacheTarget)
					continue
				}
				require.Nil(t, comp)
				require.NotSame(t, cachedPlan, p, "a different proof class needs exactly one new plan")
				require.NotEmpty(t, cw.runtimeCacheDecimalFloatProofs)
				oldProofs := prepared.runtimeDecimalFloatProofs
				require.False(t, cw.completeRuntimeCacheCandidate(nil, context.Canceled))
				require.Same(t, cachedPlan, prepared.runtimePlan)
				require.Same(t, cached, prepared.runtimeCompile)
				require.Equal(t, oldProofs, prepared.runtimeDecimalFloatProofs, "failed compile must leave the previous proof paired with its plan")
				require.Nil(t, cw.runtimeCacheDecimalFloatProofs)
				_, p = execute()
				cached = install(p)
				cachedPlan = p
				cachedWide = step.wide
			}
			for _, tc := range []struct {
				value  string
				null   bool
				source defines.MysqlType
			}{
				{"", true, defines.MYSQL_TYPE_LONGLONG},
				{"7.25", false, defines.MYSQL_TYPE_DOUBLE},
			} {
				bind(tc.value, tc.null, tc.source)
				comp, _ := execute()
				require.Nil(t, comp, "NULL or a different source cannot reuse an integer proof")
				bind(first, false, defines.MYSQL_TYPE_LONGLONG)
				comp, p := execute()
				require.Same(t, cached, comp)
				require.Same(t, cachedPlan, p)
			}
			// AP keeps only the logical specialization. The same install path
			// must retain its proof even without a reusable physical Compile.
			old := prepared.installRuntimeSpecializationCache(prepared.runtimeSpecializationKey,
				cachedPlan, nil, prepared.runtimeDiagnosticCandidates, prepared.runtimeDecimalFloatProofs, prepared.runtimeColDefData)
			require.Same(t, cached, old)
			prepared.releaseRuntimeCompile(old)
			require.NotEmpty(t, prepared.runtimeDecimalFloatProofs)
			comp, p := execute()
			require.Nil(t, comp)
			require.Same(t, cachedPlan, p)
			prepared.clearRuntimeSpecializationCache()
			require.Nil(t, prepared.runtimeDecimalFloatProofs)
			_, p = execute()
			install(p)
			closePrepared()
			require.Nil(t, prepared.runtimeDecimalFloatProofs)
		})
	}
}
