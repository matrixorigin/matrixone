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

func TestPreparedRuntimeAPCacheReusesPlanNotTopology(t *testing.T) {
	for _, oneCN := range []bool{false, true} {
		name := "multi-cn"
		wantExecType := plan2.ExecTypeAP_MULTICN
		if oneCN {
			name = "one-cn"
			wantExecType = plan2.ExecTypeAP_ONECN
		}
		t.Run(name, func(t *testing.T) {
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 26109, "select abs(?)")
			t.Cleanup(func() {
				cw.releaseRuntimeCacheRetiredCompiles()
				cw.proc.SetPrepareParams(nil)
				prepared.Close()
			})
			prepared.params = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(prepared.params, []byte("-1"), false, cw.proc.Mp()))
			prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0}
			_, runtimePlan, _, _, _, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
			require.NoError(t, err)
			require.Same(t, runtimePlan, cw.runtimeCachePlan)
			// Select real AP compilation without a large data fixture or global
			// force-multi-CN injection that could affect unrelated tests.
			for _, node := range runtimePlan.GetQuery().Nodes {
				node.Stats = &plan.Stats{BlockNum: 1000000, Cost: 1e12, Dop: 1, ForceOneCN: oneCN}
			}
			require.Equal(t, wantExecType, plan2.GetExecType(runtimePlan.GetQuery(), false, false))
			op := newTestTxnOp()
			cw.proc.Base.TxnOperator = op
			oldTP := compile.NewCompile("", "", prepared.Sql, "", "", nil,
				cw.proc, prepared.PrepareStmt, false, nil, time.Now())
			prepared.installRuntimeSpecializationCache("previous-category",
				prepared.PreparePlan.GetDcl().GetPrepare().Plan, oldTP)
			newCompiler := func() (*compile.Compile, func()) {
				c := compile.NewCompile("ingress:6001", "", prepared.Sql, "", "",
					&successfulSchedulingPreviewEngine{}, cw.proc, prepared.PrepareStmt, false, nil, time.Now())
				released := false
				release := func() {
					if !released {
						released = true
						params := cw.proc.DetachPrepareParams()
						c.Release()
						cw.proc.RestorePrepareParams(params)
					}
				}
				t.Cleanup(release)
				return c, release
			}
			compiler, release := newCompiler()
			require.NoError(t, compiler.Compile(ec.reqCtx, runtimePlan, nil))
			require.False(t, compiler.IsTpQuery())
			board := cw.proc.GetMessageBoard()
			require.True(t, cw.completeRuntimeCacheCandidate(compiler, nil))
			require.Len(t, cw.runtimeCacheRetiredCompiles, 1)
			require.Same(t, oldTP, cw.runtimeCacheRetiredCompiles[0].compile)
			require.Same(t, board, cw.proc.GetMessageBoard(), "retiring TP must not clear the running AP generation")
			require.Nil(t, prepared.runtimeCompile, "AP physical state must remain statement-owned")
			require.Same(t, runtimePlan, prepared.runtimePlan)
			key := prepared.runtimeSpecializationKey
			require.NotEmpty(t, key)
			// Release through the ordinary statement owner. A cached prepare
			// compile would ignore Release and retain this process state.
			release()
			cw.releaseRuntimeCacheRetiredCompiles()
			require.Nil(t, cw.proc.GetMessageBoard())

			// A prepare-time TP cache must not override a plan-only AP hit.
			prepared.compile = compile.NewCompile("", "", prepared.Sql, "", "", nil,
				cw.proc, prepared.PrepareStmt, false, nil, time.Now())
			prepared.compile.SetIsPrepare(true)
			for _, writable := range []bool{false, true} {
				op.wp.readonly = !writable
				retained, nextPlan, _, _, _, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
				require.NoError(t, err)
				require.Nil(t, retained, "each AP execution must schedule again, writable=%v", writable)
				require.Same(t, runtimePlan, nextPlan, "the type-specialization cache must still hit")
				require.Nil(t, cw.runtimeCachePlan, "a hit must not specialize or publish another category")
				require.Same(t, prepared.params, cw.proc.GetPrepareParams())
				next, releaseNext := newCompiler()
				err = next.Compile(ec.reqCtx, nextPlan, nil)
				if writable && !oneCN {
					require.ErrorContains(t, err, "required-current-cn-outside-pool",
						"a remote-only pool cannot hide the ingress's writable workspace")
				} else {
					require.NoError(t, err)
				}
				releaseNext()
				require.Equal(t, key, prepared.runtimeSpecializationKey)
				require.Same(t, runtimePlan, prepared.runtimePlan, "compile failure must not destroy the valid logical cache")
			}
		})
	}
}
