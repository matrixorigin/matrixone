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

package compile

import (
	"context"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestIVFOwnershipWire(t *testing.T) {
	makeProc := func(t *testing.T) *process.Process {
		t.Helper()
		proc := testutil.NewProcess(t)
		t.Cleanup(proc.Free)
		return proc
	}
	readPipeline := func(t *testing.T, data []byte) *pipeline.Pipeline {
		t.Helper()
		wire := &pipeline.Pipeline{}
		require.NoError(t, wire.Unmarshal(data))
		require.Len(t, wire.GetInstructionList(), 1)
		return wire
	}

	c, node := ivfCandidateCompileFixture(t, 2, nil)
	scopes, err := c.compileIvfSearchParallel(node)
	require.NoError(t, err)
	require.Len(t, scopes, 2)
	t.Cleanup(func() { ReleaseScopes(scopes) })
	remoteTVF := scopes[1]
	wireBytes, err := encodeScope(remoteTVF)
	require.NoError(t, err)
	wire := readPipeline(t, wireBytes)
	require.Equal(t, wireIvfSearchStableOwnership, wire.InstructionList[0].Op)
	require.GreaterOrEqual(t, wireIvfSearchStableOwnership, int32(vm.OpTypeEnd))
	require.GreaterOrEqual(t, wireIvfEntriesScanStableOwnership, int32(vm.OpTypeEnd))
	// convertToVmOperator's default arm is unchanged from the old receiver.
	_, err = convertToVmOperator(wire.InstructionList[0], nil, nil)
	require.ErrorContains(t, err, "unexpected operator")

	wire.InstructionList[0].Op = int32(vm.TableFunction)
	legacyTVF, err := wire.Marshal()
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		bytes  []byte
		stable bool
	}{
		{"upgraded-coordinator-upgraded-worker", wireBytes, true},
		{"old-coordinator-upgraded-worker", legacyTVF, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receiver := makeProc(t)
			top := receiver.GetTopContext()
			if !tc.stable {
				// Unmarked input explicitly clears a prior marker.
				receiver.ReplaceTopCtx(context.WithValue(top, stableIvfOwnershipKey{}, true))
			}

			scope, err := decodeScope(tc.bytes, receiver, true, nil)
			require.NoError(t, err)
			t.Cleanup(scope.release)
			fn, ok := scope.RootOp.(*table_function.TableFunction)
			require.True(t, ok)
			require.Equal(t, int32(1), fn.IndexReaderParam.GetPartitionCnIdx())
			require.Equal(t, int32(2), fn.IndexReaderParam.GetPartitionCnCnt())
			require.Equal(t, tc.stable, stableIvfOwnership(receiver.GetTopContext()))

			receiver.BuildPipelineContext(receiver.GetTopContext())
			scope.Proc.BuildPipelineContext(receiver.Ctx)
			spy := &spySQLExecutor{}
			rt := moruntime.ServiceRuntime(scope.Proc.GetService())
			previous, hadPrevious := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spy)
			t.Cleanup(func() {
				if hadPrevious {
					rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, spy)
				}
			})
			sqlProc := sqlexec.NewSqlProcess(scope.Proc)
			sqlProc.IndexReaderParam = fn.IndexReaderParam
			_, err = sqlexec.RunSql(sqlProc, "select 1")
			require.NoError(t, err)
			require.Equal(t, fn.IndexReaderParam, spy.ctx.Value(defines.IvfReaderParam{}))

			// The real SQL invocation's derived context is also used by the
			// inner compiler when this outer worker's entries scan is local.
			inner := newScope(Remote)
			t.Cleanup(inner.release)
			inner.Proc = makeProc(t)
			inner.Proc.ReplaceTopCtx(spy.ctx)
			inner.Proc.BuildPipelineContext(spy.ctx)
			inner.DataSource = &Source{node: ivfTestEntriesScanNode(), MembershipFilterBytes: []byte{1, 2}}
			inner.NodeInfo = engine.Node{CNCNT: 2, CNIDX: 1}
			inner.setRootOperator(table_scan.NewArgument())
			rsp := inner.rangesShuffleParam(spy.ctx)
			require.Equal(t, tc.stable, rsp.KeepAppendableLocal)
			require.Equal(t, !tc.stable, rsp.IsLocalCN, "physically local is not the stable logical partition owner")

			innerBytes, err := encodeScope(inner)
			require.NoError(t, err)
			wire := readPipeline(t, innerBytes)
			require.Equal(t, []byte{1, 2}, wire.GetDataSource().GetMembershipFilter())
			if tc.stable {
				require.Equal(t, wireIvfEntriesScanStableOwnership, wire.InstructionList[0].Op)
				_, err = convertToVmOperator(wire.InstructionList[0], nil, nil)
				require.ErrorContains(t, err, "unexpected operator")
			} else {
				require.Equal(t, int32(vm.TableScan), wire.InstructionList[0].Op)
			}
			scan, err := decodeScope(innerBytes, makeProc(t), true, nil)
			require.NoError(t, err)
			t.Cleanup(scan.release)
			require.Equal(t, vm.TableScan, scan.RootOp.OpType())
			require.Equal(t, tc.stable, scan.rangesShuffleParam(scan.Proc.GetTopContext()).KeepAppendableLocal)
		})
	}

	t.Run("reject-malformed-or-mixed-fragment", func(t *testing.T) {
		wire := readPipeline(t, wireBytes)
		wire.InstructionList[0].TableFunction.IndexReaderParam = nil
		malformed, err := wire.Marshal()
		require.NoError(t, err)
		scope, err := decodeScope(malformed, makeProc(t), true, nil)
		require.Nil(t, scope)
		require.ErrorContains(t, err, "invalid IVF ownership instruction payload")

		wire = readPipeline(t, wireBytes)
		wire.Children = append(wire.Children, readPipeline(t, legacyTVF))
		mixed, err := wire.Marshal()
		require.NoError(t, err)
		scope, err = decodeScope(mixed, makeProc(t), true, nil)
		require.Nil(t, scope)
		require.ErrorContains(t, err, "mixed IVF ownership instructions")
	})

	t.Run("ordinary-and-single-CN-scans-keep-wire-opcode", func(t *testing.T) {
		for _, tc := range []struct {
			node *plan.Node
			cns  int32
		}{
			{&plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Name: "ordinary"}}, 2},
			{ivfTestEntriesScanNode(), 1},
		} {
			scope := newScope(Remote)
			t.Cleanup(scope.release)
			scope.Proc = makeProc(t)
			scope.Proc.BuildPipelineContext(scope.Proc.GetTopContext())
			scope.DataSource = &Source{node: tc.node}
			scope.NodeInfo = engine.Node{CNCNT: tc.cns}
			scope.setRootOperator(table_scan.NewArgument())
			wireBytes, err := encodeScope(scope)
			require.NoError(t, err)
			wire := readPipeline(t, wireBytes)
			require.Equal(t, int32(vm.TableScan), wire.InstructionList[0].GetOp())
		}
	})
}
