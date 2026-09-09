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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func installAutoIDCacheTestService(t *testing.T, proc *process.Process, enabled bool) runtime.Runtime {
	t.Helper()
	rt := runtime.ServiceRuntime(proc.GetService())
	old, had := rt.GetGlobalVariables(runtime.AutoIncrementService)
	oldVersion, hadVersion := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	svc := incrservice.NewIncrService(proc.GetService(), incrservice.NewMemStore(), incrservice.Config{EnableAutoIDCache: enabled})
	rt.SetGlobalVariables(runtime.AutoIncrementService, svc)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(runtime.AutoIncrementService, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.AutoIncrementService, svc)
		}
		if hadVersion {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion58)
		}
		svc.Close()
	})
	return rt
}

func TestAutoIDCacheDDLRejectsBeforeMetadataWork(t *testing.T) {
	for _, temporary := range []bool{false, true} {
		proc := testutil.NewProcess(t)
		installAutoIDCacheTestService(t, proc, false)
		def := &plan.TableDef{Name: "t", AutoIdCache: 1}
		scope := &Scope{Plan: &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
			Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{TableDef: def, Temporary: temporary}},
		}}}}
		// No engine/session/txn is provided: any metadata or temp-owner work before
		// the gate would fail instead of returning the intended disabled error.
		require.ErrorContains(t, scope.CreateTable(&Compile{proc: proc}), "AUTO_ID_CACHE is disabled")
		require.Zero(t, def.TblId)
		node := &plan.Node{PreInsertCtx: &plan.PreInsertCtx{TableDef: def}}
		_, err := constructPreInsert(nil, node, nil, proc)
		require.ErrorContains(t, err, "AUTO_ID_CACHE is disabled")
	}
}

func TestAutoIDCacheRemoteWireGate(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := installAutoIDCacheTestService(t, proc, true)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion58)
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	op := &preinsert.PreInsert{HasAutoCol: true, TableDef: &plan.TableDef{Name: "t", AutoIdCache: 1}}
	_, instruction, err := convertToPipelineInstruction(op, proc, ctx, 1)
	require.NoError(t, err)
	require.Equal(t, int32(vm.PreInsertAutoIDCache), instruction.Op)
	wire, err := instruction.Marshal()
	require.NoError(t, err)
	var restoredInstruction pipeline.Instruction
	require.NoError(t, restoredInstruction.Unmarshal(wire))
	restored, err := convertToVmOperator(&restoredInstruction, ctx, nil)
	require.NoError(t, err)
	restoredPreInsert := restored.(*preinsert.PreInsert)
	defer restoredPreInsert.Release()
	require.Equal(t, vm.PreInsert, restoredPreInsert.OpType())
	require.Equal(t, uint64(1), restoredPreInsert.TableDef.AutoIdCache)
	p := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{instruction}}}}
	require.NoError(t, validateRemoteAutoIDCachePipelineProtocol(proc, p))
	require.NoError(t, validateRemoteAutoIDCachePipelineProtocol(nil, nil))
	require.NoError(t, validateRemoteAutoIDCachePipelineProtocol(nil, &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{Op: int32(vm.PreInsert), PreInsert: &pipeline.PreInsert{}}}}))
	require.ErrorContains(t, validateRemoteAutoIDCachePipelineProtocol(nil, p), "requires a process")
	_, _, err = convertToPipelineInstruction(op, nil, ctx, 1)
	require.ErrorContains(t, err, "requires a process")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion57)
	_, _, err = convertToPipelineInstruction(op, proc, ctx, 1)
	require.ErrorContains(t, err, "version 58")
	wire, err = p.Marshal()
	require.NoError(t, err)
	_, err = decodeScope(wire, proc, true, nil)
	require.ErrorContains(t, err, "version 58")
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion58)
	installAutoIDCacheTestService(t, proc, false)
	_, err = decodeScope(wire, proc, true, nil)
	require.ErrorContains(t, err, "AUTO_ID_CACHE is disabled")

	// A stripped marker cannot silently downgrade a nonzero policy on a new receiver.
	instruction.Op = int32(vm.PreInsert)
	require.ErrorContains(t, validateRemoteAutoIDCachePipelineProtocol(proc, p), "marker")
	_, err = convertToVmOperator(instruction, ctx, nil)
	require.ErrorContains(t, err, "marker")
	instruction.Op = int32(vm.PreInsertAutoIDCache)
	instruction.PreInsert.TableDef.AutoIdCache = 0
	require.ErrorContains(t, validateRemoteAutoIDCachePipelineProtocol(proc, p), "marker")
	// The decoder's default path rejects unknown appended opcodes (the same
	// path a pre-feature decoder takes for PreInsertAutoIDCache).
	instruction.Op = int32(vm.OpTypeEnd)
	_, err = convertToVmOperator(instruction, ctx, nil)
	require.ErrorContains(t, err, "unexpected operator")
}
