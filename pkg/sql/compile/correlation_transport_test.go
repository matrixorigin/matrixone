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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDependentRemoteFence(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, e := range []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Slot{Slot: &plan.CorrelationSlot{ProgramId: 1}}},
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Dependent{Dependent: &plan.DependentExpr{ProgramId: 1}}},
	} {
		op := projection.NewArgument()
		op.ProjectList = []*plan.Expr{e}
		scope := &Scope{Magic: Remote, Proc: proc, RootOp: op}
		_, err := encodeScope(scope)
		require.ErrorContains(t, err, "coordinator execution owner")
		_, err = encodeRemoteScope(scope, proc)
		require.ErrorContains(t, err, "coordinator execution owner")
		op.Release()
		wire := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{Op: int32(vm.Projection), ProjectList: []*plan.Expr{e}}}}
		data, err := wire.Marshal()
		require.NoError(t, err)
		_, err = decodeScope(data, proc, true, nil)
		require.ErrorContains(t, err, "coordinator execution owner")
		// Retained query metadata cannot smuggle an orphan private expression
		// past the fence by omitting the program descriptor.
		wire = &pipeline.Pipeline{Qry: &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			Nodes: []*plan.Node{{ProjectList: []*plan.Expr{e}}},
		}}}}
		data, err = wire.Marshal()
		require.NoError(t, err)
		_, err = decodeScope(data, proc, true, nil)
		require.ErrorContains(t, err, "coordinator execution owner")
	}
	_, err := encodeScope(&Scope{Magic: Remote, Proc: proc, RootOp: &dependent.Operator{}})
	require.ErrorContains(t, err, "coordinator execution owner")
	require.NoError(t, validateRemoteCorrelation(&pipeline.Pipeline{}))
}

func TestDependentCoordinatorAdmissionBeforeSourceWork(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, t.Name(), mpool.MustNewZero())
	defer proc.Free()
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	previous, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion118)
	c := &Compile{proc: proc}
	// Neither source metadata nor a cached execution may initialize when the
	// deployment capability is below the program's contract.
	_, err := c.compileDependentQuery(nil)
	require.ErrorContains(t, err, "protocol version 119")
	op := &dependent.Operator{}
	require.ErrorContains(t, op.Prepare(proc), "protocol version 119")
}

func TestDependentWriteTargetAdmission(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, t.Name(), mpool.MustNewZero())
	defer proc.Free()
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)
	shared := &plan.TableDef{Defs: []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{
		Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: catalog.PropFromPublication, Value: "true"}}},
	}}}}
	for _, node := range []*plan.Node{
		{NodeType: plan.Node_INSERT, InsertCtx: &plan.InsertCtx{TableDef: shared}},
		{NodeType: plan.Node_DELETE, DeleteCtx: &plan.DeleteCtx{TableDef: shared}},
		{NodeType: plan.Node_MULTI_UPDATE, UpdateCtxList: []*plan.UpdateCtx{{TableDef: &plan.TableDef{}}, {TableDef: shared}}},
	} {
		t.Run(node.NodeType.String(), func(t *testing.T) {
			c := &Compile{proc: proc}
			// No engine, source compiler or analysis state is installed: admission
			// must fail before any of those resources can be touched.
			_, err := c.compileDependentQuery(&plan.Query{Steps: []int32{0}, Nodes: []*plan.Node{node}})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrCCPRReadOnly), "%v", err)
		})
	}
	c := &Compile{proc: proc}
	require.NoError(t, c.validateDMLTarget(&plan.Node{NodeType: plan.Node_INSERT, InsertCtx: &plan.InsertCtx{TableDef: &plan.TableDef{}}}))
	require.NoError(t, c.validateDMLTarget(&plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: shared}))
}
