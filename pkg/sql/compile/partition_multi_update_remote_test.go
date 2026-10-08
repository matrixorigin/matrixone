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
	"errors"
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPartitionFulltextRouteDestinationAdmission(t *testing.T) {
	for _, kind := range []string{"preinsert", "multi_update"} {
		t.Run(kind, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			instruction := &pipeline.Instruction{}
			if kind == "preinsert" {
				instruction.PreInsert = &pipeline.PreInsert{PreserveInput: true}
			} else {
				instruction.MultiUpdate = &pipeline.MultiUpdate{UpdateCtxList: []*plan.UpdateCtx{nil, {PartitionIndexCtx: &plan.PartitionIndexCtx{}}}}
			}
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}, Children: []*pipeline.Pipeline{nil, {InstructionList: []*pipeline.Instruction{nil, instruction}}}}
			// Reuse one endpoint across sends: successful admission is never cached.
			for _, version := range []int64{defines.MORPCVersion106, defines.MORPCVersion107, defines.MORPCVersion106} {
				client.version = version
				calls, releases := client.calls, client.releases
				err := validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{DecimalDivisionSemantics: true})
				if version < defines.MORPCVersion107 {
					require.ErrorContains(t, err, "partitioned FULLTEXT routing (MORPC protocol version 107)")
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, 1, client.calls-calls, "expression and routing floors share one observation")
				require.Equal(t, 1, client.releases-releases)
			}
			client.customResponse = true
			require.Error(t, validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{}))
			client.sendErr = errors.New("unreachable destination")
			require.Error(t, validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{}))
			require.Error(t, validateRemoteExpressionDestination(nil, p, plan.RemoteExpressionFeatures{}))
			p.Node = nil
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{}), "versioned remote destination")
			// An ordinary pipeline imposes no routing floor and performs no probe.
			p.Children = nil
			calls := client.calls
			require.NoError(t, validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{}))
			require.Equal(t, calls, client.calls)
		})
	}
}

func TestPartitionFulltextRouteSendAdmission(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	op := preinsert.NewArgument()
	op.TableDef = &plan.TableDef{}
	op.PreserveInput = true
	t.Cleanup(op.Release)
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, version := range []int64{defines.MORPCVersion106, defines.MORPCVersion107, defines.MORPCVersion106} {
		client.version = version
		calls := client.calls
		data, err := encodeRemoteScope(scope, c.proc)
		if version < defines.MORPCVersion107 {
			require.ErrorContains(t, err, "partitioned FULLTEXT routing (MORPC protocol version 107)")
			require.Empty(t, data)
		} else {
			require.NoError(t, err)
			require.NotEmpty(t, data)
		}
		require.Equal(t, 1, client.calls-calls)
	}
	op.PreserveInput = false
	calls := client.calls
	_, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.Equal(t, calls, client.calls)
}

// Codec tests need only the admission capability, not metadata storage.
type codecPartitionService struct {
	partitionservice.PartitionService
	disabled bool
}

func (s codecPartitionService) Enabled() bool { return !s.disabled }

func TestPartitionMultiUpdateRemoteConstructorRoundTrip(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.PartitionService = codecPartitionService{}
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion107)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, old) })
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}, scope: &Scope{Proc: proc}}
	for _, kind := range []string{"parent", "index", "plain"} {
		for _, action := range []multi_update.UpdateAction{multi_update.UpdateWriteTable, multi_update.UpdateWriteS3, multi_update.UpdateFlushS3Info} {
			t.Run(fmt.Sprintf("%s/%d", kind, action), func(t *testing.T) {
				target := &plan.UpdateCtx{
					ObjRef:                 &plan.ObjectRef{Obj: 10, ObjName: "target"},
					TableDef:               &plan.TableDef{TblId: 10, Name: "target"},
					InsertCols:             []plan.ColRef{{ColPos: 0}, {ColPos: 1}},
					DeleteCols:             []plan.ColRef{{ColPos: 2}, {ColPos: 3}},
					PartitionCols:          []plan.ColRef{{ColPos: 4}, {ColPos: 5}},
					CountDeleteAffectRows:  true,
					IgnoreAffectedRows:     kind == "index",
					AffectedRowsCols:       []plan.ColRef{{ColPos: 7}},
					ChangedRowsCol:         &plan.ColRef{ColPos: 8},
					AffectedRowsWeightCol:  &plan.ColRef{ColPos: 9},
					PhysicalChangedRowsCol: &plan.ColRef{ColPos: 10},
					SkipInsertOnNullPk:     true, InsertPkColIdx: 1,
					DedupByTargetRowId: true, TargetUpdateCtxIdx: 0,
				}
				if kind == "parent" {
					target.TableDef.FeatureFlag |= features.Partitioned
				}
				if kind == "index" {
					target.TableDef.FeatureFlag |= features.IndexTable
					target.PartitionIndexCtx = &plan.PartitionIndexCtx{
						ParentRef:    &plan.ObjectRef{Obj: 77, ObjName: "parent"},
						ParentTable:  &plan.TableDef{TblId: 77, FeatureFlag: features.Partitioned},
						PartitionCol: plan.ColRef{ColPos: 6},
					}
				}
				node := &plan.Node{UpdateCtxList: []*plan.UpdateCtx{target}}
				original, err := constructMultiUpdate(node, nil, proc, action, true)
				require.NoError(t, err)
				defer original.Release()
				_, instruction, err := convertToPipelineInstruction(original, proc, ctx, 1)
				require.NoError(t, err)
				data, err := instruction.Marshal()
				require.NoError(t, err)
				decoded := new(pipeline.Instruction)
				require.NoError(t, decoded.Unmarshal(data))
				restored, err := convertToVmOperator(decoded, ctx, nil)
				require.NoError(t, err)
				defer restored.Release()
				var raw *multi_update.MultiUpdate
				if kind != "plain" && action != multi_update.UpdateFlushS3Info {
					require.IsType(t, &multi_update.PartitionMultiUpdate{}, restored)
					raw = restored.(*multi_update.PartitionMultiUpdate).RawMultiUpdate()
				} else {
					require.IsType(t, &multi_update.MultiUpdate{}, restored)
					raw = restored.(*multi_update.MultiUpdate)
				}
				require.Equal(t, action, raw.Action)
				require.True(t, raw.IsRemote)
				require.True(t, raw.CountDeleteAffectRows)
				require.Equal(t, []int{0, 1}, raw.MultiUpdateCtx[0].InsertCols)
				require.Equal(t, []int{2, 3}, raw.MultiUpdateCtx[0].DeleteCols)
				require.Equal(t, []int{4, 5}, raw.MultiUpdateCtx[0].PartitionCols)
				require.Equal(t, target.PartitionIndexCtx, raw.MultiUpdateCtx[0].PartitionIndexCtx)
				require.Equal(t, target.IgnoreAffectedRows, raw.MultiUpdateCtx[0].IgnoreAffectedRows)
				require.Equal(t, []int{7}, raw.MultiUpdateCtx[0].AffectedRowsCols)
				require.Equal(t, 8, *raw.MultiUpdateCtx[0].ChangedRowsCol)
				require.Equal(t, 9, *raw.MultiUpdateCtx[0].AffectedRowsWeightCol)
				require.Equal(t, 10, *raw.MultiUpdateCtx[0].PhysicalChangedRowsCol)
				require.True(t, raw.MultiUpdateCtx[0].SkipInsertOnNullPk)
				require.Equal(t, 1, raw.MultiUpdateCtx[0].InsertPkColIdx)
				require.True(t, raw.MultiUpdateCtx[0].DedupByTargetRowID)
				require.Equal(t, uint64(10), raw.MultiUpdateCtx[0].TargetTableID)
				for _, unavailable := range []partitionservice.PartitionService{nil, codecPartitionService{disabled: true}} {
					proc.Base.PartitionService = unavailable
					localOp, localErr := constructMultiUpdate(node, nil, proc, action, true)
					remoteOp, remoteErr := convertToVmOperator(decoded, ctx, nil)
					proc.Base.PartitionService = codecPartitionService{}
					if localOp != nil {
						localOp.Release()
					}
					if remoteOp != nil {
						remoteOp.Release()
					}
					if kind != "plain" && action != multi_update.UpdateFlushS3Info {
						require.ErrorContains(t, localErr, "partition service")
						require.ErrorContains(t, remoteErr, "partition service")
					} else {
						require.NoError(t, localErr)
						require.NoError(t, remoteErr)
					}
				}
			})
		}
	}
}

func TestPartitionMultiUpdateRemoteProtocol(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, old) })
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	for _, action := range []multi_update.UpdateAction{multi_update.UpdateWriteTable, multi_update.UpdateWriteS3, multi_update.UpdateFlushS3Info} {
		t.Run(fmt.Sprint(action), func(t *testing.T) {
			raw := &multi_update.MultiUpdate{Action: action, MultiUpdateCtx: []*multi_update.MultiUpdateCtx{{
				TableDef: &plan.TableDef{}, DeleteCols: []int{0, 1},
				PartitionIndexCtx: &plan.PartitionIndexCtx{PartitionCol: plan.ColRef{ColPos: 2}},
			}}}
			op := multi_update.NewPartitionMultiUpdate(raw)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion107)
			_, instruction, err := convertToPipelineInstruction(op, proc, ctx, 1)
			require.NoError(t, err)
			p := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{instruction}}}}
			require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(proc, p))
			data, err := p.Marshal()
			require.NoError(t, err)
			for _, version := range []int64{defines.MORPCVersion88, defines.MORPCVersion100, defines.MORPCVersion101, defines.MORPCVersion102, defines.MORPCVersion103, defines.MORPCVersion104, defines.MORPCVersion105, defines.MORPCVersion106} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				_, _, err = convertToPipelineInstruction(op, proc, ctx, 1)
				require.ErrorContains(t, err, "MORPC protocol version 107")
				require.ErrorContains(t, validateRemoteStatementLastInsertIDPipelineProtocol(proc, p), "MORPC protocol version 107")
				_, err = decodeScope(data, proc, true, nil)
				require.ErrorContains(t, err, "MORPC protocol version 107")
			}
			raw.MultiUpdateCtx[0].PartitionIndexCtx = nil
			_, _, err = convertToPipelineInstruction(raw, proc, ctx, 1)
			require.NoError(t, err)
			instruction.MultiUpdate.UpdateCtxList[0].PartitionIndexCtx = nil
			require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(proc, p))
		})
	}
}
