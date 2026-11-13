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
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExpressionDestinationProtocolPreservesMainFloors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		features plan.RemoteExpressionFeatures
		version  int64
	}{
		{"table collation", plan.RemoteExpressionFeatures{TableCollationFunction: true}, defines.MORPCVersion118},
		{"decimal DIV", plan.RemoteExpressionFeatures{DecimalIntegerDivisionSemantics: true}, defines.MORPCVersion121},
		{"temporal results", plan.RemoteExpressionFeatures{TimestampDatetimeResultContracts: true}, defines.MORPCVersion122},
		{"fractional TIMESTAMPADD", plan.RemoteExpressionFeatures{TimestampAddSecondIntervals: true}, defines.MORPCVersion124},
		{"LOAD FK IGNORE", plan.RemoteExpressionFeatures{LoadFKIgnoreCheck: true}, defines.MORPCVersion125},
		{"JSON numeric comparisons", plan.RemoteExpressionFeatures{JSONNumericComparisons: true}, defines.MORPCVersion128},
		{"JSON ELT", plan.RemoteExpressionFeatures{JSONStringConsumerOverload: true}, defines.MORPCVersion129},
		{"incomplete temporal strings", plan.RemoteExpressionFeatures{IncompleteTemporalStrings: true}, defines.MORPCVersion130},
		{"float conversions", plan.RemoteExpressionFeatures{FloatConversionSemantics: true}, defines.MORPCVersion132},
		{"typed JSON minmax", plan.RemoteExpressionFeatures{JSONMinMaxTypedComparison: true}, defines.MORPCVersion133},
		{"ODKU result", plan.RemoteExpressionFeatures{ODKUStatementResult: true}, defines.MORPCVersion134},
		{"session baseline", plan.RemoteExpressionFeatures{SessionLastInsertIDBaseline: true}, defines.MORPCVersion134},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
			client.version = tc.version - 1
			var requiredProtocol int64
			require.Error(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, tc.features, &requiredProtocol))
			client.version = tc.version
			require.NoError(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, tc.features, &requiredProtocol))
			require.Equal(t, tc.version, requiredProtocol)
			requiredProtocol = defines.MORPCVersion134
			require.NoError(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, tc.features, &requiredProtocol))
			require.Equal(t, defines.MORPCVersion134, requiredProtocol, "a composed requirement must never decrease")
		})
	}
}

func TestPartitionFulltextRouteIndexSearchComposition(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	spec := &plan.IndexSearchScan{AlgoOptions: []byte(`{}`), AlgoExprNames: []string{"probe"}, AlgoExprs: []*plan.Expr{{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 3}}}}}
	p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "receiver", Addr: "remote:6001"}, Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{{Apply: &pipeline.Apply{IndexSearchScan: spec}}, {PreInsert: &pipeline.PreInsert{PreserveInput: true}}}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	decoded := new(pipeline.Pipeline)
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, spec, decoded.Children[0].InstructionList[0].Apply.IndexSearchScan)
	require.True(t, decoded.Children[0].InstructionList[1].PreInsert.PreserveInput)
	require.Equal(t, defines.MORPCVersion111, minimumRemoteVectorProtocol(decoded))
	for _, version := range []int64{defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion134} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		if version < defines.MORPCVersion111 {
			require.ErrorContains(t, validateRemoteVectorPartitionProtocol(c.proc, decoded), "version 111")
		} else {
			require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, decoded))
		}
		require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(c.proc, decoded))
		client.version = version
		calls := client.calls
		require.NoError(t, validateRemoteExpressionDestination(c.proc, decoded, plan.RemoteExpressionFeatures{}))
		require.Equal(t, calls, client.calls)
	}
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
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
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
	proc.Base.PartitionService = codecPartitionService{}
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, old) })
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
	ctx := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	for _, action := range []multi_update.UpdateAction{multi_update.UpdateWriteTable, multi_update.UpdateWriteS3, multi_update.UpdateFlushS3Info} {
		t.Run(fmt.Sprint(action), func(t *testing.T) {
			raw := &multi_update.MultiUpdate{Action: action, MultiUpdateCtx: []*multi_update.MultiUpdateCtx{{
				TableDef: &plan.TableDef{}, DeleteCols: []int{0, 1},
				PartitionIndexCtx: &plan.PartitionIndexCtx{PartitionCol: plan.ColRef{ColPos: 2}},
			}}}
			op := multi_update.NewPartitionMultiUpdate(raw)
			_, instruction, err := convertToPipelineInstruction(op, proc, ctx, 1)
			require.NoError(t, err)
			p := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{instruction}}}}
			require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(proc, p))
			data, err := p.Marshal()
			require.NoError(t, err)
			decoded, err := decodeScope(data, proc, true, nil)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			decoded.release()
			raw.MultiUpdateCtx[0].PartitionIndexCtx = nil
			_, _, err = convertToPipelineInstruction(raw, proc, ctx, 1)
			require.NoError(t, err)
			instruction.MultiUpdate.UpdateCtxList[0].PartitionIndexCtx = nil
			require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(proc, p))
		})
	}
}
