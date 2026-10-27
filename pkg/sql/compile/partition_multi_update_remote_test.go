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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPartitionFulltextRouteDestinationAdmission(t *testing.T) {
	require.False(t, queryRequiresPartitionFulltextRoute(nil))
	require.False(t, queryRequiresPartitionFulltextRoute(&plan.Query{Nodes: []*plan.Node{nil}}))
	// Actual main owns functional indexes at 107, regexp migration at 108,
	// Unicode metadata at 109, MEDIUMINT at 110, and IndexSearch at 111.
	// Main also owns decimal statistics at 113 through SELECT diagnostics at 116.
	// Collation-aware LEAST/GREATEST owns 117 on the frozen main base.
	// Main owns 118-128 through category-preserving JSON/numeric comparisons
	// and uses 129 for JSON ELT and opaque text expression execution.
	// Main owns incomplete temporal strings at 130 and weighted percentiles at 131.
	// Main owns decimal rounding and textual float conversions at 132.
	// Partitioned FULLTEXT routing has its own provisional capability at 135.
	require.Equal(t, int64(107), defines.MORPCVersion107)
	require.Equal(t, int64(108), defines.MORPCVersion108)
	require.Equal(t, int64(109), defines.MORPCVersion109)
	require.Equal(t, int64(110), defines.MORPCVersion110)
	require.Equal(t, int64(111), defines.MORPCVersion111)
	require.Equal(t, int64(113), defines.MORPCVersion113)
	require.Equal(t, int64(116), defines.MORPCVersion116)
	require.Equal(t, int64(117), defines.MORPCVersion117)
	require.Equal(t, int64(118), defines.MORPCVersion118)
	require.Equal(t, int64(119), defines.MORPCVersion119)
	require.Equal(t, int64(120), defines.MORPCVersion120)
	require.Equal(t, int64(121), defines.MORPCVersion121)
	require.Equal(t, int64(122), defines.MORPCVersion122)
	require.Equal(t, int64(123), defines.MORPCVersion123)
	require.Equal(t, int64(124), defines.MORPCVersion124)
	require.Equal(t, int64(125), defines.MORPCVersion125)
	require.Equal(t, int64(126), defines.MORPCVersion126)
	require.Equal(t, int64(127), defines.MORPCVersion127)
	require.Equal(t, int64(128), defines.MORPCVersion128)
	require.Equal(t, int64(129), defines.MORPCVersion129)
	require.Equal(t, int64(130), defines.MORPCVersion130)
	require.Equal(t, int64(131), defines.MORPCVersion131)
	require.Equal(t, int64(132), defines.MORPCVersion132)
	require.Equal(t, int64(135), defines.MORPCVersion135)
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
			node := &plan.Node{}
			if kind == "preinsert" {
				node.PreInsertCtx = &plan.PreInsertCtx{PreserveInput: true}
			} else {
				node.UpdateCtxList = instruction.MultiUpdate.UpdateCtxList
			}
			qry := &plan.Query{Nodes: []*plan.Node{node}, Steps: []int32{0}}
			// Reuse one endpoint across sends: successful admission is never cached.
			for _, version := range []int64{defines.MORPCVersion106, defines.MORPCVersion107, defines.MORPCVersion108, defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134, defines.MORPCVersion135} {
				client.version = version
				c.execType = plan2.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
				if version < defines.MORPCVersion135 {
					require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
				} else {
					require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
				}
				calls, releases := client.calls, client.releases
				requiredProtocol := defines.MORPCVersion111
				err := validateRemoteExpressionDestinationWithProtocol(c.proc, p, plan.RemoteExpressionFeatures{DecimalDivisionSemantics: true}, &requiredProtocol)
				if version < defines.MORPCVersion135 {
					require.ErrorContains(t, err, "partitioned FULLTEXT routing (MORPC protocol version 135)")
				} else {
					require.NoError(t, err)
					require.Equal(t, defines.MORPCVersion135, requiredProtocol)
				}
				require.Equal(t, 1, client.calls-calls, "expression and routing floors share one observation")
				require.Equal(t, 1, client.releases-releases)
			}
			// A new destination cannot override the cluster's older rollout floor.
			// Main's float floor composes with routing without replacing either
			// capability: an admitted route still requires the larger prefix.
			var composedProtocol int64
			require.NoError(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p,
				plan.RemoteExpressionFeatures{FloatConversionSemantics: true}, &composedProtocol))
			require.Equal(t, defines.MORPCVersion135, composedProtocol)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
			client.version = defines.MORPCVersion135
			calls := client.calls
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{}), "MORPC protocol version 135")
			require.Equal(t, calls, client.calls)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
			client.version = defines.MORPCVersion135
			client.customResponse = true
			require.Error(t, validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{}))
			client.sendErr = errors.New("unreachable destination")
			require.Error(t, validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{}))
			require.Error(t, validateRemoteExpressionDestination(nil, p, plan.RemoteExpressionFeatures{}))
			p.Node = nil
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, plan.RemoteExpressionFeatures{}), "versioned remote destination")
			// An ordinary pipeline imposes no routing floor and performs no probe.
			p.Children = nil
			node.PreInsertCtx = nil
			node.UpdateCtxList = nil
			require.False(t, queryRequiresPartitionFulltextRoute(qry))
			calls = client.calls
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
	for _, version := range []int64{defines.MORPCVersion106, defines.MORPCVersion107, defines.MORPCVersion108, defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134, defines.MORPCVersion135} {
		client.version = version
		calls := client.calls
		var requiredProtocol int64
		data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &requiredProtocol)
		if version < defines.MORPCVersion135 {
			require.ErrorContains(t, err, "partitioned FULLTEXT routing (MORPC protocol version 135)")
			require.Empty(t, data)
		} else {
			require.NoError(t, err)
			require.NotEmpty(t, data)
			require.Equal(t, defines.MORPCVersion135, requiredProtocol, "route admission must reach the execution stream")
		}
		require.Equal(t, 1, client.calls-calls)
	}
	// remoteRun receives the requirement from the final, post-folding encoding.
	client.version = defines.MORPCVersion135
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	c.proc.Ctx = defines.AttachAccountId(context.Background(), 0)
	var requiredProtocol int64
	_, _, _, _, err := prepareRemoteRunSendingDataWithProtocol("", scope, c.proc, nil, uuid.Nil, &requiredProtocol)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion135, requiredProtocol)
	op.PreserveInput = false
	calls := client.calls
	requiredProtocol = 0
	_, err = encodeRemoteScopeWithProtocol(scope, c.proc, &requiredProtocol)
	require.NoError(t, err)
	require.Zero(t, requiredProtocol)
	require.Equal(t, calls, client.calls)
}

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
			requiredProtocol = defines.MORPCVersion135
			require.NoError(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, tc.features, &requiredProtocol))
			require.Equal(t, defines.MORPCVersion135, requiredProtocol, "a composed requirement must never decrease")
		})
	}
}

func TestPartitionFulltextRouteIndexSearchComposition(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	// This is a codec/admission fixture, not an executed query. Keep the
	// IndexSearch-only payload in APPLY so a data source cannot mask its loss.
	spec := &plan.IndexSearchScan{
		AlgoOptions:   []byte(`{}`),
		AlgoExprNames: []string{"probe"},
		AlgoExprs: []*plan.Expr{{Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 1, ColPos: 3},
		}}},
	}
	p := &pipeline.Pipeline{
		Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"},
		Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{
			{Apply: &pipeline.Apply{IndexSearchScan: spec}},
			{PreInsert: &pipeline.PreInsert{PreserveInput: true}},
		}}},
	}
	data, err := p.Marshal()
	require.NoError(t, err)
	decoded := new(pipeline.Pipeline)
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, spec, decoded.Children[0].InstructionList[0].Apply.IndexSearchScan)
	require.Equal(t, defines.MORPCVersion111, minimumRemoteVectorProtocol(decoded))
	for _, version := range []int64{defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134, defines.MORPCVersion135} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		if version < defines.MORPCVersion111 {
			require.ErrorContains(t, validateRemoteVectorPartitionProtocol(c.proc, decoded), "version 111")
		} else {
			require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, decoded))
		}
		if version < defines.MORPCVersion135 {
			require.ErrorContains(t, validateRemoteStatementLastInsertIDPipelineProtocol(c.proc, decoded), "version 135")
		} else {
			require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(c.proc, decoded))
		}
	}
	for _, version := range []int64{defines.MORPCVersion111, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134, defines.MORPCVersion135} {
		client.version = version
		err := validateRemoteExpressionDestination(c.proc, decoded, plan.RemoteExpressionFeatures{})
		if version < defines.MORPCVersion135 {
			require.ErrorContains(t, err, "version 135")
		} else {
			require.NoError(t, err)
		}
	}
	// Main's IndexSearch path alone still admits 111; it must not inherit the
	// partition-route floor merely because both contracts occur in one file.
	decoded.Children[0].InstructionList[1].PreInsert.PreserveInput = false
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion111)
	require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, decoded))
	require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(c.proc, decoded))
	calls := client.calls
	require.NoError(t, validateRemoteExpressionDestination(c.proc, decoded, plan.RemoteExpressionFeatures{}))
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
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
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
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
			_, instruction, err := convertToPipelineInstruction(op, proc, ctx, 1)
			require.NoError(t, err)
			p := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{instruction}}}}
			require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(proc, p))
			data, err := p.Marshal()
			require.NoError(t, err)
			for _, version := range []int64{defines.MORPCVersion88, defines.MORPCVersion100, defines.MORPCVersion101, defines.MORPCVersion102, defines.MORPCVersion103, defines.MORPCVersion104, defines.MORPCVersion105, defines.MORPCVersion106, defines.MORPCVersion107, defines.MORPCVersion108, defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				_, _, err = convertToPipelineInstruction(op, proc, ctx, 1)
				require.ErrorContains(t, err, "MORPC protocol version 135")
				require.ErrorContains(t, validateRemoteStatementLastInsertIDPipelineProtocol(proc, p), "MORPC protocol version 135")
				_, err = decodeScope(data, proc, true, nil)
				require.ErrorContains(t, err, "MORPC protocol version 135")
			}
			raw.MultiUpdateCtx[0].PartitionIndexCtx = nil
			_, _, err = convertToPipelineInstruction(raw, proc, ctx, 1)
			require.NoError(t, err)
			instruction.MultiUpdate.UpdateCtxList[0].PartitionIndexCtx = nil
			require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(proc, p))
		})
	}
}
