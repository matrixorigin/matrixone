// Copyright 2026 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type enabledCompilePartitionService struct {
	partitionservice.PartitionService
}

func (enabledCompilePartitionService) Enabled() bool { return true }

func TestPartitionMultiUpdateWriterStaysOnCoordinator(t *testing.T) {
	for _, action := range []struct {
		name    string
		writeS3 bool
	}{
		{"s3", true},
		{"table", false},
	} {
		for _, sourceAddr := range []string{"cn1:6001", "cn2:6001"} {
			t.Run(action.name+"/"+sourceAddr, func(t *testing.T) {
				c := NewMockCompile(t)
				defer c.proc.Free()
				c.addr = "cn1:6001"
				c.ncpu = 1
				c.execType = plan2.ExecTypeTP
				c.anal = &AnalyzeModule{qry: &plan.Query{LoadWriteS3: action.writeS3}}
				c.proc.Base.PartitionService = enabledCompilePartitionService{partitionservice.DisabledService}
				source := newScope(Remote)
				source.NodeInfo = engine.Node{Addr: sourceAddr, Mcpu: 1}
				source.IsRemote = sourceAddr != c.addr
				source.Proc = c.proc.NewNoContextChildProc(1)
				source.RootOp = value_scan.NewArgument()
				node := &plan.Node{
					Stats:         &plan.Stats{},
					UpdateCtxList: []*plan.UpdateCtx{{TableDef: &plan.TableDef{TblId: 1, FeatureFlag: features.Partitioned}}},
				}

				result, err := c.compileMultiUpdate(node, []*Scope{source})
				require.NoError(t, err)
				require.Len(t, result, 1)
				var writers []*Scope
				var remoteSources []*Scope
				var visit func(*Scope)
				visit = func(scope *Scope) {
					if scope == nil {
						return
					}
					if scope.Magic == Remote && !scope.ipAddrMatch(c.addr) {
						remoteSources = append(remoteSources, scope)
					}
					var walkOp func(vm.Operator)
					walkOp = func(op vm.Operator) {
						if op == nil {
							return
						}
						if _, ok := op.(*multi_update.PartitionMultiUpdate); ok {
							writers = append(writers, scope)
						}
						for _, child := range op.GetOperatorBase().Children {
							walkOp(child)
						}
					}
					walkOp(scope.RootOp)
					for _, pre := range scope.PreScopes {
						visit(pre)
					}
				}
				visit(result[0])
				require.Len(t, writers, 1)
				require.True(t, writers[0].ipAddrMatch(c.addr), "partition writer must execute on the coordinator")
				if sourceAddr != c.addr {
					require.Len(t, remoteSources, 1)
					encoded, _ := getScopeForRemoteRunEncoding(remoteSources[0])
					require.IsType(t, &value_scan.ValueScan{}, encoded.RootOp)
					_, _, err = convertToPipelineInstruction(encoded.RootOp, c.proc, &scopeContext{}, 1)
					require.NoError(t, err, "the remote source must use the existing operator codec")
				} else {
					require.Empty(t, remoteSources)
				}
			})
		}
	}
}

func TestPartitionMultiUpdateKeepsParallelWritersForRemoteSources(t *testing.T) {
	c := NewMockCompile(t)
	defer c.proc.Free()
	c.addr = "cn1:6001"
	c.ncpu = 2
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{qry: &plan.Query{LoadWriteS3: true}}
	c.proc.Base.PartitionService = enabledCompilePartitionService{partitionservice.DisabledService}
	sources := make([]*Scope, 2)
	for i, addr := range []string{"cn2:6001", "cn3:6001"} {
		sources[i] = newScope(Remote)
		sources[i].NodeInfo = engine.Node{Addr: addr, Mcpu: 1}
		sources[i].IsRemote = true
		sources[i].Proc = c.proc.NewNoContextChildProc(1)
		sources[i].RootOp = value_scan.NewArgument()
	}
	node := &plan.Node{
		Stats:         &plan.Stats{},
		UpdateCtxList: []*plan.UpdateCtx{{TableDef: &plan.TableDef{TblId: 1, FeatureFlag: features.Partitioned}}},
	}
	originalSources := append([]*Scope(nil), sources...)
	result, err := c.compileMultiUpdate(node, sources)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0].PreScopes, 2)
	for i, writer := range result[0].PreScopes {
		require.True(t, writer.ipAddrMatch(c.addr))
		require.Len(t, writer.PreScopes, 1)
		require.Same(t, originalSources[i], writer.PreScopes[0])
		require.IsType(t, &multi_update.PartitionMultiUpdate{}, writer.RootOp.GetOperatorBase().GetChildren(0))
		require.Equal(t, remoteS3None, scopeS3Output(originalSources[i]))
	}
}

func TestPartitionMultiUpdateGroupsRemoteShuffleBeforeMovingWriter(t *testing.T) {
	c := NewMockCompile(t)
	defer c.proc.Free()
	c.addr = "cn1:6001"
	c.ncpu = 2
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 2}, {Addr: "cn2:6001", Mcpu: 2}}
	c.anal = &AnalyzeModule{qry: &plan.Query{LoadWriteS3: true}}
	c.proc.Base.PartitionService = enabledCompilePartitionService{partitionservice.DisabledService}
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	buckets := make([]*Scope, 4)
	for i, addr := range []string{"cn1:6001", "cn1:6001", "cn2:6001", "cn2:6001"} {
		buckets[i] = newScope(Remote)
		buckets[i].NodeInfo = engine.Node{Addr: addr, Mcpu: 1}
		buckets[i].Proc = c.proc.NewContextChildProc(1)
		buckets[i].setRootOperator(merge.NewArgument())
	}
	remoteDispatch := newDispatchSrcScopeForTest(c.proc, "cn2:6001", buckets[2:], buckets[:2])
	buckets[2].PreScopes = append(buckets[2].PreScopes, remoteDispatch)
	require.False(t, checkPipelineStandaloneExecutableAtRemote(buckets[2]))
	node := &plan.Node{
		Stats:         &plan.Stats{},
		UpdateCtxList: []*plan.UpdateCtx{{TableDef: &plan.TableDef{TblId: 1, FeatureFlag: features.Partitioned}}},
	}
	result, err := c.compileMultiUpdate(node, buckets)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0].PreScopes, 2)
	var remoteGroup *Scope
	for _, writer := range result[0].PreScopes {
		if len(writer.PreScopes) == 1 && writer.PreScopes[0].NodeInfo.Addr == "cn2:6001" {
			remoteGroup = writer.PreScopes[0]
			require.True(t, writer.ipAddrMatch(c.addr))
			require.IsType(t, &multi_update.PartitionMultiUpdate{}, writer.RootOp.GetOperatorBase().GetChildren(0))
		}
	}
	require.NotNil(t, remoteGroup)
	require.Len(t, remoteGroup.PreScopes, 2)
	require.True(t, checkPipelineStandaloneExecutableAtRemote(remoteGroup))
}
