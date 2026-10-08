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
	"context"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func makeRightSingleTopologyPlan(tag int32) *plan.Query {
	probeSpec := &plan.RuntimeFilterSpec{Tag: tag, Expr: &plan.Expr{}}
	buildSpec := &plan.RuntimeFilterSpec{
		Tag:         tag,
		BuildExpr:   &plan.Expr{},
		KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
	}
	return &plan.Query{Nodes: []*plan.Node{
		{
			NodeType:               plan.Node_TABLE_SCAN,
			NodeId:                 0,
			RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{probeSpec},
		},
		{
			NodeType:               plan.Node_JOIN,
			NodeId:                 1,
			JoinType:               plan.Node_SINGLE,
			IsRightJoin:            true,
			Children:               []int32{0},
			Stats:                  &plan.Stats{HashmapStats: &plan.HashMapStats{}},
			RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{buildSpec},
		},
	}}
}

func makeRuntimeFilterConsumerScope(tag int32, cn engine.Node) *Scope {
	probeSpec := &plan.RuntimeFilterSpec{Tag: tag}
	return &Scope{
		NodeInfo: cn,
		DataSource: &Source{
			RuntimeFilterSpecs: []*plan.RuntimeFilterSpec{probeSpec},
			node: &plan.Node{
				NodeType:               plan.Node_TABLE_SCAN,
				RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{probeSpec},
			},
		},
	}
}

func makeRuntimeFilterProducerScope(tag int32, cn engine.Node) (*Scope, *hashbuild.HashBuild) {
	build := hashbuild.NewArgument()
	build.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tag}
	return &Scope{NodeInfo: cn, RootOp: build}, build
}

func TestValidateLocalRuntimeFilterTopology(t *testing.T) {
	const tag int32 = 7
	cn1 := engine.Node{Id: "cn1", Addr: "cn1:6001"}
	cn2 := engine.Node{Id: "cn2", Addr: "cn2:6001"}
	qry := makeRightSingleTopologyPlan(tag)
	compiled := []int32{1}

	t.Run("one local producer is reachable", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer}))
	})

	t.Run("standalone scopes without CN metadata are colocated", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, engine.Node{})
		producer, build := makeRuntimeFilterProducerScope(tag, engine.Node{})
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer}))
	})

	t.Run("partial CN metadata is rejected as ambiguous", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, engine.Node{})
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		err := validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "consumer <local> cannot reach colocated producer")
		require.ErrorContains(t, err, "cn1:6001")
	})

	t.Run("remote-only producer is rejected before execution", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn2)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		err := validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "tag 7")
		require.ErrorContains(t, err, "cn2:6001")
		require.ErrorContains(t, err, "cannot reach colocated producer")
		require.ErrorContains(t, err, "cn1:6001")
	})

	t.Run("missing producer is rejected", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)

		err := validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "tag 7 has no producer")
	})

	t.Run("duplicate local producers are rejected", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer1, build1 := makeRuntimeFilterProducerScope(tag, cn1)
		producer2, build2 := makeRuntimeFilterProducerScope(tag, cn1)
		defer build1.Release()
		defer build2.Release()
		consumer.PreScopes = []*Scope{producer1, producer2}

		err := validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "2 producers")
		require.ErrorContains(t, err, "expected exactly one colocated producer")
	})

	t.Run("parallel scan counts future HashBuild clones", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		scan := table_scan.NewArgument()
		defer scan.Release()
		build.AppendChild(scan)
		producer.NodeInfo.Mcpu = 4
		consumer.PreScopes = []*Scope{producer}

		err := validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "4 producers")
		require.ErrorContains(t, err, "expected exactly one colocated producer")
	})

	t.Run("Mcpu alone does not clone a non-scan producer", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		mergeInput := merge.NewArgument()
		defer mergeInput.Release()
		build.AppendChild(mergeInput)
		producer.NodeInfo.Mcpu = 4
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer}))
	})

	t.Run("one producer on each CN is not a colocated phase-1 topology", func(t *testing.T) {
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer1, build1 := makeRuntimeFilterProducerScope(tag, cn1)
		producer2, build2 := makeRuntimeFilterProducerScope(tag, cn2)
		defer build1.Release()
		defer build2.Release()
		consumer.PreScopes = []*Scope{producer1, producer2}

		err := validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "2 producers")
		require.ErrorContains(t, err, "cn1:6001")
		require.ErrorContains(t, err, "cn2:6001")
	})

	t.Run("missing scan consumer is rejected", func(t *testing.T) {
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()

		err := validateLocalRuntimeFilterTopology(qry, compiled, []*Scope{producer})
		require.ErrorContains(t, err, "tag 7 has no scan consumer")
	})

	t.Run("fully unmaterialized tag from a pruned subtree is ignored", func(t *testing.T) {
		require.NoError(t, validateLocalRuntimeFilterTopology(qry, nil, nil))
	})

	t.Run("fully missing topology for a compiled subtree is rejected", func(t *testing.T) {
		err := validateLocalRuntimeFilterTopology(qry, compiled, nil)
		require.ErrorContains(t, err, "tag 7 has no scan consumer")
	})

	t.Run("pruned tag does not hide a partial executed topology", func(t *testing.T) {
		const prunedTag int32 = 8
		mixedQry := makeRightSingleTopologyPlan(tag)
		prunedJoin := makeRightSingleTopologyPlan(prunedTag).Nodes[1]
		mixedQry.Nodes = append(mixedQry.Nodes, prunedJoin)
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)

		err := validateLocalRuntimeFilterTopology(mixedQry, compiled, []*Scope{consumer})
		require.ErrorContains(t, err, "tag 7 has no producer")
	})

	t.Run("pruned tag does not invalidate a complete executed topology", func(t *testing.T) {
		const prunedTag int32 = 8
		mixedQry := makeRightSingleTopologyPlan(tag)
		prunedJoin := makeRightSingleTopologyPlan(prunedTag).Nodes[1]
		mixedQry.Nodes = append(mixedQry.Nodes, prunedJoin)
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateLocalRuntimeFilterTopology(mixedQry, compiled, []*Scope{consumer}))
	})

	t.Run("unrelated and shuffle runtime filters are ignored", func(t *testing.T) {
		noRightSingle := &plan.Query{Nodes: []*plan.Node{{
			NodeType:               plan.Node_TABLE_SCAN,
			RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{Tag: tag}},
		}}}
		require.NoError(t, validateLocalRuntimeFilterTopology(noRightSingle, nil, nil))

		shuffle := makeRightSingleTopologyPlan(tag)
		shuffle.Nodes[1].Stats.HashmapStats.Shuffle = true
		require.NoError(t, validateLocalRuntimeFilterTopology(shuffle, compiled, nil))
	})

	t.Run("scalar predicate producer must share the consumer CN", func(t *testing.T) {
		scalar := makeRightSingleTopologyPlan(tag)
		scalar.Nodes[1].IsRightJoin = false
		scalar.Nodes[1].RuntimeFilterBuildList[0].ScalarPredicate = true
		consumer := makeRuntimeFilterConsumerScope(tag, cn2)
		otherSpec := &plan.RuntimeFilterSpec{Tag: 99}
		consumer.DataSource.RuntimeFilterSpecs = append(
			consumer.DataSource.RuntimeFilterSpecs, otherSpec)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateLocalRuntimeFilterTopology(scalar, compiled, []*Scope{consumer}))
		require.Equal(t, []*plan.RuntimeFilterSpec{otherSpec},
			consumer.DataSource.RuntimeFilterSpecs,
			"an unreachable scalar consumer must keep the no-filter fallback")
		require.Len(t, consumer.DataSource.node.RuntimeFilterProbeList, 1,
			"physical fallback must not mutate the reusable logical plan")
		require.Nil(t, build.RuntimeFilterSpec,
			"the unreachable producer must not send a current-CN message")
	})

	t.Run("colocated scalar predicate keeps the runtime filter", func(t *testing.T) {
		scalar := makeRightSingleTopologyPlan(tag)
		scalar.Nodes[1].IsRightJoin = false
		scalar.Nodes[1].RuntimeFilterBuildList[0].ScalarPredicate = true
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer, build := makeRuntimeFilterProducerScope(tag, cn1)
		defer build.Release()
		consumer.PreScopes = []*Scope{producer}

		require.NoError(t, validateLocalRuntimeFilterTopology(scalar, compiled, []*Scope{consumer}))
		require.Len(t, consumer.DataSource.RuntimeFilterSpecs, 1)
		require.NotNil(t, build.RuntimeFilterSpec)
	})

	t.Run("scalar predicate keeps one complete producer per CN", func(t *testing.T) {
		scalar := makeRightSingleTopologyPlan(tag)
		scalar.Nodes[1].IsRightJoin = false
		scalar.Nodes[1].RuntimeFilterBuildList[0].ScalarPredicate = true
		consumer1 := makeRuntimeFilterConsumerScope(tag, cn1)
		consumer2 := makeRuntimeFilterConsumerScope(tag, cn2)
		producer1, build1 := makeRuntimeFilterProducerScope(tag, cn1)
		producer2, build2 := makeRuntimeFilterProducerScope(tag, cn2)
		defer build1.Release()
		defer build2.Release()
		consumer1.PreScopes = []*Scope{producer1, producer2}
		consumer2.PreScopes = []*Scope{producer1, producer2}

		require.NoError(t, validateLocalRuntimeFilterTopology(
			scalar, compiled, []*Scope{consumer1, consumer2}))
		require.Len(t, consumer1.DataSource.RuntimeFilterSpecs, 1)
		require.Len(t, consumer2.DataSource.RuntimeFilterSpecs, 1)
		require.NotNil(t, build1.RuntimeFilterSpec)
		require.NotNil(t, build2.RuntimeFilterSpec)
	})

	t.Run("scalar predicate rejects duplicate producer on one CN", func(t *testing.T) {
		scalar := makeRightSingleTopologyPlan(tag)
		scalar.Nodes[1].IsRightJoin = false
		scalar.Nodes[1].RuntimeFilterBuildList[0].ScalarPredicate = true
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer1, build1 := makeRuntimeFilterProducerScope(tag, cn1)
		producer2, build2 := makeRuntimeFilterProducerScope(tag, cn1)
		defer build1.Release()
		defer build2.Release()
		consumer.PreScopes = []*Scope{producer1, producer2}

		require.NoError(t, validateLocalRuntimeFilterTopology(
			scalar, compiled, []*Scope{consumer}))
		require.Empty(t, consumer.DataSource.RuntimeFilterSpecs)
		require.Nil(t, build1.RuntimeFilterSpec)
		require.Nil(t, build2.RuntimeFilterSpec)
	})

	t.Run("scalar predicate rejects producer-only CN", func(t *testing.T) {
		scalar := makeRightSingleTopologyPlan(tag)
		scalar.Nodes[1].IsRightJoin = false
		scalar.Nodes[1].RuntimeFilterBuildList[0].ScalarPredicate = true
		consumer := makeRuntimeFilterConsumerScope(tag, cn1)
		producer1, build1 := makeRuntimeFilterProducerScope(tag, cn1)
		producer2, build2 := makeRuntimeFilterProducerScope(tag, cn2)
		defer build1.Release()
		defer build2.Release()
		consumer.PreScopes = []*Scope{producer1, producer2}

		require.NoError(t, validateLocalRuntimeFilterTopology(
			scalar, compiled, []*Scope{consumer}))
		require.Empty(t, consumer.DataSource.RuntimeFilterSpecs)
		require.Nil(t, build1.RuntimeFilterSpec)
		require.Nil(t, build2.RuntimeFilterSpec)
	})
}

func TestCollectRuntimeFilterTopologyVisitsSharedScopeOnce(t *testing.T) {
	const tag int32 = 9
	cn := engine.Node{Id: "cn1", Addr: "cn1:6001"}
	consumer := makeRuntimeFilterConsumerScope(tag, cn)
	producer, build := makeRuntimeFilterProducerScope(tag, cn)
	defer build.Release()
	consumer.PreScopes = []*Scope{producer}

	topology := collectRuntimeFilterTopology([]*Scope{consumer, producer}, []int32{tag})
	require.Len(t, topology.consumers[tag], 1)
	require.Len(t, topology.producers[tag], 1)
}

func TestParallelBuildScanIsMergedBeforeRuntimeFilterHashBuild(t *testing.T) {
	const tag int32 = 10
	testCompile := NewMockCompile(t)
	testCompile.addr = "cn1:6001"
	testCompile.cnList = engine.Nodes{{Id: "cn1", Addr: testCompile.addr, Mcpu: 8}}
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	buildScan := generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.TableScan})
	buildScan.NodeInfo = engine.Node{Id: "cn1", Addr: testCompile.addr, Mcpu: 4}
	probe := generateScopeWithRootOperator(testCompile.proc, []vm.OpType{vm.HashJoin})
	probe.NodeInfo = engine.Node{Id: "cn1", Addr: testCompile.addr, Mcpu: 8}
	probe.RootOp.(*hashjoin.HashJoin).RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{{
		Tag:  tag,
		Expr: &plan.Expr{},
	}}

	testCompile.compileBuildSideForBroadcastJoin(
		&plan.Node{Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{}}},
		[]*Scope{probe},
		[]*Scope{buildScan},
	)

	require.Len(t, probe.PreScopes, 1)
	producer := probe.PreScopes[0]
	require.Equal(t, 1, producer.NodeInfo.Mcpu)
	require.NoError(t, checkScopeWithExpectedList(producer, []vm.OpType{vm.Merge, vm.HashBuild}))
	require.NoError(t, checkScopeWithExpectedList(buildScan, []vm.OpType{vm.TableScan, vm.Connector}))
	topology := collectRuntimeFilterTopology([]*Scope{probe}, []int32{tag})
	require.Len(t, topology.producers[tag], 1)
}

func BenchmarkValidateLocalRuntimeFilterTopologyNoFilter(b *testing.B) {
	qry := &plan.Query{Nodes: make([]*plan.Node, 64)}
	for i := range qry.Nodes {
		qry.Nodes[i] = &plan.Node{NodeType: plan.Node_TABLE_SCAN}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := validateLocalRuntimeFilterTopology(qry, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRequiredIVFTopologyBroadcastAndRemoteFragment(t *testing.T) {
	c, client := vectorPlacementCompile(t, engine.Nodes{{Id: "a", Addr: "a:6001"}, {Id: "b", Addr: "b:6001"}})
	moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion107)
	client.version = defines.MORPCVersion107
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	c.proc.Ctx = defines.AttachAccountId(context.Background(), 0)
	c.proc.SetMessageBoard(message.NewMessageBoard())
	const tag int32 = 17
	roots := make([]*Scope, 0, len(c.cnList))
	producers := make([]*Scope, 0, len(c.cnList))
	for i, cn := range c.cnList {
		cn.Mcpu, cn.CNCNT, cn.CNIDX = 1, 2, int32(i)
		consumer := makeRuntimeFilterConsumerScope(tag, cn)
		consumer.Proc = c.proc.NewNoContextChildProc(0)
		consumer.DataSource.node.NodeType = plan.Node_INDEX_SEARCH_SCAN
		consumer.DataSource.node.RuntimeFilterProbeList[0].MustApply = true
		consumer.DataSource.node.RuntimeFilterProbeList[0].UseMembershipFilter = true
		consumer.RootOp = table_scan.NewArgument()
		t.Cleanup(consumer.RootOp.Release)
		producer, op := makeRuntimeFilterProducerScope(tag, cn)
		op.RuntimeFilterSpec.MustApply, op.RuntimeFilterSpec.UseMembershipFilter = true, true
		t.Cleanup(op.Release)
		producer.Proc = c.proc.NewNoContextChildProc(0)
		producer.Proc.Reg.MergeReceivers = []*process.WaitRegister{process.NewPipelineEdge(1, 0)}
		consumer.PreScopes = []*Scope{producer}
		roots, producers = append(roots, consumer), append(producers, producer)
	}
	input := &Scope{NodeInfo: roots[0].NodeInfo, Proc: c.proc.NewNoContextChildProc(0)}
	_, d := constructDispatchLocalAndRemote(0, producers, input)
	d.FuncId = dispatch.SendToAllFunc
	t.Cleanup(d.Release)
	input.RootOp = d
	roots[0].PreScopes = append(roots[0].PreScopes, input)
	topology := collectRuntimeFilterTopology(roots, []int32{tag})
	require.NoError(t, requiredIVFTopologyError(tag, topology, roots))
	// Both Any and a missing remote edge would send partial domains.
	d.FuncId = dispatch.SendToAllLocalFunc
	require.Error(t, requiredIVFTopologyError(tag, topology, roots))
	d.FuncId = dispatch.SendToAnyFunc
	require.Error(t, requiredIVFTopologyError(tag, topology, roots))
	d.FuncId = dispatch.SendToAllFunc
	remoteRegs := d.RemoteRegs
	d.RemoteRegs = nil
	require.Error(t, requiredIVFTopologyError(tag, topology, roots))
	d.RemoteRegs = remoteRegs
	extra := dispatch.NewArgument()
	extra.FuncId, extra.LocalRegs = dispatch.SendToAllLocalFunc, d.LocalRegs
	t.Cleanup(extra.Release)
	extraSource := &Scope{NodeInfo: input.NodeInfo, Proc: c.proc.NewNoContextChildProc(0), RootOp: extra}
	roots[0].PreScopes = append(roots[0].PreScopes, extraSource)
	require.Error(t, requiredIVFTopologyError(tag, topology, roots), "an extra partial sender cannot seal the domain")
	roots[0].PreScopes = roots[0].PreScopes[:len(roots[0].PreScopes)-1]
	connectorOp := connector.NewArgument().WithReg(producers[0].Proc.Reg.MergeReceivers[0])
	t.Cleanup(connectorOp.Release)
	roots[0].PreScopes = append(roots[0].PreScopes, &Scope{NodeInfo: input.NodeInfo, RootOp: connectorOp})
	require.Error(t, requiredIVFTopologyError(tag, topology, roots))
	roots[0].PreScopes = roots[0].PreScopes[:len(roots[0].PreScopes)-1]
	producers[0].RootOp.(*hashbuild.HashBuild).IsShuffle = true
	require.Error(t, requiredIVFTopologyError(tag, topology, roots))
	producers[0].RootOp.(*hashbuild.HashBuild).IsShuffle = false
	roots[1].NodeInfo.CNIDX = 0
	require.Error(t, requiredIVFTopologyError(tag, topology, roots))
	roots[1].NodeInfo.CNIDX = 1
	// A separate RPC fragment can be colocated but have an unreachable board.
	saved := roots[1].PreScopes
	roots[1].PreScopes = nil
	require.Error(t, requiredIVFTopologyError(tag, topology, roots))
	roots[1].PreScopes = saved
	var required int64
	data, err := encodeRemoteScopeWithVectorProtocol(roots[1], c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion107, required)
	decoded, err := decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(decoded.release)
	require.Len(t, decoded.PreScopes, 1)
	message.SendMessage(message.RuntimeFilterMessage{Tag: tag, Typ: message.RuntimeFilter_DROP}, decoded.PreScopes[0].Proc.GetMessageBoard())
	receiver := message.NewMessageReceiver([]int32{tag}, message.MessageAddress{CnAddr: message.CURRENTCN}, decoded.Proc.GetMessageBoard())
	msgs, _, receiveErr := receiver.ReceiveMessage(false, context.Background())
	require.NoError(t, receiveErr)
	require.Len(t, msgs, 1)
	require.Equal(t, int32(message.RuntimeFilter_DROP), msgs[0].(message.RuntimeFilterMessage).Typ)
	require.NotNil(t, decoded.Proc.GetMessageBoard())
	require.Same(t, decoded.Proc.GetMessageBoard(), decoded.PreScopes[0].Proc.GetMessageBoard(), "actual RPC decoding must keep producer and reader on the same board")
}

func TestOuterPKFilterBuildMergesAllVectorPartitions(t *testing.T) {
	for _, owners := range []int{1, 2} {
		t.Run(map[int]string{1: "local row fetch", 2: "distributed row fetch"}[owners], func(t *testing.T) {
			c := NewMockCompile(t)
			c.addr = "cn1:6001"
			c.cnList = engine.Nodes{{Id: "cn1", Addr: c.addr, Mcpu: 1}, {Id: "cn2", Addr: "cn2:6001", Mcpu: 1}}
			c.execType = plan2.ExecTypeAP_MULTICN
			c.anal = &AnalyzeModule{qry: &plan.Query{}}
			const tag int32 = 23
			filter := &plan.RuntimeFilterSpec{Tag: tag, Expr: &plan.Expr{}}
			node := &plan.Node{JoinType: plan.Node_INNER, Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{}}, RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{filter}}
			candidates := make([]*Scope, 2)
			probes := make([]*Scope, owners)
			// Register cleanup before assembling the graph; scopes are never run.
			t.Cleanup(func() {
				seen := make(map[*Scope]bool)
				var release func(*Scope)
				release = func(s *Scope) {
					if s == nil || seen[s] {
						return
					}
					seen[s] = true
					for _, child := range s.PreScopes {
						release(child)
					}
					_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error { op.Release(); return nil })
				}
				for _, s := range probes {
					release(s)
				}
				for _, s := range candidates {
					release(s)
				}
			})
			for i, cn := range c.cnList {
				candidates[i] = generateScopeWithRootOperator(c.proc.NewNoContextChildProc(0), []vm.OpType{vm.TableScan})
				candidates[i].NodeInfo = cn
				if i < owners {
					probes[i] = generateScopeWithRootOperator(c.proc.NewNoContextChildProc(0), []vm.OpType{vm.HashJoin})
					probes[i].NodeInfo = cn
					probes[i].RootOp.(*hashjoin.HashJoin).RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{filter}
				}
			}
			c.compileBuildSideForBroadcastJoin(node, probes, candidates)
			complete := probes[0].PreScopes[0]
			require.Len(t, complete.PreScopes, 2, "both vector partitions must reach the same build before row-fetch filtering")
			for i, candidate := range candidates {
				require.Same(t, candidate, complete.PreScopes[i])
				require.NoError(t, checkScopeWithExpectedList(candidate, []vm.OpType{vm.TableScan, vm.Connector}))
			}
			if owners == 1 {
				require.NoError(t, checkScopeWithExpectedList(complete, []vm.OpType{vm.Merge, vm.HashBuild}))
				require.Same(t, filter, complete.RootOp.(*hashbuild.HashBuild).RuntimeFilterSpec)
			} else {
				require.NoError(t, checkScopeWithExpectedList(complete, []vm.OpType{vm.Merge, vm.Dispatch}))
				producers := make([]*Scope, 0, owners)
				for _, probe := range probes {
					producer := probe.PreScopes[len(probe.PreScopes)-1]
					require.Equal(t, probe.NodeInfo.Addr, producer.NodeInfo.Addr)
					require.Same(t, filter, producer.RootOp.(*hashbuild.HashBuild).RuntimeFilterSpec)
					producers = append(producers, producer)
				}
				require.True(t, ivfBroadcastTargets(complete.RootOp.(*dispatch.Dispatch), producers))
			}
			require.False(t, filter.MustApply, "outer PK filters retain optional PASS semantics")
		})
	}
}
