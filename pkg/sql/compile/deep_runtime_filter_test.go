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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestDeepRuntimeFilterPhysicalAdmission(t *testing.T) {
	const tag int32 = 7
	for _, tc := range []struct {
		name          string
		edit          func(*Compile, *Scope, *Scope)
		keep          bool
		unsafeLogical bool
	}{
		{name: "complete local AP producer", keep: true},
		{name: "unproved logical dependency order", unsafeLogical: true},
		{name: "remote placement", edit: func(_ *Compile, _, p *Scope) { p.NodeInfo.Addr = "remote:6001" }},
		{name: "different board", edit: func(_ *Compile, _, p *Scope) {
			p.Proc = newPlanTestProcess(t)
			p.Proc.SetMessageBoard(message.NewMessageBoard())
		}},
		{name: "lazy startup", edit: func(_ *Compile, c, _ *Scope) { c.LazyPreScopes = true }},
		{name: "unstarted prescope", edit: func(_ *Compile, c, _ *Scope) { c.Magic = Normal }},
		{name: "serial execution", edit: func(c *Compile, _, _ *Scope) { c.execType = plan2.ExecTypeTP }},
		{name: "unproved complete input", edit: func(c *Compile, _, _ *Scope) { c.completeDeepRuntimeFilterBuilds = nil }},
		{name: "cloned publisher", edit: func(_ *Compile, _, p *Scope) {
			scan := table_scan.NewArgument()
			t.Cleanup(scan.Release)
			p.RootOp.AppendChild(scan)
			p.NodeInfo.Mcpu = 4
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Compile{proc: newPlanTestProcess(t), addr: "local:6001", execType: plan2.ExecTypeAP_ONECN,
				deepRuntimeFilterTags: map[int32]bool{tag: true}}
			c.proc.SetMessageBoard(message.NewMessageBoard())
			cn := engine.Node{Id: "local", Addr: c.addr, Mcpu: 1}
			consumer := makeRuntimeFilterConsumerScope(tag, cn)
			consumer.Proc = c.proc.NewNoContextChildProc(0)
			logical := consumer.DataSource.node
			producer, build := makeRuntimeFilterProducerScope(tag, cn)
			t.Cleanup(build.Release)
			producer.Proc = c.proc.NewNoContextChildProc(0)
			consumer.PreScopes = []*Scope{producer}
			c.noteCompleteBroadcastRuntimeFilterBuild(build)
			if tc.edit != nil {
				tc.edit(c, consumer, producer)
			}
			c.validateDeepRuntimeFilterTopology([]int32{tag}, !tc.unsafeLogical, []*Scope{consumer})
			if tc.keep {
				require.Len(t, consumer.DataSource.RuntimeFilterSpecs, 1)
				require.NotNil(t, build.RuntimeFilterSpec)
				return
			}
			require.Empty(t, consumer.DataSource.RuntimeFilterSpecs)
			require.Empty(t, consumer.DataSource.node.RuntimeFilterProbeList)
			require.Nil(t, build.RuntimeFilterSpec)
			require.Len(t, logical.RuntimeFilterProbeList, 1, "plan reuse retains the logical pair")
			// Revalidation cannot restore the pair; the source view remains
			// disabled until a fresh compilation from the logical template.
			c.validateDeepRuntimeFilterTopology([]int32{tag}, true, []*Scope{consumer})
			require.Empty(t, collectRuntimeFilterTopology([]*Scope{consumer}, []int32{tag}).consumers[tag])
			require.Nil(t, build.RuntimeFilterSpec)
		})
	}
}

// compileBuildSideForBroadcastJoin is the completeness owner. Exercise that
// real path with a parallel source, rather than labelling a partial map safe.
func TestDeepRuntimeFilterCompleteBroadcastBuild(t *testing.T) {
	const tag int32 = 7
	c := NewMockCompile(t)
	c.addr, c.execType = "local:6001", plan2.ExecTypeAP_ONECN
	c.anal = &AnalyzeModule{qry: &plan.Query{}}
	c.cnList = engine.Nodes{{Id: "local", Addr: c.addr, Mcpu: 4}}
	c.proc.SetMessageBoard(message.NewMessageBoard())
	c.deepRuntimeFilterTags = map[int32]bool{tag: true}
	input := newScope(Remote)
	var probe *Scope
	t.Cleanup(func() {
		if probe == nil || !ivfFragmentContains(probe, input) {
			input.FreeOperator(c)
			input.release()
		}
		if probe != nil {
			probe.FreeOperator(c)
			probe.release()
		}
		c.proc.Free()
	})
	input.Proc = c.proc.NewNoContextChildProc(0)
	input.setRootOperator(table_scan.NewArgument())
	input.NodeInfo = engine.Node{Id: "local", Addr: c.addr, Mcpu: 4}
	probe = newScope(Remote)
	probe.Proc = c.proc.NewNoContextChildProc(0)
	join := hashjoin.NewArgument()
	join.EqConds = [][]*plan.Expr{nil, nil}
	probe.setRootOperator(join)
	probe.NodeInfo = engine.Node{Id: "local", Addr: c.addr, Mcpu: 4}
	probe.DataSource = makeRuntimeFilterConsumerScope(tag, probe.NodeInfo).DataSource
	probe.RootOp.(*hashjoin.HashJoin).RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{{Tag: tag, Expr: &plan.Expr{}}}
	c.compileBuildSideForBroadcastJoin(&plan.Node{Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{}}}, []*Scope{probe}, []*Scope{input})
	producer := probe.PreScopes[0]
	require.Equal(t, 1, producer.NodeInfo.Mcpu)
	count := 0
	_ = vm.HandleAllOp(producer.RootOp, func(_ vm.Operator, op vm.Operator) error {
		if c.completeDeepRuntimeFilterBuilds[op] {
			count++
		}
		return nil
	})
	require.Equal(t, 1, count)
	c.validateDeepRuntimeFilterTopology([]int32{tag}, true, []*Scope{probe})
	require.Len(t, probe.DataSource.RuntimeFilterSpecs, 1)
}

func TestDeepRuntimeFilterCompilesAcrossInnerShuffle(t *testing.T) {
	const tag int32 = 7
	c := newLazyUnionAllTestCompile(t)
	c.isPrepare = false
	c.execType, c.addr = plan2.ExecTypeAP_ONECN, "local:6001"
	c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 4}}
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	c.proc.SetMessageBoard(message.NewMessageBoard())
	col := plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 0)
	nodes := make([]*plan.Node, 5)
	for _, id := range []int32{0, 1, 3} {
		nodes[id] = &plan.Node{NodeId: id, NodeType: plan.Node_TABLE_SCAN,
			TableDef:    &plan.TableDef{Name: "t", Cols: []*plan.ColDef{{Name: "k", Typ: col.Typ}}},
			ObjRef:      &plan.ObjectRef{SchemaName: "testdb", ObjName: "t"},
			ProjectList: []*plan.Expr{plan2.DeepCopyExpr(col)}, Stats: &plan.Stats{Dop: 4}}
	}
	lower := newShuffleJoinTestNode(4)
	lower.NodeId, lower.Children = 2, []int32{1, 0}
	lower.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
	retained := plan2.DeepCopyExpr(col)
	retained.GetCol().RelPos = 1 // target scan is the lower join's build side
	lower.ProjectList = []*plan.Expr{retained}
	nodes[2] = lower
	outer := plan2.DeepCopyNode(lower)
	outer.NodeId, outer.Children = 4, []int32{2, 3}
	outer.Stats.HashmapStats = &plan.HashMapStats{}
	outer.ProjectList = []*plan.Expr{plan2.DeepCopyExpr(col)}
	outer.SendMsgList[0].MsgTag = 2
	buildExpr := plan2.GetColExpr(col.Typ, -1, 0)
	outer.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{{Tag: tag, BuildExpr: buildExpr,
		UpperLimit: 3, ProbeType: &col.Typ, KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1}}
	nodes[0].RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: tag, Expr: plan2.DeepCopyExpr(col)}}
	nodes[4] = outer
	query := &plan.Query{StmtType: plan.Query_SELECT, Nodes: nodes, Steps: []int32{4}}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
	c.initAnalyzeModule(query)
	tags, safe := plan2.DeepOrdinaryRuntimeFilterTags(query)
	require.True(t, safe)
	require.Equal(t, []int32{tag}, tags)
	c.deepRuntimeFilterTags = map[int32]bool{tag: true}
	var scopes []*Scope
	t.Cleanup(func() {
		for _, scope := range scopes {
			scope.FreeOperator(c)
			scope.release()
		}
		c.proc.Free()
	})
	var err error
	scopes, err = c.compileProductRegion(0, 4, nodes, false, query)
	require.NoError(t, err)
	c.validateDeepRuntimeFilterTopology(tags, safe, scopes)
	physical := collectRuntimeFilterTopology(scopes, tags)
	require.Len(t, physical.producers[tag], 1, "complete broadcast survives lower shuffle")
	require.NotEmpty(t, physical.consumers[tag], "the descendant scan must retain its matched wait")
}
