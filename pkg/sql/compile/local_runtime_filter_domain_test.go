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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestBindLocalRuntimeFilterDomainTopology(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Compile, *plan.Query, *Scope, *Scope)
		want bool
	}{
		{name: "complete independent local source", want: true},
		{name: "prepared", edit: func(c *Compile, _ *plan.Query, _, _ *Scope) { c.isPrepare = true }},
		{name: "multiple CNs", edit: func(c *Compile, _ *plan.Query, _, _ *Scope) { c.cnList = append(c.cnList, engine.Node{Addr: "other"}) }},
		{name: "remote source", edit: func(_ *Compile, _ *plan.Query, s, _ *Scope) { s.IsRemote = true }},
		{name: "source has prerequisite", edit: func(_ *Compile, _ *plan.Query, s, p *Scope) { s.PreScopes = []*Scope{p} }},
		{name: "source has a shuffle membership wait", edit: func(_ *Compile, _ *plan.Query, s, _ *Scope) {
			sh := shuffle.NewArgument()
			sh.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 1}
			t.Cleanup(sh.Release)
			sh.AppendChild(s.RootOp)
			s.RootOp = sh
		}},
		{name: "normal scope does not start prerequisites", edit: func(_ *Compile, _ *plan.Query, _, p *Scope) { p.Magic = Normal }},
		{name: "stale annotation", edit: func(_ *Compile, q *plan.Query, _, _ *Scope) {
			q.LocalRuntimeFilterDomains[0].SourceColumnName = "missing"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetMessageBoard(message.NewMessageBoard())
			t.Cleanup(proc.Free)
			typ := plan.Type{Id: int32(types.T_int64)}
			col := func(side int32) *plan.Expr { return plan2.GetColExpr(typ, side, 0) }
			scan := func(id int32, rows float64) *plan.Node {
				return &plan.Node{NodeId: id, NodeType: plan.Node_TABLE_SCAN, ProjectList: []*plan.Expr{col(0)},
					TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "key", Typ: typ}}},
					Stats:    &plan.Stats{Outcnt: rows, TableCnt: rows}}
			}
			probeNode, sourceNode := scan(0, 10000), scan(1, 1000)
			sourceNode.TableDef.Pkey = &plan.PrimaryKeyDef{PkeyColName: "key", Names: []string{"key"}}
			sourceNode.Stats.Outcnt = 4
			sourceNode.FilterList = []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}}
			q := &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{2}, Nodes: []*plan.Node{probeNode, sourceNode,
				{NodeId: 2, NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{0, 1}, ProjectList: []*plan.Expr{col(0)},
					OnList: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "=", Obj: function.EncodeOverloadID(int32(function.EQUAL), 0)}, Args: []*plan.Expr{col(0), col(1)},
					}}}},
				},
			}}
			q.LocalRuntimeFilterDomains = plan2.LocalRuntimeFilterDomains(q, func(*plan.Node, string) float64 { return 1000 })
			require.Len(t, q.LocalRuntimeFilterDomains, 1)
			sourceOp, probeOp := table_scan.NewArgument(), table_scan.NewArgument()
			t.Cleanup(sourceOp.Release)
			t.Cleanup(probeOp.Release)
			sourceOp.SetAnalyzeControl(1, true)
			probeOp.SetAnalyzeControl(0, true)
			cn := engine.Node{Addr: "local", Mcpu: 2}
			source := &Scope{Magic: Normal, NodeInfo: cn, Proc: proc, RootOp: sourceOp, DataSource: &Source{node: sourceNode}}
			probe := &Scope{Magic: Remote, NodeInfo: cn, Proc: proc, RootOp: probeOp, DataSource: &Source{node: probeNode}, PreScopes: []*Scope{source}}
			c := &Compile{addr: "local", cnList: engine.Nodes{cn}, proc: proc, execType: plan2.ExecTypeAP_ONECN}
			t.Cleanup(func() {
				for _, d := range c.localRuntimeDomains {
					d.Close()
				}
			})
			if tc.edit != nil {
				tc.edit(c, q, source, probe)
			}
			c.bindLocalRuntimeFilterDomains(q, []*Scope{probe})
			if tc.want {
				require.Len(t, sourceOp.LocalDomainBuilds, 1)
				require.Len(t, probeOp.LocalDomainProbes, 1)
				require.Same(t, sourceOp.LocalDomainBuilds[0].Domain, probeOp.LocalDomainProbes[0].Domain)
			} else {
				require.Empty(t, sourceOp.LocalDomainBuilds)
				require.Empty(t, probeOp.LocalDomainProbes)
			}
		})
	}
}
