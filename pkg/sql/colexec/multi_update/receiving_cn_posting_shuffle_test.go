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

//go:build receiving_cn_acceptance

package multi_update

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestReceivingCNPostingShuffleAdmission(t *testing.T) {
	col := func(pos int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	join := func() *plan.Node {
		return &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER,
			Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{}},
			OnList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "="}, Args: []*plan.Expr{col(1), col(0)},
			}}}}}
	}
	left := &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Name: "__mo_index_secondary_fixture"}}
	right := &plan.Node{NodeType: plan.Node_SINK_SCAN}
	n := join()
	q := &plan.Query{Nodes: []*plan.Node{n, {RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{{Tag: 1}},
		RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{Tag: 2}},
		SendMsgList:            []plan.MsgHeader{{MsgTag: 3}}, RecvMsgList: []plan.MsgHeader{{MsgTag: 4}}}}}
	require.False(t, configureReceivingCNPostingShuffle(false, n, left, right, q))
	require.False(t, n.Stats.HashmapStats.Shuffle)
	require.False(t, configureReceivingCNPostingShuffle(true, n, &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Name: "user_table"}}, right, q))
	require.False(t, configureReceivingCNPostingShuffle(true, n, left, &plan.Node{NodeType: plan.Node_TABLE_SCAN}, q))
	n.OnList[0].GetF().Func.ObjName = ">"
	require.False(t, configureReceivingCNPostingShuffle(true, n, left, right, q))
	n.OnList[0].GetF().Func.ObjName = "="
	require.True(t, configureReceivingCNPostingShuffle(true, n, left, right, q))
	require.True(t, n.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(0), n.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, plan.ShuffleType_Hash, n.Stats.HashmapStats.ShuffleType)
	require.Len(t, n.RuntimeFilterBuildList, 1)
	require.Len(t, n.RuntimeFilterProbeList, 1)
	require.Equal(t, int32(5), n.RuntimeFilterBuildList[0].Tag)
	require.Equal(t, n.RuntimeFilterBuildList[0].Tag, n.RuntimeFilterProbeList[0].Tag)

	// Both an already selected shuffle and a forced one must be observed.
	t.Setenv("MO_RECEIVING_CN_TARGETED_FORCE", "1")
	t.Setenv("MO_RECEIVING_CN_FORCE_POSTING_SHUFFLE", "1")
	id := [16]byte{1}
	var stages []string
	stop, err := InstallReceivingCNProbe("shuffle", id, func(_ context.Context, event ReceivingCNEvent) error {
		stages = append(stages, event.Stage)
		return nil
	})
	require.NoError(t, err)
	defer stop()
	proc := &process.Process{Ctx: context.Background(), Base: &process.BaseProcess{TxnOperator: &receivingCNTestTxn{id: id[:]}}}
	fresh := join()
	ReceivingCNForcePostingShuffle(proc, fresh, left, right, &plan.Query{Nodes: []*plan.Node{fresh}})
	require.Equal(t, []string{"forced-posting-shuffle"}, stages)
	wrapped := &plan.Node{NodeType: plan.Node_FILTER, Children: []int32{1}}
	ReceivingCNForcePostingShuffle(proc, fresh, wrapped, right, &plan.Query{Nodes: []*plan.Node{wrapped, left}})
	require.Equal(t, []string{"forced-posting-shuffle", "planned-posting-shuffle"}, stages)
	ReceivingCNForcePostingShuffle(proc, fresh, &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Name: "user_table"}}, right, q)
	require.Len(t, stages, 2)
}
