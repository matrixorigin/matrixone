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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// kafkaTestNode builds a KAFKA_TB scan node with the control columns.
func kafkaTestNode(autocommit bool, filters ...*plan.Expr) *plan.Node {
	return &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "a", ColId: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.KafkaReadStartID, ColId: catalog.KafkaReadStartIDColId,
					Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_KAFKA_TB),
			TbColToDataCol: map[string]int32{"a": 0},
			KafkaScan: &plan.KafkaScan{
				Brokers: "h:9092", Topic: "t", Group: "g", Autocommit: autocommit,
				Format: "csv", Separator: ",", TimeoutSeconds: 10,
			},
		},
		FilterList: filters,
	}
}

func kafkaStartEq(val int64) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "=", Obj: int64(function.EQUAL) << 32},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: catalog.KafkaReadStartID}}},
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: val}}}},
			},
		}},
	}
}

func TestCompileKafkaScan(t *testing.T) {
	newCompile := func() *Compile {
		c := NewMockCompile(t)
		c.addr = "local:6001"
		c.anal = &AnalyzeModule{qry: &plan.Query{}}
		return c
	}

	// the control conjunct resolves the read position and is consumed
	c := newCompile()
	node := kafkaTestNode(false, kafkaStartEq(1000))
	scopes, err := c.compileKafkaScan(node, true)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	require.Empty(t, node.FilterList)
	op, ok := scopes[0].RootOp.(*external.External)
	require.True(t, ok)
	require.True(t, op.Es.KafkaScan.HasStartId)
	require.Equal(t, int64(1000), op.Es.KafkaScan.StartId)
	require.NotNil(t, op.Es.Extern)

	// autocommit=false without a start id fails at compile, before any dial
	c = newCompile()
	node = kafkaTestNode(false)
	_, err = c.compileKafkaScan(node, true)
	require.ErrorContains(t, err, "__mo_read_start_id")

	// missing scan metadata is a clean error
	c = newCompile()
	_, err = c.compileKafkaScan(&plan.Node{ExternScan: &plan.ExternScan{}}, true)
	require.ErrorContains(t, err, "missing scan metadata")
}

// TestShuffleStageNodesKeepKafkaScanCN mirrors the foreign-scan rule: the
// session-pinned kafka source keeps its CN in the shuffle receiver stage set.
func TestShuffleStageNodesKeepKafkaScanCN(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-session:6001"
	c.anal = &AnalyzeModule{qry: &plan.Query{}}

	node := kafkaTestNode(true)
	scopes, err := c.compileKafkaScan(node, true)
	require.NoError(t, err)
	require.Len(t, scopes, 1)

	c.cnList = engine.Nodes{
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}
	stageNodes, local := c.shuffleJoinStageNodes(scopes, nil)
	require.True(t, local, "kafka scan must count as a local pinned source")
	var hasSession bool
	for _, n := range stageNodes {
		if n.Addr == "cn-session:6001" {
			hasSession = true
		}
	}
	require.True(t, hasSession, "session CN must be in the receiver stage set")
}
