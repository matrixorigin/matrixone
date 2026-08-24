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

package external

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// kafkaControlNode builds a KAFKA_TB scan node: one data column plus the
// synthetic columns, with the given filter conjuncts.
func kafkaControlNode(autocommit bool, filters ...*plan.Expr) *plan.Node {
	return &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "a", ColId: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.KafkaReadStartID, ColId: catalog.KafkaReadStartIDColId,
					Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.KafkaReadSize, ColId: catalog.KafkaReadSizeColId,
					Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.KafkaReadTimeout, ColId: catalog.KafkaReadTimeoutColId,
					Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		},
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_KAFKA_TB),
			KafkaScan: &plan.KafkaScan{
				Autocommit:     autocommit,
				TimeoutSeconds: 10,
			},
		},
		FilterList: filters,
	}
}

func kafkaCtrlEq(colPos int32, colName string, val int64) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "=", Obj: int64(function.EQUAL) << 32},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: colPos, Name: colName}}},
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: val}}}},
			},
		}},
	}
}

func kafkaRowFilter() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: ">", Obj: int64(function.GREAT_THAN) << 32},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "a"}}},
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 5}}}},
			},
		}},
	}
}

func TestDeriveKafkaReadControl(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := proc.Ctx

	// all three controls set; the generating conjuncts are consumed, the
	// row-level one stays
	node := kafkaControlNode(false,
		kafkaCtrlEq(1, catalog.KafkaReadStartID, 1000),
		kafkaCtrlEq(2, catalog.KafkaReadSize, 50),
		kafkaCtrlEq(3, catalog.KafkaReadTimeout, 7),
		kafkaRowFilter(),
	)
	require.NoError(t, DeriveKafkaReadControl(ctx, node, proc))
	ks := node.ExternScan.KafkaScan
	require.True(t, ks.HasStartId)
	require.Equal(t, int64(1000), ks.StartId)
	require.Equal(t, int64(50), ks.Size)
	require.Equal(t, int64(7), ks.TimeoutSeconds)
	require.Len(t, node.FilterList, 1)
	require.Equal(t, ">", node.FilterList[0].GetF().Func.GetObjName())

	// reversed operand order works too
	node = kafkaControlNode(true, &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "=", Obj: int64(function.EQUAL) << 32},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 3}}}},
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: catalog.KafkaReadStartID}}},
			},
		}},
	})
	require.NoError(t, DeriveKafkaReadControl(ctx, node, proc))
	require.True(t, node.ExternScan.KafkaScan.HasStartId)
	require.Equal(t, int64(3), node.ExternScan.KafkaScan.StartId)
	require.Empty(t, node.FilterList)

	// autocommit=false without a start id is an error
	node = kafkaControlNode(false, kafkaRowFilter())
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), "__mo_read_start_id")

	// autocommit=true defaults are fine
	node = kafkaControlNode(true)
	require.NoError(t, DeriveKafkaReadControl(ctx, node, proc))
	require.False(t, node.ExternScan.KafkaScan.HasStartId)

	// -1 (latest/earliest) is legal, -2 is not
	node = kafkaControlNode(false, kafkaCtrlEq(1, catalog.KafkaReadStartID, -1))
	require.NoError(t, DeriveKafkaReadControl(ctx, node, proc))
	node = kafkaControlNode(false, kafkaCtrlEq(1, catalog.KafkaReadStartID, -2))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), ">= -1")

	// size must be positive, timeout non-negative
	node = kafkaControlNode(true, kafkaCtrlEq(2, catalog.KafkaReadSize, 0))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), "positive")
	node = kafkaControlNode(true, kafkaCtrlEq(3, catalog.KafkaReadTimeout, -5))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), ">= 0")

	// contradictory values are an error; a repeated identical value is not
	node = kafkaControlNode(true,
		kafkaCtrlEq(2, catalog.KafkaReadSize, 5), kafkaCtrlEq(2, catalog.KafkaReadSize, 6))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), "contradictory")
	node = kafkaControlNode(true,
		kafkaCtrlEq(2, catalog.KafkaReadSize, 5), kafkaCtrlEq(2, catalog.KafkaReadSize, 5))
	require.NoError(t, DeriveKafkaReadControl(ctx, node, proc))
	require.Equal(t, int64(5), node.ExternScan.KafkaScan.Size)

	// a REAL user column that merely shares the name is not a control
	node = kafkaControlNode(true)
	node.TableDef.Cols[1].ColId = 42 // not the reserved ColId
	node.FilterList = []*plan.Expr{kafkaCtrlEq(1, catalog.KafkaReadStartID, 9)}
	require.NoError(t, DeriveKafkaReadControl(ctx, node, proc))
	require.False(t, node.ExternScan.KafkaScan.HasStartId)
	require.Len(t, node.FilterList, 1)
}
