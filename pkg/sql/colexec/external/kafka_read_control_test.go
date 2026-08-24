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

// TestDeriveKafkaControlBoundsAndTypes covers every accepted integer type in
// the control evaluator and the overflow caps (a huge timeout would overflow
// time.Duration into an instantly-expired poll; MaxInt64 start would wrap on
// +1 and read from the wrong position).
func TestDeriveKafkaControlBoundsAndTypes(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := proc.Ctx

	typedEq := func(colPos int32, colName string, typ types.T, lit *plan.Literal) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "=", Obj: int64(function.EQUAL) << 32},
				Args: []*plan.Expr{
					{Typ: plan.Type{Id: int32(types.T_int64)},
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: colPos, Name: colName}}},
					{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Lit{Lit: lit}},
				},
			}},
		}
	}

	// every integer literal type resolves
	for _, tc := range []struct {
		typ types.T
		lit *plan.Literal
	}{
		{types.T_int8, &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 7}}},
		{types.T_int16, &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 7}}},
		{types.T_int32, &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 7}}},
		{types.T_int64, &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 7}}},
		{types.T_uint8, &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 7}}},
		{types.T_uint16, &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 7}}},
		{types.T_uint32, &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 7}}},
		{types.T_uint64, &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 7}}},
	} {
		node := kafkaControlNode(true, typedEq(2, catalog.KafkaReadSize, tc.typ, tc.lit))
		require.NoError(t, DeriveKafkaReadControl(ctx, node, proc), tc.typ.String())
		require.Equal(t, int64(7), node.ExternScan.KafkaScan.Size, tc.typ.String())
	}

	// a string value is rejected with a clear error
	node := kafkaControlNode(true, typedEq(2, catalog.KafkaReadSize, types.T_varchar,
		&plan.Literal{Value: &plan.Literal_Sval{Sval: "ten"}}))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), "integer")

	// overflow caps
	node = kafkaControlNode(true, kafkaCtrlEq(3, catalog.KafkaReadTimeout, int64(1)<<40))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), "seconds")
	node = kafkaControlNode(false, kafkaCtrlEq(1, catalog.KafkaReadStartID, int64(1)<<62+1))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), "out of range")
	node = kafkaControlNode(true, kafkaCtrlEq(2, catalog.KafkaReadSize, int64(1)<<62+1))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), "out of range")
	// huge uint64 literal
	node = kafkaControlNode(true, typedEq(2, catalog.KafkaReadSize, types.T_uint64,
		&plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1 << 63}}))
	require.ErrorContains(t, DeriveKafkaReadControl(ctx, node, proc), "out of range")
}

// TestDeriveKafkaControlFromLastMessageID: server-side exactly-once chaining
// — the LAST_KAFKA_MESSAGE_ID() builtin is a valid control value, resolved
// against this session's state at compile time; NULL (no scan yet) generates
// nothing and the autocommit=false requirement then fires.
func TestDeriveKafkaControlFromLastMessageID(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := proc.Ctx

	lastIDCall := func() *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "last_kafka_message_id",
					Obj: int64(function.LAST_KAFKA_MESSAGE_ID) << 32},
			}},
		}
	}
	ctrlEqExpr := func(rhs *plan.Expr) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "=", Obj: int64(function.EQUAL) << 32},
				Args: []*plan.Expr{
					{Typ: plan.Type{Id: int32(types.T_int64)},
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: catalog.KafkaReadStartID}}},
					rhs,
				},
			}},
		}
	}

	// a session with a recorded last id: the builtin resolves to it
	ses := &fakeKafkaSession{}
	ses.SetLastKafkaMessageID(4711)
	proc.Session = ses
	node := kafkaControlNode(false, ctrlEqExpr(lastIDCall()))
	require.NoError(t, DeriveKafkaReadControl(ctx, node, proc))
	require.True(t, node.ExternScan.KafkaScan.HasStartId)
	require.Equal(t, int64(4711), node.ExternScan.KafkaScan.StartId)
	require.Empty(t, node.FilterList)

	// no scan yet: NULL generates nothing; autocommit=false then errors
	proc2 := testutil.NewProc(t)
	proc2.Session = &fakeKafkaSession{}
	node = kafkaControlNode(false, ctrlEqExpr(lastIDCall()))
	require.ErrorContains(t, DeriveKafkaReadControl(proc2.Ctx, node, proc2), "__mo_read_start_id")
}
