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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestEnumUnionAllInsertProof(t *testing.T) {
	storage := planpb.Type{Id: int32(types.T_enum), Enumvalues: ",x", EnumCollation: types.EnumCollationUTF8Bin}
	left, err := makeEnumOrSetDisplayValue(context.Background(), GetColExpr(storage, 10, 0))
	require.NoError(t, err)
	right, err := makeEnumOrSetDisplayValue(context.Background(), GetColExpr(storage, 20, 0))
	require.NoError(t, err)
	newBuilder := func() *QueryBuilder {
		return &QueryBuilder{
			qry: &planpb.Query{Nodes: []*planpb.Node{
				{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{10}},
				{NodeType: planpb.Node_PROJECT, BindingTags: []int32{11}, Children: []int32{0}, ProjectList: []*planpb.Expr{DeepCopyExpr(left)}},
				{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{20}},
				{NodeType: planpb.Node_PROJECT, BindingTags: []int32{21}, Children: []int32{2}, ProjectList: []*planpb.Expr{DeepCopyExpr(right)}},
				{NodeType: planpb.Node_UNION_ALL, BindingTags: []int32{30}, Children: []int32{1, 3}, ProjectList: []*planpb.Expr{GetColExpr(left.Typ, 11, 0)}},
			}},
			tag2NodeID: map[int32]int32{10: 0, 11: 1, 20: 2, 21: 3, 30: 4},
			ctxByNode:  []*BindContext{nil, {}, nil, {}, {}},
		}
	}
	expr := GetColExpr(left.Typ, 30, 0)
	b := newBuilder()
	raw, ok, err := b.materializeEnumUnionAllForInsert(expr)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int32(types.T_enum), raw.Typ.Id)
	require.Equal(t, int32(1), raw.GetCol().ColPos)
	for _, id := range []int{1, 3, 4} {
		require.Len(t, b.qry.Nodes[id].ProjectList, 2)
	}
	for name, block := range map[string]func(*QueryBuilder){
		"union distinct":   func(b *QueryBuilder) { b.qry.Nodes[4].NodeType = planpb.Node_UNION },
		"distinct branch":  func(b *QueryBuilder) { b.ctxByNode[3].isDistinct = true },
		"limited branch":   func(b *QueryBuilder) { b.qry.Nodes[3].Limit = makePlan2Int64ConstExprWithType(1) },
		"different domain": func(b *QueryBuilder) { b.qry.Nodes[3].ProjectList[0].GetF().Args[1].Typ.Enumvalues = "x," },
		"different collation": func(b *QueryBuilder) {
			b.qry.Nodes[3].ProjectList[0].GetF().Args[1].Typ.EnumCollation = types.EnumCollationGeneralCI
		},
		"text branch": func(b *QueryBuilder) { b.qry.Nodes[3].ProjectList[0] = makePlan2StringConstExprWithType("") },
	} {
		t.Run(name, func(t *testing.T) {
			b := newBuilder()
			block(b)
			before, err := b.qry.Marshal()
			require.NoError(t, err)
			_, ok, err := b.materializeEnumUnionAllForInsert(expr)
			require.NoError(t, err)
			require.False(t, ok)
			after, err := b.qry.Marshal()
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
