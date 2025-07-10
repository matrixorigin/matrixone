// Copyright 2021-2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBuildPartitionDefs(t *testing.T) {
	ctx := context.Background()
	sql := "create table t1 (a int) partition by list columns(a) (partition p1 values in (1, 2));"
	ast, err := mysql.ParseOne(ctx, sql, 1)
	require.NoError(t, err)

	option := ast.(*tree.CreateTable).PartitionOption

	binder := newTestPartitionBinder()

	_, err = binder.buildPartitionDefs(ctx, option)
	require.NoError(t, err)
}

func TestConstructListExpression(t *testing.T) {
	ctx := context.Background()
	sql := "create table t1 (a int) partition by list columns(a) (partition p1 values in (1, 2));"
	ast, err := mysql.ParseOne(ctx, sql, 1)
	require.NoError(t, err)

	option := ast.(*tree.CreateTable).PartitionOption
	method := option.PartBy.PType.(*tree.ListType)

	binder := newTestPartitionBinder()

	_, err = binder.constructListExpression(ctx, option, method.ColumnList[0], 0)
	require.NoError(t, err)
}

func TestConstructRangeExpression(t *testing.T) {
	ctx := context.Background()
	sql := "create table t1 (a int) partition by range columns(a) (partition p1 values less than (1));"
	ast, err := mysql.ParseOne(ctx, sql, 1)
	require.NoError(t, err)

	option := ast.(*tree.CreateTable).PartitionOption
	method := option.PartBy.PType.(*tree.RangeType)

	binder := newTestPartitionBinder()

	_, err = binder.constructRangeExpression(ctx, option, method.ColumnList[0], 0)
	require.NoError(t, err)
}

func TestConstructHashExpression(t *testing.T) {
	ctx := context.Background()
	sql := "create table t1 (a int) partition by hash(a) partitions 2;"
	ast, err := mysql.ParseOne(ctx, sql, 1)
	require.NoError(t, err)

	option := ast.(*tree.CreateTable).PartitionOption
	method := option.PartBy.PType.(*tree.HashType)
	num := option.PartBy.Num

	binder := newTestPartitionBinder()

	_, err = binder.constructHashExpression(ctx, num, method, 0)
	require.NoError(t, err)

	_, err = binder.constructHashExpression(ctx, num, method, 1)
	require.NoError(t, err)
}

func newTestPartitionBinder() *PartitionBinder {
	mock := NewMockOptimizer(false)
	builder := NewQueryBuilder(plan.Query_SELECT, mock.CurrentContext(), false, false)
	bindContext := NewBindContext(builder, nil)

	nodeID := builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       nil,
		ObjRef:      nil,
		TableDef:    newTestTableDef(1, []string{"a"}, []types.T{types.T_int32}),
		BindingTags: []int32{builder.genNewTag()},
	}, bindContext)

	err := builder.addBinding(nodeID, tree.AliasClause{}, bindContext)
	if err != nil {
		panic(err)
	}

	return NewPartitionBinder(builder, bindContext)
}

func newTestTableDef(
	id uint64,
	columns []string,
	types []types.T,
) *TableDef {
	def := &TableDef{
		TblId: id,
	}

	for idx, col := range columns {
		def.Cols = append(
			def.Cols,
			&ColDef{
				Name: col,
				Typ:  Type{Id: int32(types[idx])},
			},
		)
	}
	return def
}
