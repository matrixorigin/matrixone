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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
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

func TestConstructRangeExpressionWithMaxValue(t *testing.T) {
	ctx := context.Background()
	sql := "create table t3 ( a int NOT NULL AUTO_INCREMENT, key_num int NOT NULL DEFAULT '0', hiredate date NOT NULL, PRIMARY KEY (a), KEY key_num (key_num) ) PARTITION BY RANGE COLUMNS(a) ( PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN(20), PARTITION p2 VALUES LESS THAN(30), PARTITION p3 VALUES LESS THAN(MAXVALUE) );"
	ast, err := mysql.ParseOne(ctx, sql, 1)
	require.NoError(t, err)

	option := ast.(*tree.CreateTable).PartitionOption
	method := option.PartBy.PType.(*tree.RangeType)

	binder := newTestPartitionBinder()

	// Test p0: a < 10
	expr0, err := binder.constructRangeExpression(ctx, option, method.ColumnList[0], 0)
	require.NoError(t, err)
	require.NotNil(t, expr0)

	// Test p1: 10 <= a < 20
	expr1, err := binder.constructRangeExpression(ctx, option, method.ColumnList[0], 1)
	require.NoError(t, err)
	require.NotNil(t, expr1)

	// Test p2: 20 <= a < 30
	expr2, err := binder.constructRangeExpression(ctx, option, method.ColumnList[0], 2)
	require.NoError(t, err)
	require.NotNil(t, expr2)

	// Test p3: a >= 30 (MAXVALUE partition)
	expr3, err := binder.constructRangeExpression(ctx, option, method.ColumnList[0], 3)
	require.NoError(t, err)
	require.NotNil(t, expr3)

	// Verify that the MAXVALUE partition expression is different from regular partitions
	// The MAXVALUE partition should generate "a >= 30" instead of "30 <= a < MAXVALUE"
	require.NotEqual(t, expr2, expr3, "MAXVALUE partition should generate different expression")
}

func TestConstructRangeExpressionWithMaxValueFirstPartition(t *testing.T) {
	ctx := context.Background()
	sql := "create table t4 ( a int NOT NULL ) PARTITION BY RANGE COLUMNS(a) ( PARTITION p0 VALUES LESS THAN(MAXVALUE) );"
	ast, err := mysql.ParseOne(ctx, sql, 1)
	require.NoError(t, err)

	option := ast.(*tree.CreateTable).PartitionOption
	method := option.PartBy.PType.(*tree.RangeType)

	binder := newTestPartitionBinder()

	// Test p0: all values (MAXVALUE partition)
	expr0, err := binder.constructRangeExpression(ctx, option, method.ColumnList[0], 0)
	require.NoError(t, err)
	require.NotNil(t, expr0)
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
