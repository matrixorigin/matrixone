// Copyright 2022 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSingleDDLPartition(t *testing.T) {
	//sql := `CREATE TABLE k1 (
	//			id INT NOT NULL PRIMARY KEY,
	//			name VARCHAR(20)
	//		)
	//		PARTITION BY KEY()
	//		PARTITIONS 2;`

	//sql := `CREATE TABLE k1 (
	//			id INT NOT NULL,
	//			name VARCHAR(20),
	//			sal DOUBLE,
	//			PRIMARY KEY (id, name)
	//		)
	//		PARTITION BY KEY()
	//		PARTITIONS 2;`

	sql := `CREATE TABLE k1 (
				id INT NOT NULL,
				name VARCHAR(20),
				UNIQUE KEY (id)
			)
			PARTITION BY KEY()
			PARTITIONS 2;`

	//sql := `CREATE TABLE quarterly_report_status (
	//		report_id INT NOT NULL,
	//		report_status VARCHAR(20) NOT NULL,
	//		report_updated TIMESTAMP NOT NULL
	//	)
	//		PARTITION BY RANGE ( UNIX_TIMESTAMP(report_updated) ) (
	//		PARTITION p0 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-01-01 00:00:00') ),
	//		PARTITION p1 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-04-01 00:00:00') ),
	//		PARTITION p2 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-07-01 00:00:00') ),
	//		PARTITION p3 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-10-01 00:00:00') ),
	//		PARTITION p4 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-01-01 00:00:00') ),
	//		PARTITION p5 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-04-01 00:00:00') ),
	//		PARTITION p6 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-07-01 00:00:00') ),
	//		PARTITION p7 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-10-01 00:00:00') ),
	//		PARTITION p8 VALUES LESS THAN ( UNIX_TIMESTAMP('2010-01-01 00:00:00') ),
	//		PARTITION p9 VALUES LESS THAN (MAXVALUE)
	//	);`

	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

func TestPartitionError(t *testing.T) {
	sqls := []string{
		`create temporary table p_table_non(col1 int,col2 varchar(25))partition by key(col2)partitions 4;`,
	}

	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}
}

func TestPartitioningKeysUniqueKeys(t *testing.T) {
	sqls := []string{
		`CREATE TABLE t1 (
			col1 INT  NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col2)
		)
		PARTITION BY KEY()
		PARTITIONS 4;`,

		`CREATE TABLE t1 (
				col1 INT NOT NULL,
				col2 DATE NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				UNIQUE KEY (col1, col2, col3)
			)
			PARTITION BY HASH(col3)
			PARTITIONS 4;`,

		`CREATE TABLE t2 (
				col1 INT NOT NULL,
				col2 DATE NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				UNIQUE KEY (col1, col3)
			)
			PARTITION BY HASH(col1 + col3)
			PARTITIONS 4;`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col2, col3)
		)
			PARTITION BY KEY(col3)
			PARTITIONS 4;`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col3)
		)
			PARTITION BY KEY(col1,col3)
			PARTITIONS 4;`,

		`CREATE TABLE t3 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col2, col3),
			UNIQUE KEY (col3)
		)
			PARTITION BY HASH(col3)
			PARTITIONS 4;`,

		`CREATE TABLE t3 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col2, col3),
			UNIQUE KEY (col3)
		)
			PARTITION BY KEY(col3)
			PARTITIONS 4;`,

		`CREATE TABLE t4 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL UNIQUE,
			col4 INT NOT NULL
		)
			PARTITION BY KEY(col3)
			PARTITIONS 4;`,
	}

	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		t.Log(sql)
		logicPlan, err := buildSingleStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

func TestPartitioningKeysUniqueKeysError(t *testing.T) {
	sqls := []string{
		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col2)
		)
		PARTITION BY HASH(col3)
		PARTITIONS 4;`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1),
			UNIQUE KEY (col3)
		)
		PARTITION BY HASH(col1 + col3)
		PARTITIONS 4;`,

		`CREATE TABLE t1 (
			col1 INT UNIQUE NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL
		)
		PARTITION BY HASH(col3)
		PARTITIONS 4;`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1),
			UNIQUE KEY (col3)
		)
		PARTITION BY KEY(col1,col3)
		PARTITIONS 4;`,

		`CREATE TABLE t3 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col2),
			UNIQUE KEY (col3)
		)
			PARTITION BY HASH(col1 + col3)
			PARTITIONS 4;`,

		`CREATE TABLE t3 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col2),
			UNIQUE KEY (col3)
		)
			PARTITION BY KEY(col1, col3)
			PARTITIONS 4;`,
		// should show error:Field in list of fields for partition function not found in table
		`CREATE TABLE k1 (
			id INT NOT NULL,
			name VARCHAR(20),
			sal DOUBLE
		)
		PARTITION BY KEY()
		PARTITIONS 2;`,

		`CREATE TABLE t6 (
		col1 INT NOT NULL,
		col2 DATE NOT NULL,
		col3 INT NOT NULL UNIQUE,
		col4 INT NOT NULL
	   )
		PARTITION BY KEY(col1)
		PARTITIONS 4;`,

		`CREATE TABLE t7 (
		col1 INT NOT NULL,
		col2 DATE NOT NULL,
		col3 INT NOT NULL UNIQUE,
		col4 INT NOT NULL
	   )
		PARTITION BY HASH(col4)
		PARTITIONS 4;`,
	}

	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}
}

func TestPartitioningKeysPrimaryKeys(t *testing.T) {
	sqls := []string{
		`CREATE TABLE t7 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col2)
		)
		PARTITION BY HASH(col1 + YEAR(col2))
		PARTITIONS 4;`,

		`CREATE TABLE t8 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col2, col4),
			UNIQUE KEY(col2, col1)
		)
		PARTITION BY HASH(col1 + YEAR(col2))
		PARTITIONS 4;`,

		`CREATE TABLE t7 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col2)
		)
		PARTITION BY KEY(col1,col2)
		PARTITIONS 4;`,

		`CREATE TABLE t8 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col2, col4),
			UNIQUE KEY(col2, col1)
		)
		PARTITION BY KEY(col1,col2)
		PARTITIONS 4;`,

		`CREATE TABLE k1 (
			id INT NOT NULL,
			name VARCHAR(20),
			sal DOUBLE,
			PRIMARY KEY (id, name),
			unique key (id)
		)
		PARTITION BY KEY(id)
		PARTITIONS 2;`,
	}

	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		t.Log(sql)
		logicPlan, err := buildSingleStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

func TestPartitioningKeysPrimaryKeysError(t *testing.T) {
	sqls := []string{
		`CREATE TABLE t5 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col2)
		)
		PARTITION BY HASH(col3)
		PARTITIONS 4;`,

		`CREATE TABLE t6 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col3),
			UNIQUE KEY(col2)
		)
		PARTITION BY HASH( YEAR(col2) )
		PARTITIONS 4;`,

		`CREATE TABLE t5 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col2)
		)
		PARTITION BY KEY(col3)
		PARTITIONS 4;`,

		`CREATE TABLE t6 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col3),
			UNIQUE KEY(col2)
		)
		PARTITION BY KEY(col2)
		PARTITIONS 4;`,
	}

	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}
}

// A UNIQUE INDEX must include all columns in the table's partitioning function
func TestPartitionKeysShouldShowError(t *testing.T) {
	sqls := []string{
		`CREATE TABLE t4 (
				col1 INT NOT NULL,
				col2 INT NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				UNIQUE KEY (col1, col3),
				UNIQUE KEY (col2, col4)
			)
			PARTITION BY KEY(col1, col3)
			PARTITIONS 2;`,

		`CREATE TABLE t4 (
				col1 INT NOT NULL,
				col2 INT NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				UNIQUE KEY (col1, col3),
				UNIQUE KEY (col2, col4)
			)
			PARTITION BY HASH(col1 + col3)
			PARTITIONS 2;`,

		`CREATE TABLE t4 (
				col1 INT NOT NULL,
				col2 INT NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				UNIQUE KEY (col1, col3),
				UNIQUE KEY (col2, col4)
			)
			PARTITION BY RANGE (col1 + col3) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21)
			);`,

		`CREATE TABLE t4 (
				col1 INT NOT NULL,
				col2 INT NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				UNIQUE KEY (col1, col3),
				UNIQUE KEY (col2, col4)
			)
			PARTITION BY RANGE COLUMNS(col1, col3) PARTITIONS 4 (
			PARTITION p0 VALUES LESS THAN (10,5),
			PARTITION p1 VALUES LESS THAN (20,10),
			PARTITION p2 VALUES LESS THAN (50,20),
			PARTITION p3 VALUES LESS THAN (65,30)
			);`,

		`CREATE TABLE t4 (
				col1 INT NOT NULL,
				col2 INT NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				UNIQUE KEY (col1),
				UNIQUE KEY (col2, col4)
			)
			PARTITION BY LIST (col1) (
			PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
			PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
			PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
			PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
			);`,

		`CREATE TABLE t4 (
				col1 INT NOT NULL,
				col2 INT NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				UNIQUE KEY (col1, col3),
				UNIQUE KEY (col2, col4)
			)
			PARTITION BY LIST COLUMNS(col1, col3) (
			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
			PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
			PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
			PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
			);`,
	}
	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}
}

func buildSingleStmt(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	statements, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1, 0)
	if err != nil {
		return nil, err
	}
	// this sql always return single statement
	context := opt.CurrentContext()
	plan, err := BuildPlan(context, statements[0], false)
	if plan != nil {
		testDeepCopy(plan)
	}
	return plan, err
}

func Test_checkUniqueKeyIncludePartKey(t *testing.T) {
	partKeys := map[string]int{
		"a": 0,
		"b": 0,
		"c": 0,
	}

	partKeys2 := map[string]int{
		"a": 0,
		"b": 0,
		"e": 0,
	}

	uniqueKeys := map[string]int{
		"a": 0,
		"b": 0,
		"c": 0,
		"d": 0,
	}

	r1 := checkUniqueKeyIncludePartKey(partKeys, uniqueKeys)
	require.True(t, r1)
	r2 := checkUniqueKeyIncludePartKey(partKeys2, uniqueKeys)
	require.False(t, r2)

	r3 := findColumnInIndexCols("a", uniqueKeys)
	require.True(t, r3)

	r4 := findColumnInIndexCols("e", uniqueKeys)
	require.False(t, r4)

	x := make(map[string]int)
	r5 := findColumnInIndexCols("e", x)
	require.False(t, r5)

	x["e"] = 0
	r6 := findColumnInIndexCols("e", x)
	require.True(t, r6)
}

func mockPartitionBinder(tableDef *plan.TableDef) (*PartitionBinder, error) {
	mock := NewMockOptimizer(false)
	builder := NewQueryBuilder(plan.Query_SELECT, mock.CurrentContext(), false, false)
	bindContext := NewBindContext(builder, nil)
	nodeID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       nil,
		ObjRef:      nil,
		TableDef:    tableDef,
		BindingTags: []int32{builder.genNewTag()},
	}, bindContext)

	err := builder.addBinding(nodeID, tree.AliasClause{}, bindContext)
	if err != nil {
		return nil, err
	}
	return NewPartitionBinder(builder, bindContext), err
}

func mockExpr(t *testing.T, s string) (tree.Expr, error) {
	selStr := "select " + s
	one, err := parsers.ParseOne(context.TODO(), dialect.MYSQL, selStr, 1, 0)
	require.Nil(t, err)
	return one.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, err
}

func Test_checkPartitionKeys(t *testing.T) {
	addCol := func(tableDef *TableDef, col *ColDef) {
		tableDef.Cols = append(tableDef.Cols, col)
	}

	/*
		table test:
		col1 int32 pk
		col2 int32
	*/
	tableDef := &plan.TableDef{
		Name: "test",
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"col1"},
		},
	}

	addCol(tableDef, &ColDef{
		Name: "col1",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef, &ColDef{
		Name: "col2",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})

	/*
		table test2:
		col1 int32 pk
		col2 int32 pk
		col3 int32

	*/
	tableDef2 := &plan.TableDef{
		Name: "test2",
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"col1", "col2"},
		},
	}

	addCol(tableDef2, &ColDef{
		Name: "col1",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef2, &ColDef{
		Name: "col2",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef2, &ColDef{
		Name: "col3",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})

	addIndex := func(tableDef *TableDef, index string, names ...string) {
		tableDef.Indexes = append(tableDef.Indexes, &IndexDef{
			Unique: true,
			Parts:  names,
		})
	}

	addIndex(tableDef2, "index1", "col1", "col2")

	{
		//partition keys [col1,col2] error
		pb, err := mockPartitionBinder(tableDef)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, "col1+1+col2")
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		require.Nil(t, err)

		partDef := &PartitionByDef{}
		partDef.PartitionExpr = &plan.PartitionExpr{
			Expr: expr,
		}

		err = checkPartitionKeys(context.TODO(), pb.builder.nameByColRef, tableDef, partDef)
		require.NotNil(t, err)
	}
	{
		//partition keys [col1]
		pb, err := mockPartitionBinder(tableDef)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, "col1+1")
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		require.Nil(t, err)

		partDef := &PartitionByDef{}
		partDef.PartitionExpr = &plan.PartitionExpr{
			Expr: expr,
		}

		err = checkPartitionKeys(context.TODO(), pb.builder.nameByColRef, tableDef, partDef)
		require.Nil(t, err)
	}
	{
		//partition keys []
		pb, err := mockPartitionBinder(tableDef)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, "1")
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		require.Nil(t, err)

		partDef := &PartitionByDef{}
		partDef.PartitionExpr = &plan.PartitionExpr{
			Expr: expr,
		}

		err = checkPartitionKeys(context.TODO(), pb.builder.nameByColRef, tableDef, partDef)
		require.Nil(t, err)
	}
	{
		//partition keys [col1,col2]
		pb, err := mockPartitionBinder(tableDef2)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, "col1+1+col2")
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		require.Nil(t, err)

		partDef := &PartitionByDef{}
		partDef.PartitionExpr = &plan.PartitionExpr{
			Expr: expr,
		}

		err = checkPartitionKeys(context.TODO(), pb.builder.nameByColRef, tableDef2, partDef)
		require.Nil(t, err)
	}
	addIndex(tableDef2, "index2", "col1")
	{
		//partition keys [col1,col2]
		pb, err := mockPartitionBinder(tableDef2)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, "col1+1+col2")
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		require.Nil(t, err)

		partDef := &PartitionByDef{}
		partDef.PartitionExpr = &plan.PartitionExpr{
			Expr: expr,
		}

		err = checkPartitionKeys(context.TODO(), pb.builder.nameByColRef, tableDef2, partDef)
		require.NotNil(t, err)
	}
	{
		//partition keys [col2]
		pb, err := mockPartitionBinder(tableDef2)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, "col2")
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		require.Nil(t, err)

		partDef := &PartitionByDef{}
		partDef.PartitionExpr = &plan.PartitionExpr{
			Expr: expr,
		}

		err = checkPartitionKeys(context.TODO(), pb.builder.nameByColRef, tableDef2, partDef)
		require.NotNil(t, err)
	}
	{
		//partition columns [col1]
		partDef := &PartitionByDef{}
		partDef.PartitionColumns = &plan.PartitionColumns{
			PartitionColumns: []string{"col1"},
		}

		err := checkPartitionKeys(context.TODO(), nil, tableDef2, partDef)
		require.Nil(t, err)
	}
	{
		//partition columns [col1,col1]
		partDef := &PartitionByDef{}
		partDef.PartitionColumns = &plan.PartitionColumns{
			PartitionColumns: []string{"col1", "col1"},
		}

		err := checkPartitionKeys(context.TODO(), nil, tableDef2, partDef)
		require.NotNil(t, err)
	}
	{
		//partition columns [col1]
		partDef := &PartitionByDef{}
		partDef.PartitionColumns = &plan.PartitionColumns{
			PartitionColumns: []string{"col1"},
		}
		tableDef2.Pkey = &PrimaryKeyDef{
			Names: []string{"col1", "col1"},
		}
		err := checkPartitionKeys(context.TODO(), nil, tableDef2, partDef)
		require.NotNil(t, err)
	}
	{
		//partition columns [col1]
		partDef := &PartitionByDef{}
		partDef.PartitionColumns = &plan.PartitionColumns{
			PartitionColumns: []string{"col1"},
		}
		tableDef2.Indexes[0].Parts = []string{
			"col1", "col1",
		}
		err := checkPartitionKeys(context.TODO(), nil, tableDef2, partDef)
		require.NotNil(t, err)
	}
}

func addCol(tableDef *TableDef, col *ColDef) {
	tableDef.Cols = append(tableDef.Cols, col)
}

func Test_checkPartitionExprType(t *testing.T) {
	/*
		table test:
		col1 int32 pk
		col2 int32
	*/
	tableDef := &plan.TableDef{
		Name: "test",
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"col1"},
		},
	}

	addCol(tableDef, &ColDef{
		Name: "col1",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef, &ColDef{
		Name: "col2",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})

	{
		//partition keys [col1]
		pb, err := mockPartitionBinder(tableDef)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, "col1")
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		require.Nil(t, err)

		partDef := &PartitionByDef{}
		partDef.PartitionExpr = &plan.PartitionExpr{
			Expr: expr,
		}

		err = checkPartitionExprType(context.TODO(), nil, nil, partDef)
		require.Nil(t, err)
	}
	{
		//partition keys [col1]
		pb, err := mockPartitionBinder(tableDef)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, "col1 / 3")
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		require.NotNil(t, err)
		require.Nil(t, expr)

	}
}

func Test_stringSliceToMap(t *testing.T) {
	smap := make(map[string]int)
	r1, _ := stringSliceToMap(nil, smap)
	require.False(t, r1)

	smap2 := make(map[string]int)
	r2, _ := stringSliceToMap([]string{"a1", "a2"}, smap2)
	require.False(t, r2)

	smap3 := make(map[string]int)
	r3, r31 := stringSliceToMap([]string{"a1", "a1"}, smap3)
	require.True(t, r3)
	require.Equal(t, r31, "a1")

	smap4 := make(map[string]int)
	r4, r41 := stringSliceToMap([]string{"", ""}, smap4)
	require.True(t, r4)
	require.Equal(t, r41, "")
}

func Test_checkDuplicatePartitionName(t *testing.T) {
	{
		partDef1 := &PartitionByDef{
			Partitions: []*plan.PartitionItem{
				{PartitionName: "p0"},
				{PartitionName: "p2"},
			},
		}

		err := checkPartitionNameUnique(context.TODO(), partDef1)
		require.Nil(t, err)
	}
	{
		partDef1 := &PartitionByDef{
			Partitions: []*plan.PartitionItem{
				{PartitionName: "p0"},
				{PartitionName: "p0"},
			},
		}

		err := checkPartitionNameUnique(context.TODO(), partDef1)
		require.NotNil(t, err)
	}
}

func Test_checkPartitionCount(t *testing.T) {
	{
		err := checkPartitionCount(context.TODO(), PartitionCountLimit-10)
		require.Nil(t, err)
	}
	{
		err := checkPartitionCount(context.TODO(), PartitionCountLimit+10)
		require.NotNil(t, err)
	}
}

func Test_checkDuplicatePartitionColumns(t *testing.T) {
	{
		partDef := &PartitionByDef{
			PartitionColumns: &plan.PartitionColumns{
				PartitionColumns: []string{},
			},
		}
		err := checkPartitionColumnsUnique(context.TODO(), partDef)
		require.Nil(t, err)
	}
	{
		partDef := &PartitionByDef{}
		err := checkPartitionColumnsUnique(context.TODO(), partDef)
		require.Nil(t, err)
	}
	{
		partDef := &PartitionByDef{
			PartitionColumns: &plan.PartitionColumns{
				PartitionColumns: []string{"a"},
			},
		}
		err := checkPartitionColumnsUnique(context.TODO(), partDef)
		require.Nil(t, err)
	}
	{
		partDef := &PartitionByDef{
			PartitionColumns: &plan.PartitionColumns{
				PartitionColumns: []string{"a", "a"},
			},
		}
		err := checkPartitionColumnsUnique(context.TODO(), partDef)
		require.NotNil(t, err)
	}
	{
		partDef := &PartitionByDef{
			PartitionColumns: &plan.PartitionColumns{
				PartitionColumns: []string{"a", "b"},
			},
		}
		err := checkPartitionColumnsUnique(context.TODO(), partDef)
		require.Nil(t, err)
	}
}

func TestCheckPartitionFuncValid(t *testing.T) {
	/*
		create table test(
			col1 int8,
			col2 int8,
			col3 date,
			PRIMARY KEY(col1, col2, col3)
		);
	*/
	tableDef := &plan.TableDef{
		Name: "test",
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"col1", "col2", "col3"},
		},
	}

	addCol(tableDef, &ColDef{
		Name: "col1",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef, &ColDef{
		Name: "col2",
		Typ: plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef, &ColDef{
		Name: "col3",
		Typ: plan.Type{
			Id: int32(types.T_date),
		},
	})

	type kase struct {
		s                        string
		wantPartitionFuncErr     bool
		wantPartitionExprTypeErr bool
	}

	checkFunc := func(k kase) {
		//fmt.Println(k.s)
		pb, err := mockPartitionBinder(tableDef)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, k.s)
		require.Nil(t, err)

		partitionDef := &PartitionByDef{
			Type: plan.PartitionType_HASH,
		}

		err = buildPartitionExpr(context.TODO(), tableDef, pb, partitionDef, astExpr)
		if !k.wantPartitionFuncErr {
			require.Nil(t, err)
			require.NotNil(t, partitionDef.PartitionExpr.Expr)

			err = checkPartitionExprType(context.TODO(), nil, nil, partitionDef)
			if !k.wantPartitionExprTypeErr {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
			}
		} else {
			require.NotNil(t, err)
		}
	}

	//---------------------------------------------------------------------------------
	rightCases := []kase{
		{"col1", false, false},
		{"col1 + col2", false, false},
		{"col1 - col2", false, false},
		{"col1 * col2", false, false},
		{"col1 div col2", false, false},
	}

	for _, k := range rightCases {
		checkFunc(k)
	}

	//--------------------------------------------------------------------------------
	wrongCases := []kase{
		{"col1 / 3", true, false},
		{"col1 / col2", true, false},
		{"col1 | col2", true, false},
		{"col1 & col2", true, false},
		{"col1 ^ col2", true, false},
		{"col1 << col2", true, false},
		{"col1 >> col2", true, false},
		{"~col2", true, false},
	}
	for _, k := range wrongCases {
		checkFunc(k)
	}

	//--------------------------------------------------------------------------------
	supportedFuncCases := []kase{
		{"abs(col1)", false, false},
		{"abs(-1)", true, false},
		{"ceiling(col1)", false, false},
		{"ceiling(0.1)", true, false},
		{"datediff('2007-12-31 23:59:59','2007-12-30')", true, false}, // XXX: should fold datediff and then report error
		{"datediff(col3,'2007-12-30')", false, false},                 // re check it
		{"day(col3)", false, false},
		{"dayofyear(col3)", false, false},
		{"extract(year from col3)", false, false},
		{"extract(year_month from col3)", false, false},
		{"floor(col1)", false, false},
		{"floor(0.1)", true, false},
		{"hour(col3)", true, false},
		{"minute(col3)", true, false},
		{"mod(col1,3)", false, false},
		{"month(col3)", false, false},
		{"second(col3)", true, false},
		{"unix_timestamp(col3)", true, false},
		{"weekday(col3)", false, false},
		{"year(col3)", false, false},
		{"to_days(col3)", false, false},
		{"to_seconds(col3)", false, false},
	}
	for _, k := range supportedFuncCases {
		checkFunc(k)
	}

	//-------------------------------------------------------------------------------------
	unsupportedFuncCases := []kase{
		{"dayofmonth(col3)", true, false},  //unimplement builtin function
		{"dayofweek(col3)", true, false},   //unimplement builtin function
		{"microsecond(col3)", true, false}, //unimplement builtin function
		{"quarter(col3)", true, false},     //unimplement builtin function
		{"time_to_sec(col3)", true, false}, //unimplement builtin function
		{"yearweek(col3)", true, false},    //unimplement builtin function
	}
	for _, k := range unsupportedFuncCases {
		checkFunc(k)
	}
}
