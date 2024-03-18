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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreateHashPartitionTable(t *testing.T) {
	sql := "CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR HASH( YEAR(col3)) PARTITIONS 6;"
	//sql := "create table p_hash_table_08(col1 tinyint,col2 varchar(30),col3 decimal(6,3))partition by hash(ceil(col3)) partitions 2;"
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

// -----------------------Hash Partition-------------------------------------
func TestHashPartition(t *testing.T) {
	// HASH(expr) Partition
	sqls := []string{
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1);",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1) PARTITIONS 4;",
		//"CREATE TABLE t1 (col1 INT, col2 DECIMAL) PARTITION BY HASH(col2) PARTITIONS 4;",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3));",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3) + col1 % (7*24));",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR HASH( YEAR(col3)) PARTITIONS 6;",
		"create table t2 (a date, b datetime) partition by hash (EXTRACT(YEAR_MONTH FROM a)) partitions 7",
		"create table t3 (a int, b int) partition by hash(ceiling(a-b)) partitions 10",
		"create table t4 (a int, b int) partition by hash(floor(a-b)) partitions 10",
		`CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT,
				store_id INT
			)
			PARTITION BY HASH(store_id)
			PARTITIONS 4;`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1, col2)
		)
		PARTITION BY HASH(col1)
		PARTITIONS 4;`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1, col3)
		)
		PARTITION BY HASH(col1 + col3)
		PARTITIONS 4;`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1)
		)
		PARTITION BY HASH(col1+10)
		PARTITIONS 4;`,
		`CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT,
				store_id INT
			)
			PARTITION BY LINEAR HASH( YEAR(hired) )
			PARTITIONS 4;`,
	}

	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		t.Log(sql)
		_, err := buildSingleStmt(mock, t, sql)
		require.Nil(t, err)
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
}

func TestHashPartition2(t *testing.T) {
	// HASH(expr) Partition
	sqls := []string{
		"CREATE TABLE t2 (col1 INT, col2 CHAR(5)) " +
			"PARTITION BY HASH(col1) PARTITIONS 1 " +
			"( PARTITION p0 " +
			"ENGINE = 'engine_name' " +
			"COMMENT = 'p0_comment' " +
			"DATA DIRECTORY = 'data_dir' " +
			"INDEX DIRECTORY = 'data_dir' " +
			"MAX_ROWS = 100 " +
			"MIN_ROWS = 100 " +
			"TABLESPACE = space " +
			"(SUBPARTITION sub_name) " +
			");",
	}

	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		t.Log(sql)
		_, err := buildSingleStmt(mock, t, sql)
		require.Nil(t, err)
		if err != nil {
			t.Fatalf("%+v", err)
		}
	}
}

func TestHashPartitionError(t *testing.T) {
	// HASH(expr) Partition
	sqls := []string{
		// In MySQL, RANGE, LIST, and HASH partitions require that the partitioning key must be of type INT or be returned through an expression.
		// For the following Partition table test case, in matrixone, when the parameter of ceil function is of decimal type and the return value type is of decimal type,
		// it cannot be used as the partition expression type, but in MySQL, when the parameter of ceil function is of decimal type and the return
		// value is of int type, it can be used as the partition expression type
		"create table p_hash_table_08(col1 tinyint,col2 varchar(30),col3 decimal(6,3))partition by hash(ceil(col3)) partitions 2;",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col2);",
		"CREATE TABLE t1 (col1 INT, col2 DECIMAL) PARTITION BY HASH(col2) PARTITIONS 4;",
		"CREATE TABLE t1 (col1 INT, col2 DECIMAL) PARTITION BY HASH(12);",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3)) PARTITIONS 4 SUBPARTITION BY KEY(col1);",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY HASH( YEAR(col3) ) PARTITIONS;",
		"create table t3 (a int, b int) partition by hash(ceiling(a-b) + 23.5) partitions 10",
		`CREATE TABLE employees (
					id INT NOT NULL,
					fname VARCHAR(30),
					lname VARCHAR(30),
					hired DATE NOT NULL DEFAULT '1970-01-01',
					separated DATE NOT NULL DEFAULT '9999-12-31',
					job_code INT,
					store_id INT
				)
				PARTITION BY HASH(4)
				PARTITIONS 4;`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1, col2)
		)
			PARTITION BY HASH(col3)
			PARTITIONS 4;`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1)
		)
			PARTITION BY HASH(col1 + col3)
			PARTITIONS 4;`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1),
			UNIQUE KEY (col3)
		)
		PARTITION BY HASH(col1+col3)
		PARTITIONS 4;`,

		`create table p_hash_table_03(
			col1 bigint ,
			col2 date default '1970-01-01',
			col3 varchar(30)
		)
		partition by hash(year(col3))
		partitions 8;`,

		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT,
			store_id INT
		) PARTITION BY HASH(store_id) PARTITIONS 102400000000;`,

		`create table p_hash_table_03(
				col1 bigint ,
				col2 date default '1970-01-01',
				col3 varchar(30)
			)
			partition by hash(col4)
			partitions 8;`,
	}

	mock := NewMockOptimizer(false)
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		require.NotNil(t, err)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}

}

func Test_hash_buildPartitionDefs(t *testing.T) {
	type kase struct {
		sql     string
		def     *plan.PartitionByDef
		wantErr bool
	}

	kases := []kase{
		{
			sql: "create table a(col1 int) partition by hash(col1) (partition x1, partition x2);",
			def: &plan.PartitionByDef{
				PartitionNum: 2,
			},
			wantErr: false,
		},
		{
			sql: "create table a(col1 int) partition by hash(col1) (partition x1, partition x2);",
			def: &plan.PartitionByDef{
				PartitionNum: 1,
			},
			wantErr: true,
		},
		{
			sql: "create table a(col1 int) partition by hash(col1) ;",
			def: &plan.PartitionByDef{
				PartitionNum: 2,
			},
			wantErr: false,
		},
		{
			sql: "create table a(col1 int) partition by hash(col1) (partition x1, partition x1);",
			def: &plan.PartitionByDef{
				PartitionNum: 4,
			},
			wantErr: true,
		},
	}

	hpb := &hashPartitionBuilder{}

	for _, k := range kases {
		one, err := parsers.ParseOne(context.TODO(), dialect.MYSQL, k.sql, 1)
		require.Nil(t, err)
		syntaxDefs := one.(*tree.CreateTable).PartitionOption.Partitions
		err = hpb.buildPartitionDefs(context.TODO(), nil, k.def, syntaxDefs)
		fmt.Println(k.sql)
		if !k.wantErr {
			require.Nil(t, err)
			require.LessOrEqual(t, len(syntaxDefs), int(k.def.PartitionNum))
			require.Equal(t, int(k.def.PartitionNum), len(k.def.Partitions))
			//check partition names
			i := 0
			for ; i < len(syntaxDefs); i++ {
				require.Equal(t, string(syntaxDefs[i].Name), k.def.Partitions[i].PartitionName)
				require.Equal(t, i, int(k.def.Partitions[i].OrdinalPosition)-1)
			}
			for ; i < int(k.def.PartitionNum); i++ {
				require.Equal(t, fmt.Sprintf("p%d", i), k.def.Partitions[i].PartitionName)
				require.Equal(t, i, int(k.def.Partitions[i].OrdinalPosition)-1)
			}
		} else {
			require.NotNil(t, err)
		}

	}

}

func Test_hash_buildEvalPartitionExpression(t *testing.T) {
	sql1 := " create table a(col1 int,col2 int) partition by hash(col1+col2)"
	one, err := parsers.ParseOne(context.TODO(), dialect.MYSQL, sql1, 1)
	require.Nil(t, err)

	/*
		table test:
		col1 int32 pk
		col2 int32
	*/
	tableDef := &plan.TableDef{
		Name: "a",
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
	//partition keys [col1]
	pb, err := mockPartitionBinder(tableDef)
	require.Nil(t, err)

	partDef := &PartitionByDef{}

	hpb := &hashPartitionBuilder{}
	err = hpb.buildEvalPartitionExpression(context.TODO(), pb, one.(*tree.CreateTable).PartitionOption, partDef)
	require.Nil(t, err)
	require.NotNil(t, partDef.PartitionExpression)
}
