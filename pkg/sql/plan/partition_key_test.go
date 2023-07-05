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

import "testing"

func TestCreateKeyPartitionTable(t *testing.T) {
	//sql := "create table p_table_18(col1 bigint,col2 varchar(25),col3 decimal(6,4))partition by key(col3)partitions 2;"
	sql := "create table p_table_18(col1 bigint,col2 varchar(25),col3 decimal(20,4))partition by key(col3)partitions 2;"
	//sql := "create table p_table_18(col1 bigint,col2 varchar(25),col3 float)partition by key(col3)partitions 2;"
	//sql := "create table p_table_18(col1 bigint,col2 varchar(25),col3 double)partition by key(col3)partitions 2;"
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

// ---------------------------------- Key Partition ----------------------------------
func TestKeyPartition(t *testing.T) {
	// KEY(column_list) Partition
	sqls := []string{
		"create table p_table_1(col1 bigint,col2 varchar(25),col3 decimal(6,4))partition by key(col3)partitions 2;",
		"create table p_table_2(col1 bigint,col2 varchar(25),col3 decimal(20,4))partition by key(col3)partitions 2;",
		"create table p_table_3(col1 bigint,col2 varchar(25),col3 float)partition by key(col3)partitions 2;",
		"create table p_table_4(col1 bigint,col2 varchar(25),col3 double)partition by key(col3)partitions 2;",
		"create table p_table_5(col1 bigint,col2 varchar(25),col3 timestamp)partition by key(col3)partitions 2;",
		"create table p_table_6(col1 bigint,col2 varchar(25),col3 time)partition by key(col3)partitions 2;",
		"CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col3) PARTITIONS 4;",
		"CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col3);",
		"CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR KEY(col3) PARTITIONS 5;",
		"CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM = 1 (col3);",
		"CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR KEY ALGORITHM = 1 (col3) PARTITIONS 5;",
		"CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col1, col2) PARTITIONS 4;",
		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1, col2)
		)
		PARTITION BY KEY(col1)
		PARTITIONS 4;`,
		`CREATE TABLE k1 (
					id INT NOT NULL PRIMARY KEY,
					name VARCHAR(20)
				)
				PARTITION BY KEY()
				PARTITIONS 2;`,
		`CREATE TABLE k1 (
				id INT NOT NULL,
				name VARCHAR(20),
				sal DOUBLE,
				PRIMARY KEY (id, name)
			)
			PARTITION BY KEY()
			PARTITIONS 2;`,
		`CREATE TABLE k1 (
				id INT NOT NULL,
				name VARCHAR(20),
				UNIQUE KEY (id)
			)
			PARTITION BY KEY()
			PARTITIONS 2;`,
		`CREATE TABLE t1 (
				col1 INT NOT NULL,
				col2 DATE NOT NULL,
				col3 INT NOT NULL,
				col4 INT NOT NULL,
				PRIMARY KEY (col1, col2)
			)
			PARTITION BY KEY()
			PARTITIONS 4;`,
		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			unique key (col1, col4)
		)
			PARTITION BY KEY()
			PARTITIONS 4;`,

		`CREATE TABLE t8 (
			col1 INT,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col3)
		)
		PARTITION BY KEY(col1)
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

func TestKeyPartitionError(t *testing.T) {
	sqls := []string{
		"create table p_t1(col1 bigint,col2 varchar(25),col3 blob)partition by key(col3)partitions 2;",
		"create table p_t2(col1 bigint,col2 varchar(25),col3 text)partition by key(col3)partitions 2;",
		"create table p_t3(col1 bigint,col2 varchar(25),col3 json)partition by key(col3)partitions 2;",
		"CREATE TABLE ts (id INT, purchased DATE) PARTITION BY KEY( id ) PARTITIONS 4 SUBPARTITION BY HASH( TO_DAYS(purchased) ) SUBPARTITIONS 2;",
		"CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col4) PARTITIONS 4;",
		"CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM = 3 (col3);",
		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1, col2)
		)
		PARTITION BY KEY(col3)
		PARTITIONS 4;`,
		`CREATE TABLE k1 (
					id INT NOT NULL,
					name VARCHAR(20)
				)
				PARTITION BY KEY()
				PARTITIONS 2;`,
		`CREATE TABLE t4 (
			col1 INT NOT NULL,
			col2 INT NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col3),
			UNIQUE KEY (col2, col4)
		)
		PARTITION BY KEY()
		PARTITIONS 2;`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			unique key (col3, col4)
		)
			PARTITION BY KEY()
			PARTITIONS 4;`,

		`CREATE TABLE t3 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1, col4),
			unique key (col1)
		)
			PARTITION BY KEY()
			PARTITIONS 4;`,

		`CREATE TABLE t1 (
		col1 INT NOT NULL,
		col2 DATE NOT NULL,
		col3 INT NOT NULL,
		col4 INT NOT NULL,
		PRIMARY KEY (col1, col2)
		)
		PARTITION BY KEY(col3)
		PARTITIONS 4;`,

		`create table p_table_07(
			col1 int,
			col2 char(25),
			col3 decimal(4,2),
			unique key k2(col1,col2)
		)partition by key()
		partitions 8;`,

		`create table p_table_09(
			col1 int NOT NULL,
			col2 char(25)  NOT NULL,
			col3 decimal(4,2) NOT NULL
		)partition by key()
		partitions 8;`,

		`create table p_table_01(
			col1 int,
			col2 char(25),
			col3 int NOT NULL,
			UNIQUE KEY k2(col1,col2),
			UNIQUE KEY k3(col3)
			)
		partition by key()
		partitions 8;`,

		`create table p_table_02(
			col1 int NOT NULL,
			col2 char(25) NOT NULL,
			col3 int NOT NULL,
			UNIQUE KEY k2(col1,col2),
			UNIQUE KEY k3(col3)
		)
		partition by key()
		partitions 8;`,

		`create table p_table_03(
			col1 int,
			col2 char(25),
			col3 decimal(4,2) NOT NULL,
			UNIQUE KEY k2(col1,col2)
		)
		partition by key()
		partitions 8;`,

		`CREATE TABLE t5 (
			col1 INT,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col3)
		)
		PARTITION BY KEY()
		PARTITIONS 4;`,

		`CREATE TABLE t7 (
			col1 INT,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1, col3)
		)
		PARTITION BY KEY()
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
