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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"testing"
)

func TestKeyPartition(t *testing.T) {
	// KEY(column_list) Partition
	//sql := "CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col3) PARTITIONS 4;"
	//sql := "CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col3);"
	//sql := "CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR KEY(col3) PARTITIONS 5;"
	//sql := "CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM = 1 (col3);"
	//sql := "CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR KEY ALGORITHM = 1 (col3) PARTITIONS 5;"
	//sql := "CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col1, col2) PARTITIONS 4;"
	sql := `CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT,
				store_id INT
			)
			PARTITION BY LINEAR HASH( YEAR(hired) )
			PARTITIONS 4;`

	mock := NewMockOptimizer()
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, false, t)
}

func TestKeyPartitionError(t *testing.T) {
	//sql := "CREATE TABLE ts (id INT, purchased DATE) PARTITION BY KEY( id ) PARTITIONS 4 SUBPARTITION BY HASH( TO_DAYS(purchased) ) SUBPARTITIONS 2;"
	//sql := "CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY(col4) PARTITIONS 4;"
	//sql := "CREATE TABLE tk (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY KEY ALGORITHM = 3 (col3);"
	//sql := "CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col2);"
	//sql := "CREATE TABLE t1 (col1 INT, col2 DECIMAL) PARTITION BY HASH(col2);"
	sql := "CREATE TABLE t1 (col1 INT, col2 DECIMAL) PARTITION BY HASH(col1+0.5);"
	mock := NewMockOptimizer()
	_, err := buildSingleStmt(mock, t, sql)
	if err == nil {
		t.Fatalf("should show error")
	} else {
		t.Log(err)
	}
}

//-----------------------------------------------------------------------------------

func TestHashPartition(t *testing.T) {
	// HASH(expr) Partition
	//sql := "CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1);"
	//sql := "CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1) PARTITIONS 4;"
	//sql := "CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3));"
	//sql := "CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR HASH( YEAR(col3)) PARTITIONS 6;"
	sql := `CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT,
				store_id INT
			)
			PARTITION BY HASH(store_id)
			PARTITIONS 4;`

	mock := NewMockOptimizer()
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, false, t)
}

func TestHashPartitionError(t *testing.T) {
	// HASH(expr) Partition
	sql := "CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3)) PARTITIONS 4 SUBPARTITION BY KEY(col1);"
	//sql := "CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY HASH( YEAR(col3) ) PARTITIONS;"
	//sql := `CREATE TABLE employees (
	//			id INT NOT NULL,
	//			fname VARCHAR(30),
	//			lname VARCHAR(30),
	//			hired DATE NOT NULL DEFAULT '1970-01-01',
	//			separated DATE NOT NULL DEFAULT '9999-12-31',
	//			job_code INT,
	//			store_id INT
	//		)
	//		PARTITION BY HASH(4)
	//		PARTITIONS 4;`

	mock := NewMockOptimizer()
	_, err := buildSingleStmt(mock, t, sql)
	if err == nil {
		t.Fatalf("Should show error")
	} else {
		t.Log(err)
	}
}

//-----------------------------------------------------------------------------------

func TestRangePartition(t *testing.T) {
	//sql := `CREATE TABLE employees (
	//			id INT NOT NULL,
	//			fname VARCHAR(30),
	//			lname VARCHAR(30),
	//			hired DATE NOT NULL DEFAULT '1970-01-01',
	//			separated DATE NOT NULL DEFAULT '9999-12-31',
	//			job_code INT NOT NULL,
	//			store_id INT NOT NULL
	//		)
	//		PARTITION BY RANGE (store_id) (
	//			PARTITION p0 VALUES LESS THAN (6),
	//			PARTITION p1 VALUES LESS THAN (11),
	//			PARTITION p2 VALUES LESS THAN (16),
	//			PARTITION p3 VALUES LESS THAN (21)
	//		);`

	//sql := `CREATE TABLE t1 (
	//			year_col  INT,
	//			some_data INT
	//		)
	//		PARTITION BY RANGE (year_col) (
	//			PARTITION p0 VALUES LESS THAN (1991),
	//			PARTITION p1 VALUES LESS THAN (1995),
	//			PARTITION p2 VALUES LESS THAN (1999),
	//			PARTITION p3 VALUES LESS THAN (2002),
	//			PARTITION p4 VALUES LESS THAN (2006),
	//			PARTITION p5 VALUES LESS THAN (2012)
	//		);`

	//sql := `CREATE TABLE t1 (
	//			year_col  INT,
	//			some_data INT
	//		)
	//		PARTITION BY RANGE (year_col) (
	//			PARTITION p0 VALUES LESS THAN (1991) COMMENT = 'Data for the years previous to 1991',
	//			PARTITION p1 VALUES LESS THAN (1995) COMMENT = 'Data for the years previous to 1995',
	//			PARTITION p2 VALUES LESS THAN (1999) COMMENT = 'Data for the years previous to 1999',
	//			PARTITION p3 VALUES LESS THAN (2002) COMMENT = 'Data for the years previous to 2002',
	//			PARTITION p4 VALUES LESS THAN (2006) COMMENT = 'Data for the years previous to 2006',
	//			PARTITION p5 VALUES LESS THAN (2012) COMMENT = 'Data for the years previous to 2012'
	//		);`

	//sql := `CREATE TABLE employees (
	//			id INT NOT NULL,
	//			fname VARCHAR(30),
	//			lname VARCHAR(30),
	//			hired DATE NOT NULL DEFAULT '1970-01-01',
	//			separated DATE NOT NULL DEFAULT '9999-12-31',
	//			job_code INT NOT NULL,
	//			store_id INT NOT NULL
	//		)
	//		PARTITION BY RANGE (store_id) (
	//			PARTITION p0 VALUES LESS THAN (6),
	//			PARTITION p1 VALUES LESS THAN (11),
	//			PARTITION p2 VALUES LESS THAN (16),
	//			PARTITION p3 VALUES LESS THAN MAXVALUE
	//		);`

	//sql := `CREATE TABLE employees (
	//			id INT NOT NULL,
	//			fname VARCHAR(30),
	//			lname VARCHAR(30),
	//			hired DATE NOT NULL DEFAULT '1970-01-01',
	//			separated DATE NOT NULL DEFAULT '9999-12-31',
	//			job_code INT NOT NULL,
	//			store_id INT NOT NULL
	//		)
	//		PARTITION BY RANGE (job_code) (
	//			PARTITION p0 VALUES LESS THAN (100),
	//			PARTITION p1 VALUES LESS THAN (1000),
	//			PARTITION p2 VALUES LESS THAN (10000)
	//		);`

	//sql := `CREATE TABLE employees (
	//			id INT NOT NULL,
	//			fname VARCHAR(30),
	//			lname VARCHAR(30),
	//			hired DATE NOT NULL DEFAULT '1970-01-01',
	//			separated DATE NOT NULL DEFAULT '9999-12-31',
	//			job_code INT,
	//			store_id INT
	//		)
	//		PARTITION BY RANGE ( YEAR(separated) ) (
	//			PARTITION p0 VALUES LESS THAN (1991),
	//			PARTITION p1 VALUES LESS THAN (1996),
	//			PARTITION p2 VALUES LESS THAN (2001),
	//			PARTITION p3 VALUES LESS THAN MAXVALUE
	//		);`

	//sql := `CREATE TABLE rc (
	//			a INT NOT NULL,
	//			b INT NOT NULL
	//		)
	//		PARTITION BY RANGE COLUMNS(a,b) (
	//			PARTITION p0 VALUES LESS THAN (10,5),
	//			PARTITION p1 VALUES LESS THAN (20,10),
	//			PARTITION p2 VALUES LESS THAN (50,20),
	//			PARTITION p3 VALUES LESS THAN (65,30)
	//		);`

	//sql := `CREATE TABLE rc (
	//			a INT NOT NULL,
	//			b INT NOT NULL
	//		)
	//		PARTITION BY RANGE COLUMNS(a,b) (
	//			PARTITION p0 VALUES LESS THAN (10,5),
	//			PARTITION p1 VALUES LESS THAN (20,10),
	//			PARTITION p2 VALUES LESS THAN (50,MAXVALUE),
	//			PARTITION p3 VALUES LESS THAN (65,MAXVALUE),
	//			PARTITION p4 VALUES LESS THAN (MAXVALUE,MAXVALUE)
	//		);`

	sql := `CREATE TABLE rc (
				a INT NOT NULL,
				b INT NOT NULL
			)
			PARTITION BY RANGE COLUMNS(a,b) (
				PARTITION p0 VALUES LESS THAN (10,5) COMMENT = 'Data for LESS THAN (10,5)',
				PARTITION p1 VALUES LESS THAN (20,10) COMMENT = 'Data for LESS THAN (20,10)',
				PARTITION p2 VALUES LESS THAN (50,MAXVALUE) COMMENT = 'Data for LESS THAN (50,MAXVALUE)',
				PARTITION p3 VALUES LESS THAN (65,MAXVALUE) COMMENT = 'Data for LESS THAN (65,MAXVALUE)',
				PARTITION p4 VALUES LESS THAN (MAXVALUE,MAXVALUE) COMMENT = 'Data for LESS THAN (MAXVALUE,MAXVALUE)'
			);`

	mock := NewMockOptimizer()
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, false, t)
}

func TestListPartition(t *testing.T) {
	//sql := `CREATE TABLE client_firms (
	//			id   INT,
	//			name VARCHAR(35)
	//		)
	//		PARTITION BY LIST (id) (
	//			PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
	//			PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
	//			PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
	//			PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
	//		);`

	sql := `CREATE TABLE lc (
				a INT NULL,
				b INT NULL
			)
			PARTITION BY LIST COLUMNS(a,b) (
				PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
				PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
				PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
				PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
			);`

	mock := NewMockOptimizer()
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, false, t)
}

func TestSubPartition(t *testing.T) {
	sql := `CREATE TABLE ts (id INT, purchased DATE)
		PARTITION BY RANGE( YEAR(purchased) )
		SUBPARTITION BY HASH( TO_DAYS(purchased) )
		SUBPARTITIONS 2 (
			PARTITION p0 VALUES LESS THAN (1990),
			PARTITION p1 VALUES LESS THAN (2000),
			PARTITION p2 VALUES LESS THAN MAXVALUE
		);`

	//sql := `CREATE TABLE ts (id INT, purchased DATE)
	//PARTITION BY RANGE( YEAR(purchased) )
	//SUBPARTITION BY HASH( TO_DAYS(purchased) ) (
	//    PARTITION p0 VALUES LESS THAN (1990) (
	//        SUBPARTITION s0,
	//        SUBPARTITION s1
	//    ),
	//    PARTITION p1 VALUES LESS THAN (2000) (
	//        SUBPARTITION s2,
	//        SUBPARTITION s3
	//    ),
	//    PARTITION p2 VALUES LESS THAN MAXVALUE (
	//        SUBPARTITION s4,
	//        SUBPARTITION s5
	//    )
	//);`

	//sql := `CREATE TABLE ts2 (id INT, purchased DATE)
	//		PARTITION BY RANGE( YEAR(purchased) )
	//		SUBPARTITION BY HASH( TO_DAYS(purchased) ) (
	//			PARTITION p0 VALUES LESS THAN (1990) COMMENT 'comment1990' (
	//				SUBPARTITION s0 COMMENT 'sub comment1990 s0',
	//				SUBPARTITION s1 COMMENT 'sub comment1990 s1'
	//			),
	//			PARTITION p1 VALUES LESS THAN (2000) COMMENT 'comment2000'(
	//				SUBPARTITION s2 COMMENT 'sub comment2000 s2',
	//				SUBPARTITION s3 COMMENT 'sub comment2000 s3'
	//			),
	//			PARTITION p2 VALUES LESS THAN MAXVALUE COMMENT 'commentMaxValue'(
	//				SUBPARTITION s4 COMMENT 'sub commentMaxValue s4',
	//				SUBPARTITION s5 COMMENT 'sub commentMaxValue s5'
	//			)
	//		);`

	mock := NewMockOptimizer()
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, false, t)
}

func buildSingleStmt(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	statements, err := mysql.Parse(sql)
	if err != nil {
		return nil, err
	}
	// this sql always return single statement
	context := opt.CurrentContext()
	return BuildPlan(context, statements[0])
}
