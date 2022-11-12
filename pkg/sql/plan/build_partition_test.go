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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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

	mock := NewMockOptimizer()
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
	}

	mock := NewMockOptimizer()
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
	}
	mock := NewMockOptimizer()
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}
}

// -----------------------Hash Partition-------------------------------------
func TestHashPartition(t *testing.T) {
	// HASH(expr) Partition
	sqls := []string{
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1);",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col1) PARTITIONS 4;",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3));",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY LINEAR HASH( YEAR(col3)) PARTITIONS 6;",
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

	mock := NewMockOptimizer()
	for _, sql := range sqls {
		t.Log(sql)
		logicPlan, err := buildSingleStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

func TestHashPartitionError(t *testing.T) {
	// HASH(expr) Partition
	sqls := []string{
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5)) PARTITION BY HASH(col2);",
		"CREATE TABLE t1 (col1 INT, col2 DECIMAL) PARTITION BY HASH(col2);",
		"CREATE TABLE t1 (col1 INT, col2 DECIMAL) PARTITION BY HASH(col1+0.5);",
		"CREATE TABLE t1 (col1 INT, col2 DECIMAL) PARTITION BY HASH(12);",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATETIME) PARTITION BY HASH (YEAR(col3)) PARTITIONS 4 SUBPARTITION BY KEY(col1);",
		"CREATE TABLE t1 (col1 INT, col2 CHAR(5), col3 DATE) PARTITION BY HASH( YEAR(col3) ) PARTITIONS;",
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
		PARTITION BY HASH(col1,col3)
		PARTITIONS 4;`,
	}

	mock := NewMockOptimizer()
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}

}

// -----------------------Range Partition-------------------------------------
func TestRangePartition(t *testing.T) {
	sqls := []string{
		`CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT NOT NULL,
				store_id INT NOT NULL
			)
			PARTITION BY RANGE (store_id) (
				PARTITION p0 VALUES LESS THAN (6),
				PARTITION p1 VALUES LESS THAN (11),
				PARTITION p2 VALUES LESS THAN (16),
				PARTITION p3 VALUES LESS THAN (21)
			);`,

		`CREATE TABLE t1 (
				year_col  INT,
				some_data INT
			)
			PARTITION BY RANGE (year_col) (
				PARTITION p0 VALUES LESS THAN (1991),
				PARTITION p1 VALUES LESS THAN (1995),
				PARTITION p2 VALUES LESS THAN (1999),
				PARTITION p3 VALUES LESS THAN (2002),
				PARTITION p4 VALUES LESS THAN (2006),
				PARTITION p5 VALUES LESS THAN (2012)
			);`,

		`CREATE TABLE t1 (
				year_col  INT,
				some_data INT
			)
			PARTITION BY RANGE (year_col) (
				PARTITION p0 VALUES LESS THAN (1991) COMMENT = 'Data for the years previous to 1991',
				PARTITION p1 VALUES LESS THAN (1995) COMMENT = 'Data for the years previous to 1995',
				PARTITION p2 VALUES LESS THAN (1999) COMMENT = 'Data for the years previous to 1999',
				PARTITION p3 VALUES LESS THAN (2002) COMMENT = 'Data for the years previous to 2002',
				PARTITION p4 VALUES LESS THAN (2006) COMMENT = 'Data for the years previous to 2006',
				PARTITION p5 VALUES LESS THAN (2012) COMMENT = 'Data for the years previous to 2012'
			);`,

		`CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT NOT NULL,
				store_id INT NOT NULL
			)
			PARTITION BY RANGE (store_id) (
				PARTITION p0 VALUES LESS THAN (6),
				PARTITION p1 VALUES LESS THAN (11),
				PARTITION p2 VALUES LESS THAN (16),
				PARTITION p3 VALUES LESS THAN MAXVALUE
			);`,

		`CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT NOT NULL,
				store_id INT NOT NULL
			)
			PARTITION BY RANGE (job_code) (
				PARTITION p0 VALUES LESS THAN (100),
				PARTITION p1 VALUES LESS THAN (1000),
				PARTITION p2 VALUES LESS THAN (10000)
			);`,

		`CREATE TABLE employees (
				id INT NOT NULL,
				fname VARCHAR(30),
				lname VARCHAR(30),
				hired DATE NOT NULL DEFAULT '1970-01-01',
				separated DATE NOT NULL DEFAULT '9999-12-31',
				job_code INT,
				store_id INT
			)
			PARTITION BY RANGE ( YEAR(separated) ) (
				PARTITION p0 VALUES LESS THAN (1991),
				PARTITION p1 VALUES LESS THAN (1996),
				PARTITION p2 VALUES LESS THAN (2001),
				PARTITION p3 VALUES LESS THAN MAXVALUE
			);`,

		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT NOT NULL,
			store_id INT NOT NULL,
			PRIMARY KEY(id, store_id)
		)
			PARTITION BY RANGE (store_id) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21)
		);`,

		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT NOT NULL,
			store_id INT NOT NULL,
			PRIMARY KEY(id, store_id)
		)
			PARTITION BY RANGE (store_id + 5) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21)
		);`,

		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT NOT NULL,
			store_id INT NOT NULL,
			PRIMARY KEY(id, hired)
		)
			PARTITION BY RANGE (year(hired)) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21)
		);`,

		`CREATE TABLE members (
			firstname VARCHAR(25) NOT NULL,
			lastname VARCHAR(25) NOT NULL,
			username VARCHAR(16) NOT NULL,
			email VARCHAR(35),
			joined DATE NOT NULL
		)
		PARTITION BY RANGE( YEAR(joined) ) PARTITIONS 5 (
			PARTITION p0 VALUES LESS THAN (1960),
			PARTITION p1 VALUES LESS THAN (1970),
			PARTITION p2 VALUES LESS THAN (1980),
			PARTITION p3 VALUES LESS THAN (1990),
			PARTITION p4 VALUES LESS THAN MAXVALUE
		);`,

		`CREATE TABLE quarterly_report_status (
			report_id INT NOT NULL,
			report_status VARCHAR(20) NOT NULL,
			report_updated TIMESTAMP NOT NULL
		)
			PARTITION BY RANGE ( UNIX_TIMESTAMP(report_updated) ) (
			PARTITION p0 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-01-01 00:00:00') ),
			PARTITION p1 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-04-01 00:00:00') ),
			PARTITION p2 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-07-01 00:00:00') ),
			PARTITION p3 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-10-01 00:00:00') ),
			PARTITION p4 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-01-01 00:00:00') ),
			PARTITION p5 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-04-01 00:00:00') ),
			PARTITION p6 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-07-01 00:00:00') ),
			PARTITION p7 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-10-01 00:00:00') ),
			PARTITION p8 VALUES LESS THAN ( UNIX_TIMESTAMP('2010-01-01 00:00:00') ),
			PARTITION p9 VALUES LESS THAN (MAXVALUE)
		);`,
	}

	mock := NewMockOptimizer()
	for _, sql := range sqls {
		t.Log(sql)
		logicPlan, err := buildSingleStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

func TestRangePartitionError(t *testing.T) {
	sqls := []string{
		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT NOT NULL,
			store_id INT NOT NULL,
			PRIMARY KEY(id, store_id)
		)
		PARTITION BY RANGE (job_code) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21)
		);`,

		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT NOT NULL,
			store_id INT NOT NULL,
			PRIMARY KEY(id, store_id)
		)
		PARTITION BY RANGE (job_code + 5) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21)
		);`,

		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT NOT NULL,
			store_id INT NOT NULL,
			PRIMARY KEY(id, hired)
		)
		PARTITION BY RANGE (year(separated)) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21)
		);`,

		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT NOT NULL,
			store_id INT NOT NULL,
			PRIMARY KEY(id, store_id)
		)
		PARTITION BY RANGE (job_code + store_id) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21)
		);`,

		`CREATE TABLE members (
			firstname VARCHAR(25) NOT NULL,
			lastname VARCHAR(25) NOT NULL,
			username VARCHAR(16) NOT NULL,
			email VARCHAR(35),
			joined DATE NOT NULL
		)
		PARTITION BY RANGE( YEAR(joined) ) PARTITIONS 4 (
			PARTITION p0 VALUES LESS THAN (1960),
			PARTITION p1 VALUES LESS THAN (1970),
			PARTITION p2 VALUES LESS THAN (1980),
			PARTITION p3 VALUES LESS THAN (1990),
			PARTITION p4 VALUES LESS THAN MAXVALUE
		);`,
	}

	mock := NewMockOptimizer()
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}
}

// ---------------------Range Columns Partition--------------------------------
func TestRangeColumnsPartition(t *testing.T) {
	sqls := []string{
		`CREATE TABLE rc (
				a INT NOT NULL,
				b INT NOT NULL
			)
			PARTITION BY RANGE COLUMNS(a,b) (
				PARTITION p0 VALUES LESS THAN (10,5),
				PARTITION p1 VALUES LESS THAN (20,10),
				PARTITION p2 VALUES LESS THAN (50,20),
				PARTITION p3 VALUES LESS THAN (65,30)
			);`,

		`CREATE TABLE rc (
				a INT NOT NULL,
				b INT NOT NULL
			)
			PARTITION BY RANGE COLUMNS(a,b) (
				PARTITION p0 VALUES LESS THAN (10,5),
				PARTITION p1 VALUES LESS THAN (20,10),
				PARTITION p2 VALUES LESS THAN (50,MAXVALUE),
				PARTITION p3 VALUES LESS THAN (65,MAXVALUE),
				PARTITION p4 VALUES LESS THAN (MAXVALUE,MAXVALUE)
			);`,

		`CREATE TABLE rc (
				a INT NOT NULL,
				b INT NOT NULL
			)
			PARTITION BY RANGE COLUMNS(a,b) (
				PARTITION p0 VALUES LESS THAN (10,5) COMMENT = 'Data for LESS THAN (10,5)',
				PARTITION p1 VALUES LESS THAN (20,10) COMMENT = 'Data for LESS THAN (20,10)',
				PARTITION p2 VALUES LESS THAN (50,MAXVALUE) COMMENT = 'Data for LESS THAN (50,MAXVALUE)',
				PARTITION p3 VALUES LESS THAN (65,MAXVALUE) COMMENT = 'Data for LESS THAN (65,MAXVALUE)',
				PARTITION p4 VALUES LESS THAN (MAXVALUE,MAXVALUE) COMMENT = 'Data for LESS THAN (MAXVALUE,MAXVALUE)'
			);`,

		`CREATE TABLE rcx (
				a INT,
				b INT,
				c CHAR(3),
				d INT
			)
			PARTITION BY RANGE COLUMNS(a,d,c) (
				PARTITION p0 VALUES LESS THAN (5,10,'ggg'),
				PARTITION p1 VALUES LESS THAN (10,20,'mmm'),
				PARTITION p2 VALUES LESS THAN (15,30,'sss'),
				PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE,MAXVALUE)
			);`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 INT NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col3)
		)
			PARTITION BY RANGE COLUMNS(col1,col3) (
			PARTITION p0 VALUES LESS THAN (10,5),
			PARTITION p1 VALUES LESS THAN (20,10),
			PARTITION p2 VALUES LESS THAN (50,20),
			PARTITION p3 VALUES LESS THAN (65,30)
		);`,

		`CREATE TABLE rc (
				a INT NOT NULL,
				b INT NOT NULL
			)
			PARTITION BY RANGE COLUMNS(a,b) PARTITIONS 4 (
				PARTITION p0 VALUES LESS THAN (10,5),
				PARTITION p1 VALUES LESS THAN (20,10),
				PARTITION p2 VALUES LESS THAN (50,20),
				PARTITION p3 VALUES LESS THAN (65,30)
         );`,
	}
	mock := NewMockOptimizer()
	for _, sql := range sqls {
		t.Log(sql)
		logicPlan, err := buildSingleStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

func TestRangeColumnsPartitionError(t *testing.T) {
	sqls := []string{
		`CREATE TABLE rc3 (
			a INT NOT NULL,
			b INT NOT NULL
		)
		PARTITION BY RANGE COLUMNS(a,b) (
			PARTITION p0 VALUES LESS THAN (a,5),
			PARTITION p1 VALUES LESS THAN (20,10),
			PARTITION p2 VALUES LESS THAN (50,20),
			PARTITION p3 VALUES LESS THAN (65,30)
		);`,

		`CREATE TABLE rc3 (
			a INT NOT NULL,
			b INT NOT NULL
		)
		PARTITION BY RANGE COLUMNS(a,b) (
			PARTITION p0 VALUES LESS THAN (a+7,5),
			PARTITION p1 VALUES LESS THAN (20,10),
			PARTITION p2 VALUES LESS THAN (50,20),
			PARTITION p3 VALUES LESS THAN (65,30)
		);`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 INT NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY(col1, col3)
		)
		PARTITION BY RANGE COLUMNS(col1,col2) (
			PARTITION p0 VALUES LESS THAN (10,5),
			PARTITION p1 VALUES LESS THAN (20,10),
			PARTITION p2 VALUES LESS THAN (50,20),
			PARTITION p3 VALUES LESS THAN (65,30)
		);`,

		`CREATE TABLE rc (
				a INT NOT NULL,
				b INT NOT NULL
			)
			PARTITION BY RANGE COLUMNS(a,b) PARTITIONS 5 (
				PARTITION p0 VALUES LESS THAN (10,5),
				PARTITION p1 VALUES LESS THAN (20,10),
				PARTITION p2 VALUES LESS THAN (50,20),
				PARTITION p3 VALUES LESS THAN (65,30)
         );`,
	}

	mock := NewMockOptimizer()
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}
}

// -----------------------List Partition--------------------------------------
func TestListPartition(t *testing.T) {
	sqls := []string{
		`CREATE TABLE client_firms (
			id   INT,
			name VARCHAR(35)
		)
		PARTITION BY LIST (id) (
			PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
			PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
			PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
			PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
		);`,

		`CREATE TABLE employees (
			id INT NOT NULL,
			fname VARCHAR(30),
			lname VARCHAR(30),
			hired DATE NOT NULL DEFAULT '1970-01-01',
			separated DATE NOT NULL DEFAULT '9999-12-31',
			job_code INT,
			store_id INT
		)
		PARTITION BY LIST(store_id) (
			PARTITION pNorth VALUES IN (3,5,6,9,17),
			PARTITION pEast VALUES IN (1,2,10,11,19,20),
			PARTITION pWest VALUES IN (4,12,13,14,18),
			PARTITION pCentral VALUES IN (7,8,15,16)
		);`,

		`CREATE TABLE t1 (
			id   INT PRIMARY KEY,
			name VARCHAR(35)
		)
		PARTITION BY LIST (id) (
			PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
			PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
			PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
			PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST (a) (
			PARTITION p0 VALUES IN(NULL,NULL),
			PARTITION p1 VALUES IN( 1,2 ),
			PARTITION p2 VALUES IN( 3,1 ),
			PARTITION p3 VALUES IN( 3,3 )
		);`,
	}

	mock := NewMockOptimizer()
	for _, sql := range sqls {
		t.Log(sql)
		logicPlan, err := buildSingleStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

func TestListPartitionError(t *testing.T) {
	sqls := []string{
		`CREATE TABLE t1 (
			id   INT,
			name VARCHAR(35)
		)
		PARTITION BY LIST (id);`,

		`CREATE TABLE t2 (
			id   INT,
			name VARCHAR(35)
		)
         PARTITION BY LIST (id) (
			PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
			PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
			PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
			PARTITION r2 VALUES IN (4, 8, 12, 16, 20, 24)
		);`,

		`CREATE TABLE t1 (
			id   INT PRIMARY KEY,
			name VARCHAR(35),
			age INT unsigned
		)
		PARTITION BY LIST (age) (
			PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
			PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
			PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
			PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
		);`,
	}

	mock := NewMockOptimizer()
	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		t.Log(sql)
		t.Log(err)
		if err == nil {
			t.Fatalf("%+v", err)
		}
	}
}

// -----------------------List Columns Partition--------------------------------------
func TestListColumnsPartition(t *testing.T) {
	sqls := []string{
		`CREATE TABLE lc (
				a INT NULL,
				b INT NULL
			)
			PARTITION BY LIST COLUMNS(a,b) (
				PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
				PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
				PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
				PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
			);`,

		`CREATE TABLE customers_1 (
				first_name VARCHAR(25),
			last_name VARCHAR(25),
			street_1 VARCHAR(30),
			street_2 VARCHAR(30),
			city VARCHAR(15),
			renewal DATE
		)
			PARTITION BY LIST COLUMNS(city) (
			PARTITION pRegion_1 VALUES IN('Oskarshamn', 'Högsby', 'Mönsterås'),
			PARTITION pRegion_2 VALUES IN('Vimmerby', 'Hultsfred', 'Västervik'),
			PARTITION pRegion_3 VALUES IN('Nässjö', 'Eksjö', 'Vetlanda'),
			PARTITION pRegion_4 VALUES IN('Uppvidinge', 'Alvesta', 'Växjo')
		);`,

		`CREATE TABLE customers_2 (
			first_name VARCHAR(25),
			last_name VARCHAR(25),
			street_1 VARCHAR(30),
			street_2 VARCHAR(30),
			city VARCHAR(15),
			renewal DATE
		)
		PARTITION BY LIST COLUMNS(renewal) (
			PARTITION pWeek_1 VALUES IN('2010-02-01', '2010-02-02', '2010-02-03',
				'2010-02-04', '2010-02-05', '2010-02-06', '2010-02-07'),
			PARTITION pWeek_2 VALUES IN('2010-02-08', '2010-02-09', '2010-02-10',
				'2010-02-11', '2010-02-12', '2010-02-13', '2010-02-14'),
			PARTITION pWeek_3 VALUES IN('2010-02-15', '2010-02-16', '2010-02-17',
				'2010-02-18', '2010-02-19', '2010-02-20', '2010-02-21'),
			PARTITION pWeek_4 VALUES IN('2010-02-22', '2010-02-23', '2010-02-24',
				'2010-02-25', '2010-02-26', '2010-02-27', '2010-02-28')
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b) PARTITIONS 4 (
			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
			PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
			PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
			PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
		);`,

		`CREATE TABLE lc (
				a INT NULL,
				b INT NULL
			)
			PARTITION BY LIST COLUMNS(a,b) (
				PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
				PARTITION p1 VALUES IN( (0,1), (0,2) ),
				PARTITION p2 VALUES IN( (1,0), (2,0) )
			);`,
	}

	mock := NewMockOptimizer()
	for _, sql := range sqls {
		t.Log(sql)
		logicPlan, err := buildSingleStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

func TestListColumnsPartitionError(t *testing.T) {
	sqls := []string{
		`CREATE TABLE t1 (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b);`,

		`CREATE TABLE t2 (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b) (
			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
			PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
			PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
			PARTITION p2 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b) PARTITIONS 5 (
			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
			PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
			PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
			PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
		);`,
	}

	mock := NewMockOptimizer()
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
	}

	mock := NewMockOptimizer()
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

		//`CREATE TABLE t1 (
		//	col1 INT UNIQUE NOT NULL,
		//	col2 DATE NOT NULL,
		//	col3 INT NOT NULL,
		//	col4 INT NOT NULL
		//)
		//PARTITION BY HASH(col3)
		//PARTITIONS 4;`,

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
	}

	mock := NewMockOptimizer()
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

	mock := NewMockOptimizer()
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

	mock := NewMockOptimizer()
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
	mock := NewMockOptimizer()
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
	statements, err := mysql.Parse(sql)
	if err != nil {
		return nil, err
	}
	// this sql always return single statement
	context := opt.CurrentContext()
	return BuildPlan(context, statements[0])
}
