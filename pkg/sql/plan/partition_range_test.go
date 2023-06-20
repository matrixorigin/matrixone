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

		//`CREATE TABLE quarterly_report_status (
		//	report_id INT NOT NULL,
		//	report_status VARCHAR(20) NOT NULL,
		//	report_updated TIMESTAMP NOT NULL
		//)
		//	PARTITION BY RANGE ( UNIX_TIMESTAMP(report_updated) ) (
		//	PARTITION p0 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-01-01 00:00:00') ),
		//	PARTITION p1 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-04-01 00:00:00') ),
		//	PARTITION p2 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-07-01 00:00:00') ),
		//	PARTITION p3 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-10-01 00:00:00') ),
		//	PARTITION p4 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-01-01 00:00:00') ),
		//	PARTITION p5 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-04-01 00:00:00') ),
		//	PARTITION p6 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-07-01 00:00:00') ),
		//	PARTITION p7 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-10-01 00:00:00') ),
		//	PARTITION p8 VALUES LESS THAN ( UNIX_TIMESTAMP('2010-01-01 00:00:00') ),
		//	PARTITION p9 VALUES LESS THAN (MAXVALUE)
		//);`,
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

func TestRangePartitionError(t *testing.T) {
	sqls := []string{
		`create table t31 (a int not null) partition by range( a );`,
		`create table t32 (a int not null) partition by range columns( a );`,
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

		`CREATE TABLE rc (
			a INT NOT NULL,
			b INT NOT NULL
		)
		PARTITION BY RANGE COLUMNS(a,b) (
			PARTITION p0 VALUES LESS THAN (10,5),
			PARTITION p1 VALUES IN( 1,2 ),
			PARTITION p2 VALUES LESS THAN (50,20),
			PARTITION p3 VALUES LESS THAN (65,30)
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

func TestRangePartitionFunctionError(t *testing.T) {
	sqls := []string{
		`CREATE TABLE r1 (
			a INT,
			b INT
		)
		PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (5/2),
			PARTITION p1 VALUES LESS THAN (MAXVALUE)
		);`,

		`CREATE TABLE r1 (
			a INT,
			b INT
		)
		PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (5.2),
			PARTITION p1 VALUES LESS THAN (12)
		);`,

		`CREATE TABLE r1 (
			a INT,
			b FLOAT
		)
		PARTITION BY RANGE (b) (
			PARTITION p0 VALUES LESS THAN (12),
			PARTITION p1 VALUES LESS THAN (MAXVALUE)
		);`,
		//`create TABLE t1 (
		//	col1 int,
		//	col2 float
		//)
		//partition by range( case when col1 > 0 then 10 else 20 end ) (
		//	partition p0 values less than (2),
		//	partition p1 values less than (6)
		//);`,
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
