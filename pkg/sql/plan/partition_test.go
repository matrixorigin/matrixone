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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
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
			PARTITION p0 VALUES IN(0,NULL),
			PARTITION p1 VALUES IN( 1,2 ),
			PARTITION p2 VALUES IN( 3,4 ),
			PARTITION p3 VALUES IN( 5,6 )
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

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST (a) (
			PARTITION p0 VALUES IN(NULL,NULL),
			PARTITION p1 VALUES IN( 1,2 ),
			PARTITION p2 VALUES IN( 3,4 ),
			PARTITION p3 VALUES IN( 5,6 )
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

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST (a) (
			PARTITION p0 VALUES IN(0,NULL),
			PARTITION p1 VALUES IN( 1,2 ),
			PARTITION p2 VALUES IN( 3,4 ),
			PARTITION p3 VALUES LESS THAN (50,20)
		);`,

		`create table pt_table_50(
			col1 tinyint,
			col2 smallint,
			col3 int,
			col4 bigint,
			col5 tinyint unsigned,
			col6 smallint unsigned,
			col7 int unsigned,
			col8 bigint unsigned,
			col9 float,
			col10 double,
			col11 varchar(255),
			col12 Date,
			col13 DateTime,
			col14 timestamp,
			col15 bool,
			col16 decimal(5,2),
			col17 text,
			col18 varchar(255),
			col19 varchar(255),
			col20 text,
			primary key(col4,col3,col11)
			) partition by list(col3) (
			PARTITION r0 VALUES IN (1, 5*2, 9, 13, 17-20, 21),
			PARTITION r1 VALUES IN (2, 6, 10, 7, 18, 22),
			PARTITION r2 VALUES IN (3, 7, 11+6, 15, 19, 23),
			PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
			);`,

		`create table pt_table_50(
			col1 tinyint,
			col2 smallint,
			col3 int,
			col4 bigint,
			col5 tinyint unsigned,
			col6 smallint unsigned,
			col7 int unsigned,
			col8 bigint unsigned,
			col9 float,
			col10 double,
			col11 varchar(255),
			col12 Date,
			col13 DateTime,
			col14 timestamp,
			col15 bool,
			col16 decimal(5,2),
			col17 text,
			col18 varchar(255),
			col19 varchar(255),
			col20 text,
			primary key(col4,col3,col11)
			) partition by list(col3) (
			PARTITION r0 VALUES IN (1, 5*2, 9, 13, 17-20, 21),
			PARTITION r1 VALUES IN (2, 6, 10, 14/2, 18, 22),
			PARTITION r2 VALUES IN (3, 7, 11+6, 15, 19, 23),
			PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
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

func TestListPartitionFunction(t *testing.T) {
	sqls := []string{
		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b) (
			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
			PARTITION p1 VALUES IN( (0,1), (0,4+2) ),
			PARTITION p2 VALUES IN( (1,0), (2,0) )
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST(a) (
			PARTITION p0 VALUES IN(0, NULL ),
			PARTITION p1 VALUES IN(1, 2),
			PARTITION p2 VALUES IN(3, 4)
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(b) (
			PARTITION p0 VALUES IN( 0,NULL ),
			PARTITION p1 VALUES IN( 1,2 ),
			PARTITION p2 VALUES IN( 3,4 )
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(b) (
			PARTITION p0 VALUES IN( 0,NULL ),
			PARTITION p1 VALUES IN( 1,1+1 ),
			PARTITION p2 VALUES IN( 3,4 )
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

func TestListPartitionFunctionError(t *testing.T) {
	sqls := []string{
		`create table pt_table_45(
			col1 tinyint,
			col2 smallint,
			col3 int,
			col4 bigint,
			col5 tinyint unsigned,
			col6 smallint unsigned,
			col7 int unsigned,
			col8 bigint unsigned,
			col9 float,
			col10 double,
			col11 varchar(255),
			col12 Date,
			col13 DateTime,
			col14 timestamp,
			col15 bool,
			col16 decimal(5,2),
			col17 text,
			col18 varchar(255),
			col19 varchar(255),
			col20 text,
			primary key(col4,col3,col11))
		partition by list(col3) (
			PARTITION r0 VALUES IN (1, 5*2, 9, 13, 17-20, 21),
			PARTITION r1 VALUES IN (2, 6, 10, 14/2, 18, 22),
			PARTITION r2 VALUES IN (3, 7, 11+6, 15, 19, 23),
			PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b) (
			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
			PARTITION p1 VALUES IN( (0,1), (0,4/2) ),
			PARTITION p2 VALUES IN( (1,0), (2,0) )
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b) (
			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
			PARTITION p1 VALUES IN( (0,1), (0,4.2) ),
			PARTITION p2 VALUES IN( (1,0), (2,0) )
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b) (
			PARTITION p0 VALUES IN( 0,NULL ),
			PARTITION p1 VALUES IN( 0,1 ),
			PARTITION p2 VALUES IN( 1,0 )
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST(a) (
			PARTITION p0 VALUES IN(0, NULL ),
			PARTITION p1 VALUES IN(1, 4/2),
			PARTITION p2 VALUES IN(3, 4)
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(b) (
			PARTITION p0 VALUES IN( 0,NULL ),
			PARTITION p1 VALUES IN( 1,4/2 ),
			PARTITION p2 VALUES IN( 3,4 )
		);`,

		`CREATE TABLE lc (
			a INT NULL,
			b INT NULL
		)
		PARTITION BY LIST COLUMNS(a,b) (
			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
			PARTITION p1 VALUES IN( (0,1,3), (0,4,5) ),
			PARTITION p2 VALUES IN( (1,0), (2,0) )
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

func buildSingleStmt(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	statements, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		return nil, err
	}
	// this sql always return single statement
	context := opt.CurrentContext()
	plan, err := BuildPlan(context, statements[0])
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
	builder := NewQueryBuilder(plan.Query_SELECT, mock.CurrentContext())
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
	one, err := parsers.ParseOne(context.TODO(), dialect.MYSQL, selStr, 1)
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
		Typ: &plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef, &ColDef{
		Name: "col2",
		Typ: &plan.Type{
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
		Typ: &plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef2, &ColDef{
		Name: "col2",
		Typ: &plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef2, &ColDef{
		Name: "col3",
		Typ: &plan.Type{
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
		Typ: &plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef, &ColDef{
		Name: "col2",
		Typ: &plan.Type{
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

		err := checkDuplicatePartitionName(context.TODO(), nil, partDef1)
		require.Nil(t, err)
	}
	{
		partDef1 := &PartitionByDef{
			Partitions: []*plan.PartitionItem{
				{PartitionName: "p0"},
				{PartitionName: "p0"},
			},
		}

		err := checkDuplicatePartitionName(context.TODO(), nil, partDef1)
		require.NotNil(t, err)
	}
}

func Test_checkPartitionCount(t *testing.T) {
	{
		err := checkPartitionCount(context.TODO(), nil, PartitionCountLimit-10)
		require.Nil(t, err)
	}
	{
		err := checkPartitionCount(context.TODO(), nil, PartitionCountLimit+10)
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
		err := checkDuplicatePartitionColumns(context.TODO(), nil, partDef)
		require.Nil(t, err)
	}
	{
		partDef := &PartitionByDef{}
		err := checkDuplicatePartitionColumns(context.TODO(), nil, partDef)
		require.Nil(t, err)
	}
	{
		partDef := &PartitionByDef{
			PartitionColumns: &plan.PartitionColumns{
				PartitionColumns: []string{"a"},
			},
		}
		err := checkDuplicatePartitionColumns(context.TODO(), nil, partDef)
		require.Nil(t, err)
	}
	{
		partDef := &PartitionByDef{
			PartitionColumns: &plan.PartitionColumns{
				PartitionColumns: []string{"a", "a"},
			},
		}
		err := checkDuplicatePartitionColumns(context.TODO(), nil, partDef)
		require.NotNil(t, err)
	}
	{
		partDef := &PartitionByDef{
			PartitionColumns: &plan.PartitionColumns{
				PartitionColumns: []string{"a", "b"},
			},
		}
		err := checkDuplicatePartitionColumns(context.TODO(), nil, partDef)
		require.Nil(t, err)
	}
}

func Test_partition_binder(t *testing.T) {
	/*
		table test:
		col1 int32 pk
		col2 int32 pk
		col3 date pk
	*/
	tableDef := &plan.TableDef{
		Name: "test",
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"col1", "col2", "col3"},
		},
	}

	addCol(tableDef, &ColDef{
		Name: "col1",
		Typ: &plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef, &ColDef{
		Name: "col2",
		Typ: &plan.Type{
			Id: int32(types.T_int8),
		},
	})
	addCol(tableDef, &ColDef{
		Name: "col3",
		Typ: &plan.Type{
			Id: int32(types.T_date),
		},
	})

	type kase struct {
		s        string
		wantErr  bool
		wantErr2 bool
	}

	checkFunc := func(k kase) {
		//fmt.Println(k.s)
		pb, err := mockPartitionBinder(tableDef)
		require.Nil(t, err)

		astExpr, err := mockExpr(t, k.s)
		require.Nil(t, err)

		expr, err := pb.BindExpr(astExpr, 0, true)
		if !k.wantErr {
			require.Nil(t, err)
			require.NotNil(t, expr)

			partDef := &PartitionByDef{
				PartitionExpr: &plan.PartitionExpr{
					Expr: expr,
				},
			}

			err = checkPartitionExprType(context.TODO(), nil, nil, partDef)
			if !k.wantErr2 {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
			}

		} else {
			require.NotNil(t, err)
		}
	}

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

	supportedFuncCases := []kase{
		{"abs(col1)", false, false},
		{"abs(-1)", false, true},
		{"ceiling(col1)", false, false},
		{"ceiling(0.1)", false, true},
		{"datediff('2007-12-31 23:59:59','2007-12-30')", false, false},
		{"datediff(col3,'2007-12-30')", false, false},
		{"day(col3)", false, false},
		{"dayofyear(col3)", false, false},
		{"extract(year from col3)", false, false},
		{"extract(year_month from col3)", false, false},
		{"floor(col1)", false, false},
		{"floor(0.1)", false, true},
		{"hour(col3)", false, false},
		{"minute(col3)", false, false},
		{"mod(col1,3)", false, false},
		{"month(col3)", false, false},
		{"second(col3)", false, false},
		{"unix_timestamp(col3)", false, false},
		{"weekday(col3)", false, false},
		{"year(col3)", false, false},
		{"to_days(col3)", false, false},
		{"to_seconds(col3)", false, false},
	}
	for _, k := range supportedFuncCases {
		checkFunc(k)
	}

	unsupportedFuncCases := []kase{
		{"dayofmonth(col3)", true, false},  //unsupported function
		{"dayofweek(col3)", true, false},   //unsupported function
		{"microsecond(col3)", true, false}, //unsupported function
		{"quarter(col3)", true, false},     //unsupported function
		{"time_to_sec(col3)", true, false},
		{"yearweek(col3)", true, false},
	}
	for _, k := range unsupportedFuncCases {
		checkFunc(k)
	}
}
