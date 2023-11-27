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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreateListPartitionTable(t *testing.T) {
	//sql := `CREATE TABLE w_videos_partition (
	//		  id bigint(20) NOT NULL AUTO_INCREMENT,
	//		  create_time char(12) NOT NULL DEFAULT '' COMMENT '创建时间戳',
	//		  created_at datetime NOT NULL COMMENT '创建时间',
	//		  updated_at datetime DEFAULT NULL COMMENT '更新时间',
	//		  content text,
	//		  event_start char(12) NOT NULL DEFAULT '' COMMENT '事件开始时间戳',
	//		  event_end char(12) NOT NULL DEFAULT '' COMMENT '事件结束时间戳',
	//		  msg_id varchar(32) NOT NULL DEFAULT '',
	//		  event_id varchar(32) NOT NULL DEFAULT '',
	//		  accept tinyint(4) NOT NULL DEFAULT '0',
	//		  PRIMARY KEY (id,created_at)
	//		)
	//		PARTITION BY LIST ((TO_DAYS(created_at)*24 + HOUR(created_at)) % (7*24))(
	//		 PARTITION hour0 VALUES IN (0),
	//		 PARTITION hour1 VALUES IN (1),
	//		 PARTITION hour2 VALUES IN (2),
	//		 PARTITION hour3 VALUES IN (3),
	//		 PARTITION hour4 VALUES IN (4)
	//		);`

	//sql := `CREATE TABLE t2 (
	//		id   INT,
	//		name VARCHAR(35)
	//	)
	//    PARTITION BY LIST (id) (
	//		PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),
	//		PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),
	//		PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),
	//		PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24)
	//	);`
	//sql := `CREATE TABLE lc (
	//			a INT NULL,
	//			b INT NULL
	//		)
	//		PARTITION BY LIST COLUMNS(a,b) (
	//			PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
	//			PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
	//			PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
	//			PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
	//		);`

	sql := `CREATE TABLE lc (
				a INT NULL,
				b INT NULL
			)
			PARTITION BY LIST COLUMNS(a) (
				PARTITION p0 VALUES IN( -1, NULL),
				PARTITION p1 VALUES IN( 0, 1),
				PARTITION p2 VALUES IN( 2, 3),
				PARTITION p3 VALUES IN( 4, 5)
			);`

	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
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

		`CREATE TABLE w_videos_partition (
			  id bigint(20) NOT NULL AUTO_INCREMENT,
			  create_time char(12) NOT NULL DEFAULT '' COMMENT '创建时间戳',
			  created_at datetime NOT NULL COMMENT '创建时间',
			  updated_at datetime DEFAULT NULL COMMENT '更新时间',
			  content text,
			  event_start char(12) NOT NULL DEFAULT '' COMMENT '事件开始时间戳',
			  event_end char(12) NOT NULL DEFAULT '' COMMENT '事件结束时间戳',
			  msg_id varchar(32) NOT NULL DEFAULT '',
			  event_id varchar(32) NOT NULL DEFAULT '',
			  accept tinyint(4) NOT NULL DEFAULT '0',
			  PRIMARY KEY (id,created_at)
			)
			PARTITION BY LIST ((TO_DAYS(created_at)*24 + HOUR(created_at)) % (7*24))( 
			 PARTITION hour0 VALUES IN (0),
			 PARTITION hour1 VALUES IN (1),
			 PARTITION hour2 VALUES IN (2),
			 PARTITION hour3 VALUES IN (3),
			 PARTITION hour4 VALUES IN (4)
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

		`CREATE TABLE lc (
				a INT NULL,
				b INT NULL
			)
			PARTITION BY LIST COLUMNS(a,a) (
				PARTITION p0 VALUES IN( (0,0), (NULL,NULL) ),
				PARTITION p1 VALUES IN( (0,1), (0,2), (0,3), (1,1), (1,2) ),
				PARTITION p2 VALUES IN( (1,0), (2,0), (2,1), (3,0), (3,1) ),
				PARTITION p3 VALUES IN( (1,3), (2,2), (2,3), (3,2), (3,3) )
			);`,

		`CREATE TABLE lc (
				a INT NULL,
				b INT NULL
			)
			PARTITION BY LIST COLUMNS(a) (
				PARTITION p0 VALUES IN( -1, NULL),
				PARTITION p1 VALUES IN( 1, 1),
				PARTITION p2 VALUES IN( 2, 3),    
				PARTITION p3 VALUES IN( 4, 5)
			);`,
		`CREATE TABLE lc (
				a INT NULL,
				b INT NULL
			)
			PARTITION BY LIST COLUMNS(a,b) (
				PARTITION p0 VALUES IN( 0, 1, NULL),
				PARTITION p1 VALUES IN( 2, 3, 4 ),
				PARTITION p2 VALUES IN( 5, 6, 7 ),    
				PARTITION p3 VALUES IN( 8, 9, 10)
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

func TestCreateTableWithListPartition(t *testing.T) {
	type errorCase struct {
		sql       string
		errorCode uint16
	}

	cases := []errorCase{
		{
			"create table t (id timestamp) partition by list (id) (partition p0 values in ('2019-01-09 11:23:34'));",
			moerr.ErrValuesIsNotIntType,
		},
		{
			"create table t (id int) partition by list (id);",
			moerr.ErrPartitionsMustBeDefined,
		},
		{
			"create table t (a int) partition by list (b) (partition p0 values in (1));",
			moerr.ErrBadFieldError,
		},
		{
			"create table t (id decimal) partition by list (id) (partition p0 values in ('2019-01-09 11:23:34'));",
			moerr.ErrValuesIsNotIntType,
		},
		{
			"create table t (id float) partition by list (id) (partition p0 values in (1));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (id double) partition by list (id) (partition p0 values in (1));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (id text) partition by list (id) (partition p0 values in ('abc'));",
			moerr.ErrValuesIsNotIntType,
		},
		{
			"create table t (id blob) partition by list (id) (partition p0 values in ('abc'));",
			moerr.ErrValuesIsNotIntType,
		},
		{
			"create table t (id enum('a','b')) partition by list (id) (partition p0 values in ('a'));",
			moerr.ErrValuesIsNotIntType,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (1), partition P0 values in (2));",
			moerr.ErrSameNamePartition,
		},
		{
			"create table t (id bigint) partition by list (cast(id as unsigned)) (partition p0 values in (1))",
			moerr.ErrPartitionFunctionIsNotAllowed,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (1), partition p0 values in (2));",
			moerr.ErrSameNamePartition,
		},
		{
			"create table t (id float) partition by list (ceiling(id)) (partition p0 values in (1))",
			moerr.ErrPartitionFuncNotAllowed,
		},
		{
			"create table t (a date) partition by list (to_days(to_days(a))) (partition p0 values in (1), partition P1 values in (2));",
			moerr.ErrWrongExprInPartitionFunc,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (1), partition p1 values in (1));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (1), partition p1 values in (+1));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list (a) (partition p0 values in (null), partition p1 values in (NULL));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			`create table t1 (id int key, name varchar(10), unique index idx(name)) partition by list  (id) (
				    partition p0 values in (3,5,6,9,17),
				    partition p1 values in (1,2,10,11,19,20),
				    partition p2 values in (4,12,13,14,18),
				    partition p3 values in (7,8,15,16)
				);`,
			moerr.ErrUniqueKeyNeedAllFieldsInPf,
		},
		{
			`CREATE TABLE t2 (id INT, name VARCHAR(35))
			PARTITION BY LIST (id) (
			PARTITION r0 VALUES IN (1, 5, MAXVALUE),
			PARTITION r1 VALUES IN (2, 6, 10)
			);`,
			moerr.ErrMaxvalueInValuesIn,
		},
		{
			`CREATE TABLE t2 (id INT, name VARCHAR(35))  PARTITION BY LIST (id) ( PARTITION r0 VALUES IN ((1, 4), (5, 6)),  PARTITION r1 VALUES IN ((2, 3), (4, 7)) );`,
			moerr.ErrRowSinglePartitionField,
		},
	}

	mock := NewMockOptimizer(false)
	for i, tt := range cases {
		_, err := buildSingleStmt(mock, t, tt.sql)
		require.Truef(t, moerr.IsMoErrCode(err, tt.errorCode),
			"case %d failed, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, tt.sql, tt.errorCode, err,
		)
	}

	validCases := []string{
		"create table t (a int) partition by list (a) (partition p0 values in (1));",
		"create table t (a bigint unsigned) partition by list (a) (partition p0 values in (18446744073709551615));",
		//"create table t (a bigint unsigned) partition by list (a) (partition p0 values in (18446744073709551615 - 1));",
		"create table t (a int) partition by list (a) (partition p0 values in (1,null));",
		"create table t (a int) partition by list (a) (partition p0 values in (1), partition p1 values in (2));",
		`create table t (id int, name varchar(10), age int) partition by list (id) (
			partition p0 values in (3,5,6,9,17),
			partition p1 values in (1,2,10,11,19,21),
			partition p2 values in (4,12,13,-14,18),
			partition p3 values in (7,8,15,+16,20)
		);`,
		"create table t (a tinyint) partition by list (a) (partition p0 values in (65536));",
		"create table t (a tinyint) partition by list (a*100) (partition p0 values in (65536));",
		"create table t(a binary) partition by list columns (a) (partition p0 values in (X'0C'));",
		"create table t (a bigint) partition by list (a) (partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')));",
		"create table t (a datetime) partition by list (to_seconds(a)) (partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')));",
	}

	//mock := NewMockOptimizer(false)
	for i, sql := range validCases {
		_, err := buildSingleStmt(mock, t, sql)
		require.Truef(t, err == nil,
			"case %d failed, sql = `%s`\n  actual error = `%v`",
			i, sql, err,
		)
	}

}

func TestCreateTableWithListColumnsPartition(t *testing.T) {
	type errorCase struct {
		sql       string
		errorCode uint16
	}
	cases := []errorCase{
		{
			"create table t (id int) partition by list columns (id);",
			moerr.ErrPartitionsMustBeDefined,
		},
		{
			"create table t (a int) partition by list columns (b) (partition p0 values in (1));",
			moerr.ErrFieldNotFoundPart,
		},
		{
			"create table t (id timestamp) partition by list columns (id) (partition p0 values in ('2019-01-09 11:23:34'));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (id decimal) partition by list columns (id) (partition p0 values in ('2019-01-09 11:23:34'));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (id float) partition by list columns (id) (partition p0 values in (1));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (id double) partition by list columns (id) (partition p0 values in (1));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (id text) partition by list columns (id) (partition p0 values in ('abc'));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (id blob) partition by list columns (id) (partition p0 values in ('abc'));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (id enum('a','b')) partition by list columns (id) (partition p0 values in ('a'));",
			moerr.ErrFieldTypeNotAllowedAsPartitionField,
		},
		{
			"create table t (a varchar(2)) partition by list columns (a) (partition p0 values in ('abc'));",
			moerr.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a tinyint) partition by list columns (a) (partition p0 values in (65536));",
			moerr.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a bigint) partition by list columns (a) (partition p0 values in (18446744073709551615));",
			moerr.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a bigint unsigned) partition by list columns (a) (partition p0 values in (-1));",
			moerr.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a char) partition by list columns (a) (partition p0 values in ('abc'));",
			moerr.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a datetime) partition by list columns (a) (partition p0 values in ('2020-11-31 12:00:00'));",
			moerr.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition p0 values in (2));",
			moerr.ErrSameNamePartition,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition P0 values in (2));",
			moerr.ErrSameNamePartition,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition p1 values in (1));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition p1 values in (+1));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a tinyint) partition by list columns (a) (partition p0 values in (1), partition p1 values in (+1));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a mediumint) partition by list columns (a) (partition p0 values in (1), partition p1 values in (+1));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a bigint) partition by list columns (a) (partition p0 values in (1), partition p1 values in (+1));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a bigint) partition by list columns (a) (partition p0 values in (1,+1))",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int) partition by list columns (a) (partition p0 values in (null), partition p1 values in (NULL));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a bigint, b int) partition by list columns (a,b) (partition p0 values in ((1,2),(1,2)))",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a bigint, b int) partition by list columns (a,b) (partition p0 values in ((1,1),(2,2)), partition p1 values in ((+1,1)));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t1 (a int, b int) partition by list columns(a,a) ( partition p values in ((1,1)));",
			moerr.ErrSameNamePartitionField,
		},
		{
			"create table t1 (a int, b int) partition by list columns(a,b,b) ( partition p values in ((1,1,1)));",
			moerr.ErrSameNamePartitionField,
		},
		{
			`create table t1 (id int key, name varchar(10), unique index idx(name)) partition by list columns (id) (
				    partition p0 values in (3,5,6,9,17),
				    partition p1 values in (1,2,10,11,19,20),
				    partition p2 values in (4,12,13,14,18),
				    partition p3 values in (7,8,15,16)
				);`,
			moerr.ErrUniqueKeyNeedAllFieldsInPf,
		},
		{
			"create table t (a date) partition by list columns (a) (partition p0 values in ('2020-02-02'), partition p1 values in ('20200202'));",
			moerr.ErrMultipleDefConstInListPart,
		},
		{
			"create table t (a int, b varchar(10)) partition by list columns (a,b) (partition p0 values in (1));",
			moerr.ErrPartitionColumnList,
		},
		{
			"create table t (a int, b varchar(10)) partition by list columns (a,b) (partition p0 values in (('ab','ab')));",
			moerr.ErrWrongTypeColumnValue,
		},
		{
			"create table t (a int, b datetime) partition by list columns (a,b) (partition p0 values in ((1)));",
			moerr.ErrPartitionColumnList,
		},
		{
			"create table t(b int) partition by hash ( b ) partitions 3 (partition p1, partition p2, partition p2);",
			moerr.ErrSameNamePartition,
		},
		{
			`CREATE TABLE t( a INT NULL, b INT NULL ) PARTITION BY LIST COLUMNS(a) ( PARTITION p1 VALUES IN( 0, maxvalue), PARTITION p2 VALUES IN( 2, 3), PARTITION p3 VALUES IN( 4, 5));`,
			moerr.ErrMaxvalueInValuesIn,
		},
		{
			`create table pt_table_21(
			col1 int,
			col2 int,
			col3 int,
			col4 int,
			col5 int,
			col6 int,
			col7 int,
			col8 int,
			col9 int,
			col10 int,
			col11 int,
			col12 int,
			col13 int,
			col14 int,
			col15 int,
			col16 int,
			col17 int,
			col18 int,
			col19 int,
			col20 int,
			col21 int
			) PARTITION BY LIST COLUMNS(col1,col2,col3, col4,col5 ,col6 ,col7 ,col8 ,col9 ,col10,col11,col12,col13,col14,col15,col16,col17) (
				PARTITION p1 VALUES IN( (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1), (2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2)),
				PARTITION p2 VALUES IN( (3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3), (4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4))
			);`,
			moerr.ErrTooManyPartitionFuncFields,
		},
	}

	mock := NewMockOptimizer(false)
	for i, tt := range cases {
		_, err := buildSingleStmt(mock, t, tt.sql)
		require.Truef(t, moerr.IsMoErrCode(err, tt.errorCode),
			"error test case %d failed, sql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, tt.sql, tt.errorCode, err,
		)
	}

	validCases := []string{
		"create table t (a int) partition by list columns (a) (partition p0 values in (1));",
		"create table t (a bigint unsigned) partition by list columns (a) (partition p0 values in (18446744073709551615));",
		//"create table t (a bigint unsigned) partition by list columns (a) (partition p0 values in (18446744073709551615 - 1));",
		"create table t (a int) partition by list columns (a) (partition p0 values in (1,null));",
		"create table t (a int) partition by list columns (a) (partition p0 values in (1), partition p1 values in (2));",
		`create table t (id int, name varchar(10), age int) partition by list columns (id) (
			partition p0 values in (3,5,6,9,17),
			partition p1 values in (1,2,10,11,19,20),
			partition p2 values in (4,12,13,-14,18),
			partition p3 values in (7,8,15,+16)
		);`,
		"create table t (a datetime) partition by list columns (a) (partition p0 values in ('2020-09-28 17:03:38','2020-09-28 17:03:39'));",
		"create table t (a date) partition by list columns (a) (partition p0 values in ('2020-09-28','2020-09-29'));",
		"create table t (a bigint, b date) partition by list columns (a,b) (partition p0 values in ((1,'2020-09-28'),(1,'2020-09-29')));",
		"create table t (a bigint)   partition by list columns (a) (partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')));",
		"create table t (a varchar(10)) partition by list columns (a) (partition p0 values in ('abc'));",
		"create table t (a char) partition by list columns (a) (partition p0 values in ('a'));",
		"create table t (a bool) partition by list columns (a) (partition p0 values in (1));",
		"create table t (c1 bool, c2 tinyint, c3 int, c4 bigint, c5 datetime, c6 date,c7 varchar(10), c8 char) " +
			"partition by list columns (c1,c2,c3,c4,c5,c6,c7,c8) (" +
			"partition p0 values in ((1,2,3,4,'2020-11-30 00:00:01', '2020-11-30','abc','a')));",
		"create table t(a int,b char(10)) partition by list columns (a, b) (partition p1 values in ((2, 'a'), (1, 'b')), partition p2 values in ((2, 'b')));",
	}

	//mock := NewMockOptimizer(false)
	for i, sql := range validCases {
		_, err := buildSingleStmt(mock, t, sql)
		require.Truef(t, err == nil,
			"valid test case %d failed, sql = `%s`\n  actual error = `%v`", i, sql, err,
		)
	}
}
