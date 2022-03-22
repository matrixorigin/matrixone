// Copyright 2021 Matrix Origin
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

package unittest

import "testing"

// TestTableFunction used to check if the process about table can be run through
func TestTableFunction(t *testing.T) {
	testCases := []testCase{
		{sql: "create table ddlt1 (a int, b int);"},
		{sql: "create table ddlt2 (orderId varchar(100), uid int, price float);"},
		{sql: "create table ddlt3 (a int, primary key(a));"},
		{sql: "show tables;", res: executeResult{
			attr: []string{"Tables"},
		}, com: "memory engine adopt a disorderly structure to store the information of tables, so we do not care the result due to unpredictable order"},
		{sql: "show tables like '%1';", res: executeResult{
			attr: []string{"Tables"},
			data: [][]string{
				{"ddlt1"},
			},
		}},
		{sql: "show columns from ddlt2;", res: executeResult{
			attr: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
			data: [][]string{
				{"orderId", "varchar(100)", "", "", "NULL", ""},
				{"uid", 	"int(32)", 		"", "", "NULL", ""},
				{"price",   "float(32)", 	"", "", "NULL", ""},
			},
		}},
		{sql: "show columns from ddlt2 like 'pri%';", res: executeResult{
			attr: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
			data: [][]string{
				{"price",   "float(32)", 	"", "", "NULL", ""},
			},
		}},
		{sql: "show create table ddlt2", res: executeResult{
			attr: []string{"Table", "Create Table"},
			data: [][]string{
				{"ddlt2", "CREATE TABLE `ddlt2` (\n `orderId` VARCHAR(100) DEFAULT NULL,\n `uid` INT(32) DEFAULT NULL,\n `price` FLOAT(32) DEFAULT NULL\n)"},
			},
		}},
		{sql: "drop table ddlt1;"},
		{sql: "drop table ddlt2, ddlt3;"},
		{sql: "drop table ddlt4;", err: "not exist", com: "this error is return by memory-engine, it's diff to real performance"},
		{sql: "drop table if exists ddlt4;"},
	}
	test(t, testCases)
}

// TestDatabaseFunction is only used to check if the whole process about database can be run through
func TestDatabaseFunction(t *testing.T) {
	// memory-engine has only one database "test", and never implement database related functions,
	// so we only test the logic of database related process
	testCases := []testCase{
		{sql: "create database if not exists d1;"},
		{sql: "show databases;", res: executeResult{
			attr: []string{"Databases"},
		}},
		{sql: "show databases like 'd_';"},
		{sql: "drop database d2;", err: "database 'd2' not exist"},
		{sql: "drop database if exists d2;"},
	}
	test(t, testCases)
}

// TestIndexFunction is only used to check if the whole process about index can be run through
func TestIndexFunction(t *testing.T) {
	testCases := []testCase{
		{sql: "create table tbl(a int, b varchar(10));"},
		{sql: "create index index_name on tbl(a);"},
		{sql: "create index index_names using bsi on tbl(a);"},
		{sql: "create index index_nameb using btree on tbl(a);", err: "[0A000]unsupported index type"},
		{sql: "create index index_nameb using hash on tbl(a);", err: "[0A000]unsupported index type"},
		{sql: "create index index_nameb using rtree on tbl(a);", err: "[0A000]unsupported index type"},
		{sql: "create index index_nameb using bsi on tbl(c);", err: "[42703]unknown column 'c'"},
		{sql: "create index index_nameb using bsi on tbl(a, b);", err: "[0A000]unsupported index type"},
		{sql: "drop index noeindex on tbl;", err: "[42602]index doesn't exist"},
	}
	test(t, testCases)
}

// Do not support this unit test now, because it for tpe engine which has supported deletion and other engine do not support.
// TestDeleteFunction is only used to check if the whole process about deletion can be run through
//func TestDeleteFunction(t *testing.T) {
//	testCases := []testCase{
//		{sql: "create table t1 (a int, b int);"},
//		{sql: "insert into t1 values (1, 2), (3, 4), (5, 6);"},
//		{sql: "delete from t1 where a > 1;"},
//		{sql: "delete from t1 where a > 1 order by b;"},
//		{sql: "delete from t1 where a > 1 limit 1;"},
//		{sql: "delete from t1 where a > 1 order by a limit 1;"},
//		{sql: "delete from t1 order by a;"},
//		{sql: "delete from t1 order by a limit 1;"},
//		{sql: "delete from t1 limit 1;"},
//	}
//	test(t, testCases)
//}