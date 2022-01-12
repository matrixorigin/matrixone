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
	e, proc := newTestEngine()

	noError := []string{
		"create table ddlt1 (a int, b int);",
		"create table ddlt2 (orderId varchar(100), uid int, price float);",
		"create table ddlt3 (a int, primary key(a));",
		"show tables;",
		"show columns from ddlt2;",
		"show tables like '%1';",
		"show columns from ddlt2 like 'pri%';",
		"show create table ddlt2",
		"drop table ddlt1, ddlt2;",
	}

	test(t, e, proc, noError, nil, nil)
}

// TestDatabaseFunction is only used to check if the whole process about database can be run through
func TestDatabaseFunction(t *testing.T) {
	e, proc := newTestEngine()
	noErrors := []string{
		"create database if not exists d1;",
		"show databases;",
		"show databases like 'd_';",
		"drop database if exists d1;",
	}
	test(t, e, proc, noErrors, nil, nil)
}

// TestIndexFunction is only used to check if the whole process about index can be run through
func TestIndexFunction(t *testing.T) {
	e, proc := newTestEngine()
	noErrors := []string{
		"create table tbl(a int, b varchar(10));",
		"create index index_name on tbl(a);",
		"create index index_names using bsi on tbl(a);",
	}
	retErrors := [][]string{
		{"create index index_nameb using btree on tbl(a);", "[0A000]unsupported index type"},
		{"create index index_nameb using hash on tbl(a);", "[0A000]unsupported index type"},
		{"create index index_nameb using rtree on tbl(a);", "[0A000]unsupported index type"},
		{"create index index_nameb using bsi on tbl(c);", "[42703]unknown column 'c'"},
		{"create index index_nameb using bsi on tbl(a, b);", "[0A000]unsupported index type"},
		{"drop index noeindex on tbl;", "[42602]index doesn't exist"},
	}
	test(t, e, proc, noErrors, nil, retErrors)
}