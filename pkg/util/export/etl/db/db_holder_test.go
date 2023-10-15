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

package db_holder

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGetPrepareSQL(t *testing.T) {
	tbl := &table.Table{
		Database: "testDB",
		Table:    "testTable",
	}
	columns := 3
	maxRowLen := 10
	middleRowLen := 2

	sqls := getPrepareSQL(tbl, columns, maxRowLen, middleRowLen)

	if sqls.maxRowNum != maxRowLen {
		t.Errorf("Expected rowNum to be %d, but got %d", maxRowLen, sqls.maxRowNum)
	}

	if sqls.columns != columns {
		t.Errorf("Expected columns to be %d, but got %d", columns, sqls.columns)
	}

	expectedOneRow := "INSERT INTO `testDB`.`testTable` () VALUES  (?,?,?)"
	if sqls.oneRow != expectedOneRow {
		t.Errorf("Expected oneRow to be %s, but got %s", expectedOneRow, sqls.oneRow)
	}
	expectedMultiRows := "INSERT INTO `testDB`.`testTable` () VALUES (?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)"
	if sqls.maxRows != expectedMultiRows {
		t.Errorf("Expected multiRows to be %s, but got %s", expectedMultiRows, sqls.maxRows)
	}
}

func TestBulkInsert(t *testing.T) {

	tbl := &table.Table{
		Account:  "test",
		Database: "testDB",
		Table:    "testTable",
		Columns: []table.Column{
			{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"},
			{Name: "int64", ColType: table.TInt64, Default: "0", Comment: "int64 column"},
			{Name: "float64", ColType: table.TFloat64, Default: "0.0", Comment: "float64 column"},
			{Name: "uint64", ColType: table.TUint64, Default: "0", Comment: "uint64 column"},
			{Name: "datetime_6", ColType: table.TDatetime, Default: "", Comment: "datetime.6 column"},
			{Name: "json_col", ColType: table.TJson, Default: "{}", Comment: "json column"},
		},
	}

	records := [][]string{
		{"str1", "1", "1.1", "1", "2023-05-16T00:00:00Z", `{"key1":"value1"}`},
		{"str2", "2", "2.2", "2", "2023-05-16T00:00:00Z", `{"key2":"value2"}`},
	}

	db, mock, err := sqlmock.New() // creating sqlmock
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectExec("LOAD DATA INLINE FORMAT='csv', DATA='str1,1,1.1,1,2023-05-16T00:00:00Z,\"{\"\"key1\"\":\"\"value1\"\"}\"\nstr2,2,2.2,2,2023-05-16T00:00:00Z,\"{\"\"key2\"\":\"\"value2\"\"}\"\n' INTO TABLE testDB.testTable")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bulkInsert(ctx, db, records, tbl, 10, 1)

	err = mock.ExpectationsWereMet()
	if err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
