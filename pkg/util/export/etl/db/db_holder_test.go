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
	"fmt"
	"regexp"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	"database/sql/driver"
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

	expectedOneRow := "INSERT INTO `testDB`.`testTable` VALUES  (?,?,?)"
	if sqls.oneRow != expectedOneRow {
		t.Errorf("Expected oneRow to be %s, but got %s", expectedOneRow, sqls.oneRow)
	}
	expectedMultiRows := "INSERT INTO `testDB`.`testTable` VALUES (?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)"
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

	mock.ExpectBegin()

	stmt := mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `testDB`.`testTable` VALUES (?,?,?,?,?,?)"))

	for _, record := range records {
		driverValues := make([]driver.Value, len(record))
		for i, v := range record {
			driverValues[i] = driver.Value(v)
		}
		stmt.ExpectExec().
			WithArgs(driverValues...).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bulkInsert(ctx, db, records, tbl, 10, 1)

	err = mock.ExpectationsWereMet()
	if err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBulkInsertWithBatch(t *testing.T) {
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

	// Generate 109 records
	records := make([][]string, 109)
	for i := 0; i < 109; i++ {
		records[i] = []string{fmt.Sprintf("str%d", i+1), fmt.Sprintf("%d", i+1), fmt.Sprintf("%.1f", float64(i+1)), fmt.Sprintf("%d", i+1), "2023-05-16T00:00:00Z", fmt.Sprintf(`{"key%d":"value%d"}`, i+1, i+1)}
	}
	// Mock db
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectBegin()

	batchSize := 10
	numBatches := len(records) / batchSize

	// Expect numBatches batches
	stmt := mock.ExpectPrepare("^INSERT INTO `testDB`.`testTable` VALUES (.+)$")
	batchArgs := make([]driver.Value, batchSize*6) // Assuming 6 fields in a record
	for i := range batchArgs {
		batchArgs[i] = sqlmock.AnyArg()
	}

	for i := 0; i < numBatches; i++ {
		stmt.ExpectExec().WithArgs(batchArgs...).WillReturnResult(sqlmock.NewResult(1, int64(batchSize)))
	}

	// Expect last 9
	stmt = mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `testDB`.`testTable` VALUES (?,?,?,?,?,?)"))
	recordArgs := make([]driver.Value, 6) // Assuming 6 fields in a record
	for i := range recordArgs {
		recordArgs[i] = sqlmock.AnyArg()
	}

	for i := 0; i < 9; i++ {
		stmt.ExpectExec().WithArgs(recordArgs...).WillReturnResult(sqlmock.NewResult(1, 1))
	}

	mock.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bulkInsert(ctx, db, records, tbl, 10, 1)

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
