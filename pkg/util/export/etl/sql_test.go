// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etl

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

func TestDefaultSqlWriter_WriteRowRecords(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	db_holder.SetDBConn(db)

	// set up your DefaultSqlWriter and records
	var dummyStrColumn = table.Column{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"}

	tbl := &table.Table{
		Columns: []table.Column{dummyStrColumn},
	}
	records := [][]string{
		{"record1"},
		{"record2"},
		// add more records as needed
	}

	// call the function to test
	cnt, err := db_holder.WriteRowRecords(records, tbl, 1*time.Second)

	// assertions
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	if cnt != len(records) {
		t.Errorf("expected %d, got %d", len(records), cnt)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
