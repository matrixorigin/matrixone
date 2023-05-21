package etl

import (
	"context"
	"testing"

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
	ctx := context.Background()
	var dummyStrColumn = table.Column{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"}

	tbl := &table.Table{
		Columns: []table.Column{dummyStrColumn},
	}
	sw := NewSqlWriter(ctx, tbl, nil) // fill in the appropriate CSVWriter
	records := [][]string{
		{"record1"},
		{"record2"},
		// add more records as needed
	}

	// call the function to test
	cnt, err := sw.WriteRowRecords(records, tbl, false)

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
