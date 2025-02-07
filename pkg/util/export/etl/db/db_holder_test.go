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
	"database/sql"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

var testWriteRowRecordsMux sync.Mutex

func SyncTestWriteRowRecords(t *testing.T, f func(t *testing.T)) {
	testWriteRowRecordsMux.Lock()
	defer testWriteRowRecordsMux.Unlock()
	f(t)
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
		{"str1", "1", "1.1", "1", "2023-05-16T00:00:00Z", `{"key1":"value1 \n test , \r 'test'"}`},
		{"str2", "2", "2.2", "2", "2023-05-16T00:00:00Z", `{"key2":"value2"}`},
	}

	db, mock, err := sqlmock.New() // creating sqlmock
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	mock.ExpectExec(regexp.QuoteMeta(`LOAD DATA INLINE FORMAT='csv', DATA='str1,1,1.1,1,2023-05-16T00:00:00Z,"{""key1"":""value1 \\n test , \\r ''test''""}"
str2,2,2.2,2,2023-05-16T00:00:00Z,"{""key2"":""value2""}"
' INTO TABLE testDB.testTable`)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bulkInsert(ctx, db, records, tbl)

	err = mock.ExpectationsWereMet()
	if err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestIsRecordExisted(t *testing.T) {
	// Create a new instance of sqlmock
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	ctx := context.TODO()
	// Assuming index 12 is for 'request_at', adding a mock value for it
	record := []string{"12345", "", "", "sys", "", "", "", "", "", "", "", "", "2021-10-10 10:00:00", "", "", "active"}
	table := &table.Table{Table: "statement_info"}

	// Set up your mock expectations
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT EXISTS(SELECT 1 FROM `system`.statement_info WHERE statement_id = ? AND status = ? AND request_at = ? AND account = ?)",
	)).WithArgs(record[0], record[15], record[12], "sys").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	// Define a function that returns the mocked DB connection
	getDBConn := func(forceNewConn bool, randomCN bool) (*sql.DB, error) {
		return db, nil
	}

	// Call your function with the mock
	exists, err := IsRecordExisted(ctx, record, table, getDBConn)
	if err != nil {
		t.Errorf("error was not expected while checking record existence: %s", err)
	}
	if !exists {
		t.Errorf("expected record to exist, but it does not")
	}

	// Ensure all expectations are met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestSetLabelSelector(t *testing.T) {
	type args struct {
		labels map[string]string
	}
	tests := []struct {
		name       string
		args       args
		want       map[string]string
		wantGetter map[string]string
	}{
		{
			name: "normal",
			args: args{labels: map[string]string{
				"role": "admin",
			}},
			want: map[string]string{
				"role": "admin",
			},
			wantGetter: map[string]string{
				"role":    "admin",
				"account": "sys",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetLabelSelector(tt.args.labels)
			require.Equal(t, tt.want, tt.args.labels)
			got := GetLabelSelector()
			if !reflect.DeepEqual(tt.wantGetter, got) {
				t.Errorf("gLabelSelector = %v, want %v", got, tt.wantGetter)
			}
		})
	}
}

func TestNewReConnectionBackOff(t *testing.T) {
	// Test with valid parameters
	backOff := NewReConnectionBackOff(1*time.Second, 3)
	if backOff.window != 1*time.Second || backOff.threshold != 3 {
		t.Errorf("Expected window 1s and threshold 3, got %v and %v", backOff.window, backOff.threshold)
	}

	// Test with zero window
	backOff = NewReConnectionBackOff(0, 3)
	if backOff.window != 0 || backOff.threshold != 3 {
		t.Errorf("Expected window 0s and threshold 3, got %v and %v", backOff.window, backOff.threshold)
	}

	// Test with negative window
	backOff = NewReConnectionBackOff(-1*time.Second, 3)
	if backOff.window != -1*time.Second || backOff.threshold != 3 {
		t.Errorf("Expected window -1s and threshold 3, got %v and %v", backOff.window, backOff.threshold)
	}

	// Test with zero threshold
	backOff = NewReConnectionBackOff(1*time.Second, 0)
	if backOff.window != 1*time.Second || backOff.threshold != 0 {
		t.Errorf("Expected window 1s and threshold 0, got %v and %v", backOff.window, backOff.threshold)
	}

	// Test with negative threshold
	backOff = NewReConnectionBackOff(1*time.Second, -1)
	if backOff.window != 1*time.Second || backOff.threshold != -1 {
		t.Errorf("Expected window 1s and threshold -1, got %v and %v", backOff.window, backOff.threshold)
	}
}

func TestCount(t *testing.T) {
	backOff := NewReConnectionBackOff(time.Minute, 2)

	// Test initial count
	got := backOff.Count()
	require.Equal(t, true, got)

	// Test count increment
	backOff.Count()
	backOff.Count()
	got = backOff.Count()
	require.Equal(t, false, got)

	// Test reset after window
	// inject: reset after window.
	backOff.last = time.Now().Add(-time.Hour)
	got = backOff.Count()
	require.Equal(t, true, got)
	require.Equal(t, 1, backOff.count)
}

func TestCheck(t *testing.T) {
	backOff := NewReConnectionBackOff(time.Minute, 2)

	// Test initial check
	got := backOff.Check()
	require.Equal(t, true, got)

	// Test check after incrementing count
	backOff.Count()
	backOff.Count()
	backOff.Count()
	got = backOff.Check()
	require.Equal(t, false, got)

	// Test reset after window
	// inject: reset after window.
	backOff.last = time.Now().Add(-time.Hour)
	got = backOff.Check()
	require.Equal(t, true, got)

	// inject count == threshold
	backOff.count = 2
	backOff.last = time.Now().Add(time.Minute)
	got = backOff.Check()
	require.Equal(t, true, got)

	// inject valid 'false' case
	backOff.count = 20
	backOff.last = time.Now()
	got = backOff.Check()
	require.Equal(t, false, got)

	// inject invalid 'last' value
	backOff.count = 20
	backOff.last = time.Now().Add(time.Hour)
	got = backOff.Check()
	require.Equal(t, true, got)
}

func Test_WriteRowRecords2(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()
		mock.ExpectExec(regexp.QuoteMeta(`LOAD DATA INLINE FORMAT='csv', DATA='record1
' INTO TABLE testDB.testTable`)).WillReturnError(moerr.NewInternalErrorNoCtx("return_err"))
		SetDBConn(db)

		// set up your DefaultSqlWriter and records
		var dummyStrColumn = table.Column{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"}

		tbl := &table.Table{
			Database: "testDB",
			Table:    "testTable",
			Columns:  []table.Column{dummyStrColumn},
		}
		records := [][]string{
			{"record1"},
			// {"record2"},
			// add more records as needed
		}

		// call the function to test
		_, err = WriteRowRecords(records, tbl, 1*time.Second)
		assert.Error(t, err)
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})
}

func TestWriteRowRecords_WithBackoff(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		old := DBConnErrCount
		defer func() {
			DBConnErrCount = old
		}()

		// set up your DefaultSqlWriter and records
		var dummyStrColumn = table.Column{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"}

		tbl := &table.Table{
			Database: "testDB",
			Table:    "testTable",
			Columns:  []table.Column{dummyStrColumn},
		}
		records := [][]string{
			{"record1"},
			// {"record2"},
			// add more records as needed
		}

		DBConnErrCount = NewReConnectionBackOff(time.Hour, 0)
		DBConnErrCount.Count()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		defer db.Close()
		mock.ExpectExec(regexp.QuoteMeta(`LOAD DATA INLINE FORMAT='csv', DATA='record1
' INTO TABLE testDB.testTable`)).WillReturnError(moerr.NewInternalErrorNoCtx("return_err"))

		newConn := false
		stubs := gostub.Stub(&GetOrInitDBConn, func(forceNewConn bool, randomCN bool) (*sql.DB, error) {
			newConn = forceNewConn
			return db, nil
		})
		defer stubs.Reset()

		// call the function to test
		_, err = WriteRowRecords(records, tbl, 1*time.Second)
		assert.Error(t, err)
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}

		require.True(t, newConn)
	})
}
