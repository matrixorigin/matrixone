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
	"errors"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
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

func setDBConnBackoffExceeded(t *testing.T) {
	t.Helper()

	old := DBConnErrCount
	DBConnErrCount = NewReConnectionBackOff(time.Hour, 0)
	DBConnErrCount.Count()
	t.Cleanup(func() {
		DBConnErrCount = old
	})
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
	// Updated indices: account_id at index 4, request_at at index 13, status at index 16
	record := []string{"12345", "", "", "sys", "0", "", "", "", "", "", "", "", "", "2021-10-10 10:00:00", "", "", "active"}
	table := &table.Table{Table: "statement_info"}

	// Set up your mock expectations
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT EXISTS(SELECT 1 FROM `system`.statement_info WHERE statement_id = ? AND status = ? AND request_at = ? AND account_id = ?)",
	)).WithArgs(record[0], record[16], record[13], uint32(0)).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

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

func TestReset(t *testing.T) {
	backOff := NewReConnectionBackOff(time.Hour, 0)
	backOff.Count()
	require.False(t, backOff.Check())

	backOff.Reset()
	require.True(t, backOff.Check())
	require.Equal(t, 0, backOff.count)
}

func TestCloseDBConnClearsGlobalPointer(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		CloseDBConn()
		dbConn, _, err := sqlmock.New()
		require.NoError(t, err)

		SetDBConn(dbConn)
		require.Same(t, dbConn, db.Load())

		CloseDBConn()
		require.Nil(t, db.Load())
		require.Error(t, dbConn.Ping())
	})
}

func TestGetOrInitDBConn_ForceReconnectSuccessResetsBackoff(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		setDBConnBackoffExceeded(t)
		require.False(t, DBConnErrCount.Check())

		CloseDBConn()
		dbConn, _, err := sqlmock.New()
		require.NoError(t, err)
		SetDBConn(dbConn)
		defer CloseDBConn()

		newDBConn, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		mock.ExpectPing()

		SetSQLWriterDBUser("user", "password")
		SetSQLWriterDBAddressFunc(func(context.Context, bool) (string, error) {
			return "127.0.0.1:6001", nil
		})

		stubs := gostub.Stub(&openDBConn, func(string, string) (*sql.DB, error) {
			return newDBConn, nil
		})
		defer stubs.Reset()

		got, err := GetOrInitDBConn(true, true)
		require.NoError(t, err)
		require.Same(t, newDBConn, got)
		require.True(t, DBConnErrCount.Check())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGetOrInitDBConn_ConcurrentForceReconnectBuildsOnce(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		setDBConnBackoffExceeded(t)

		CloseDBConn()
		dbConn, _, err := sqlmock.New()
		require.NoError(t, err)
		SetDBConn(dbConn)
		defer CloseDBConn()

		newDBConn, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		mock.ExpectPing()

		SetSQLWriterDBUser("user", "password")
		SetSQLWriterDBAddressFunc(func(context.Context, bool) (string, error) {
			return "127.0.0.1:6001", nil
		})

		firstOpenStarted := make(chan struct{})
		releaseOpen := make(chan struct{})
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(releaseOpen) })

		var openCount atomic.Int32
		stubs := gostub.Stub(&openDBConn, func(string, string) (*sql.DB, error) {
			if openCount.Add(1) == 1 {
				close(firstOpenStarted)
				<-releaseOpen
				return newDBConn, nil
			}
			return nil, errors.New("unexpected extra rebuild")
		})
		defer stubs.Reset()

		const callers = 8
		results := make(chan *sql.DB, callers)
		errs := make(chan error, callers)
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := GetOrInitDBConn(true, true)
			results <- conn
			errs <- err
		}()

		select {
		case <-firstOpenStarted:
		case <-time.After(time.Second):
			t.Fatal("first reconnect did not start")
		}

		for i := 1; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := GetOrInitDBConn(true, true)
				results <- conn
				errs <- err
			}()
		}

		releaseOnce.Do(func() { close(releaseOpen) })
		wg.Wait()
		close(results)
		close(errs)

		for err := range errs {
			require.NoError(t, err)
		}
		for conn := range results {
			require.Same(t, newDBConn, conn)
		}
		require.Equal(t, int32(1), openCount.Load())
		require.True(t, DBConnErrCount.Check())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGetOrInitDBConn_RebuildFailureKeepsCurrentConn(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		setDBConnBackoffExceeded(t)

		CloseDBConn()
		dbConn, _, err := sqlmock.New()
		require.NoError(t, err)
		SetDBConn(dbConn)
		defer CloseDBConn()

		SetSQLWriterDBUser("user", "password")
		wantErr := errors.New("address failed")
		SetSQLWriterDBAddressFunc(func(context.Context, bool) (string, error) {
			return "", wantErr
		})

		got, err := GetOrInitDBConn(true, true)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, got)
		require.Same(t, dbConn, db.Load())
		require.NoError(t, dbConn.Ping())
	})
}

func TestGetOrInitDBConn_OpenFailureKeepsCurrentConn(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		setDBConnBackoffExceeded(t)

		CloseDBConn()
		dbConn, _, err := sqlmock.New()
		require.NoError(t, err)
		SetDBConn(dbConn)
		defer CloseDBConn()

		SetSQLWriterDBUser("user", "password")
		SetSQLWriterDBAddressFunc(func(context.Context, bool) (string, error) {
			return "127.0.0.1:6001", nil
		})

		wantErr := errors.New("open failed")
		stubs := gostub.Stub(&openDBConn, func(string, string) (*sql.DB, error) {
			return nil, wantErr
		})
		defer stubs.Reset()

		got, err := GetOrInitDBConn(true, true)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, got)
		require.Same(t, dbConn, db.Load())
		require.NoError(t, dbConn.Ping())
	})
}

func TestGetOrInitDBConn_NewConnPingFailureKeepsCurrentConn(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		setDBConnBackoffExceeded(t)

		CloseDBConn()
		dbConn, _, err := sqlmock.New()
		require.NoError(t, err)
		SetDBConn(dbConn)
		defer CloseDBConn()

		newDBConn, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		wantErr := errors.New("ping failed")
		mock.ExpectPing().WillReturnError(wantErr)
		mock.ExpectClose()

		SetSQLWriterDBUser("user", "password")
		SetSQLWriterDBAddressFunc(func(context.Context, bool) (string, error) {
			return "127.0.0.1:6001", nil
		})

		stubs := gostub.Stub(&openDBConn, func(string, string) (*sql.DB, error) {
			return newDBConn, nil
		})
		defer stubs.Reset()

		got, err := GetOrInitDBConn(true, true)
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, got)
		require.Same(t, dbConn, db.Load())
		require.NoError(t, dbConn.Ping())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGetOrInitDBConn_RefreshFailureUsesCurrentConn(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		CloseDBConn()
		dbConn, _, err := sqlmock.New()
		require.NoError(t, err)
		SetDBConn(dbConn)
		dbRefreshTime = time.Now().Add(-time.Second)
		defer CloseDBConn()

		SetSQLWriterDBUser("user", "password")
		SetSQLWriterDBAddressFunc(func(context.Context, bool) (string, error) {
			return "", errors.New("address failed")
		})

		got, err := GetOrInitDBConn(false, false)
		require.NoError(t, err)
		require.Same(t, dbConn, got)
		require.Same(t, dbConn, db.Load())
		require.NoError(t, dbConn.Ping())
	})
}

func TestGetOrInitDBConn_RefreshInProgressUsesCurrentConn(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		CloseDBConn()
		dbConn, _, err := sqlmock.New()
		require.NoError(t, err)
		SetDBConn(dbConn)
		dbRefreshTime = time.Now().Add(-time.Second)
		defer CloseDBConn()

		newDBConn, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		mock.ExpectPing()

		SetSQLWriterDBUser("user", "password")
		SetSQLWriterDBAddressFunc(func(context.Context, bool) (string, error) {
			return "127.0.0.1:6001", nil
		})

		started := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })

		stubs := gostub.Stub(&openDBConn, func(string, string) (*sql.DB, error) {
			close(started)
			<-release
			return newDBConn, nil
		})
		defer stubs.Reset()

		done := make(chan struct{})
		var refreshConn *sql.DB
		var refreshErr error
		go func() {
			refreshConn, refreshErr = GetOrInitDBConn(false, false)
			close(done)
		}()

		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("refresh did not start")
		}

		start := time.Now()
		got, err := GetOrInitDBConn(false, false)
		require.NoError(t, err)
		require.Same(t, dbConn, got)
		require.Less(t, time.Since(start), 100*time.Millisecond)

		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("refresh did not finish")
		}
		require.NoError(t, refreshErr)
		require.Same(t, newDBConn, refreshConn)
		require.Same(t, newDBConn, db.Load())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestWriteRowRecords_ResetBackoffAfterSuccessfulWrite(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		old := DBConnErrCount
		defer func() {
			DBConnErrCount = old
		}()
		DBConnErrCount = NewReConnectionBackOff(time.Hour, 0)
		DBConnErrCount.Count()

		dbConn, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer dbConn.Close()

		for i := 0; i < 3; i++ {
			mock.ExpectExec(regexp.QuoteMeta(`LOAD DATA INLINE FORMAT='csv', DATA='record1
' INTO TABLE testDB.testTable`)).WillReturnResult(sqlmock.NewResult(1, 1))
		}

		var forceNewConnFlags []bool
		stubs := gostub.Stub(&GetOrInitDBConn, func(forceNewConn bool, randomCN bool) (*sql.DB, error) {
			forceNewConnFlags = append(forceNewConnFlags, forceNewConn)
			return dbConn, nil
		})
		defer stubs.Reset()

		tbl := &table.Table{
			Database: "testDB",
			Table:    "testTable",
			Columns:  []table.Column{{Name: "str", ColType: table.TVarchar}},
		}
		for i := 0; i < 3; i++ {
			n, err := WriteRowRecords([][]string{{"record1"}}, tbl, time.Second)
			require.NoError(t, err)
			require.Equal(t, 1, n)
		}

		require.Equal(t, []bool{true, false, false}, forceNewConnFlags)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestWriteRowRecords_ReturnsPingError(t *testing.T) {
	SyncTestWriteRowRecords(t, func(t *testing.T) {
		old := DBConnErrCount
		defer func() {
			DBConnErrCount = old
		}()
		DBConnErrCount = NewReConnectionBackOff(time.Hour, DBConnRetryThreshold)

		dbConn, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectClose()
		require.NoError(t, dbConn.Close())

		stubs := gostub.Stub(&GetOrInitDBConn, func(bool, bool) (*sql.DB, error) {
			return dbConn, nil
		})
		defer stubs.Reset()

		tbl := &table.Table{
			Database: "testDB",
			Table:    "testTable",
			Columns:  []table.Column{{Name: "str", ColType: table.TVarchar}},
		}
		_, err = WriteRowRecords([][]string{{"record1"}}, tbl, time.Second)
		require.Error(t, err)
		require.Contains(t, err.Error(), "database is closed")
	})
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
