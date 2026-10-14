// Copyright 2026 Matrix Origin
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

package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestMongoDBReadOnlyInsertOracle(t *testing.T) {
	expected := [][]string{{"id", "device", "site", "2026-07-27 10:00:00", "NULL", "batch"}}
	for _, name := range []string{"rejected and unchanged", "unexpected success", "unrelated failure", "source changed"} {
		t.Run(name, func(t *testing.T) {
			db, mock := newMongoDBE2ESQLMock(t)
			write := mock.ExpectExec("insert into mongodb_ci.events")
			switch name {
			case "unexpected success":
				write.WillReturnResult(sqlmock.NewResult(0, 1))
			case "unrelated failure":
				write.WillReturnError(errors.New("injected unrelated failure"))
			default:
				write.WillReturnError(errors.New("cannot insert/update/delete from external table"))
			}
			if name == "rejected and unchanged" || name == "source changed" {
				value := "NULL"
				if name == "source changed" {
					value = "99"
				}
				mock.ExpectQuery("select .* from mongodb_ci.events order by mongo_id").
					WillReturnRows(sqlmock.NewRows([]string{"id", "device", "site", "ts", "measurement", "batch"}).
						AddRow("id", "device", "site", "2026-07-27 10:00:00", value, "batch")).RowsWillBeClosed()
			}
			err := verifyReadOnlyInsert(t.Context(), db, expected)
			switch name {
			case "rejected and unchanged":
				require.NoError(t, err)
			case "source changed":
				require.ErrorContains(t, err, "source changed after rejected insert")
			case "unrelated failure":
				require.ErrorContains(t, err, "injected unrelated failure")
			default:
				require.Error(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// These tests validate the E2E oracle itself: a failed operation, partial
// write, or changed value must never produce a successful case receipt.
// They do not substitute for running the real MongoDB integration suite.
func TestMongoDBTargetConflictOracle(t *testing.T) {
	for _, tc := range []struct {
		name, count, seed, want string
		setupFailure            int
		insertErr               error
	}{
		{name: "duplicate leaves target unchanged", insertErr: &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry"}, count: "1", seed: "seed"},
		{name: "wrapped duplicate leaves target unchanged", insertErr: fmt.Errorf("insert: %w", &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry"}), count: "1", seed: "seed"},
		{name: "duplicate code does not depend on wording", insertErr: &mysqldriver.MySQLError{Number: 1062, Message: "key conflict"}, count: "1", seed: "seed"},
		{name: "create fails", setupFailure: 1, want: "prepare constrained"},
		{name: "seed fails", setupFailure: 2, want: "prepare constrained"},
		{name: "insert succeeds unexpectedly", want: "unexpectedly succeeded"},
		{name: "timeout is not a duplicate", insertErr: context.DeadlineExceeded, want: "timed out"},
		{name: "unrelated failure is rejected", insertErr: errors.New("connection unavailable"), want: "unrelated reason"},
		{name: "duplicate column is not a key conflict", insertErr: &mysqldriver.MySQLError{Number: 1060, Message: "Duplicate column name 'mongo_id'"}, want: "unrelated reason"},
		{name: "duplicate index name is not a key conflict", insertErr: &mysqldriver.MySQLError{Number: 1061, Message: "Duplicate key name 'PRIMARY'"}, want: "unrelated reason"},
		{name: "untyped duplicate message is rejected", insertErr: errors.New("Error 1062: Duplicate entry"), want: "unrelated reason"},
		{name: "partial insertion is rejected", insertErr: &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry"}, count: "2", want: "target changed"},
		{name: "seed mutation is rejected", insertErr: &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry"}, count: "1", seed: "changed", want: "seed changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMongoDBE2ESQLMock(t)
			for i, statement := range []string{"create table mongodb_ci.unique_sink", "insert into mongodb_ci.unique_sink values"} {
				query := mock.ExpectExec(statement)
				if tc.setupFailure == i+1 {
					query.WillReturnError(errors.New("injected setup failure"))
					break
				}
				query.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			if tc.setupFailure == 0 {
				insert := mock.ExpectExec("insert into mongodb_ci.unique_sink select")
				if tc.insertErr == nil {
					insert.WillReturnResult(sqlmock.NewResult(0, 5))
				} else {
					insert.WillReturnError(tc.insertErr)
				}
				if tc.count != "" {
					mock.ExpectQuery(regexp.QuoteMeta("select count(*) from mongodb_ci.unique_sink")).
						WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tc.count)).RowsWillBeClosed()
				}
				if tc.seed != "" {
					mock.ExpectQuery("select device_id from mongodb_ci.unique_sink where").
						WillReturnRows(sqlmock.NewRows([]string{"device_id"}).AddRow(tc.seed)).RowsWillBeClosed()
				}
			}
			err := verifyTargetUniqueConflictRollback(t.Context(), db)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
				if tc.want == "unrelated reason" {
					require.ErrorIs(t, err, tc.insertErr)
				}
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMongoDBMaterializeAndJoinOracle(t *testing.T) {
	expected := [][]string{{"id", "device", "site", "2026-07-27 10:00:00", "NULL", "batch"}}
	for _, tc := range []struct {
		name, want               string
		failStage, mismatchStage int
	}{
		{name: "exact rows"},
		{name: "create error", failStage: 1, want: "materialize MongoDB source"},
		{name: "materialized query error", failStage: 2, want: "materialized MongoDB rows"},
		{name: "materialized value mismatch", mismatchStage: 2, want: "fixture result mismatch"},
		{name: "join query error", failStage: 3, want: "MongoDB and local table join"},
		{name: "join value mismatch", mismatchStage: 3, want: "fixture result mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMongoDBE2ESQLMock(t)
			create := mock.ExpectExec("create table mongodb_ci.events_copy as select")
			if tc.failStage == 1 {
				create.WillReturnError(errors.New("injected create failure"))
			} else {
				create.WillReturnResult(sqlmock.NewResult(0, 1))
				for i, query := range []string{
					"select .* from mongodb_ci.events_copy order by mongo_id",
					"select .* from mongodb_ci.events e join mongodb_ci.events_copy c",
				} {
					stage := i + 2
					result := mock.ExpectQuery(query)
					if tc.failStage == stage {
						result.WillReturnError(errors.New("injected read failure"))
						break
					}
					value := "NULL"
					if tc.mismatchStage == stage {
						value = "wrong"
					}
					result.WillReturnRows(sqlmock.NewRows([]string{"id", "device", "site", "ts", "measurement", "batch"}).
						AddRow("id", "device", "site", "2026-07-27 10:00:00", value, "batch")).RowsWillBeClosed()
					if tc.mismatchStage == stage {
						break
					}
				}
			}
			err := verifyMaterializeAndJoin(t.Context(), db, expected)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMongoDBAggregateSnapshotOracle(t *testing.T) {
	expected := [][]string{{"device", "site", "2026-07-27 10:00:00", "NULL"}}
	for _, tc := range []struct {
		name, want string
		rows       func() *sqlmock.Rows
		queryErr   error
	}{
		{name: "exact snapshot", rows: func() *sqlmock.Rows {
			return sqlmock.NewRows([]string{"device", "site", "window", "measurement"}).
				AddRow("device", "site", "2026-07-27 10:00:00", "NULL")
		}},
		{name: "value changed without count change", want: "aggregate rows changed", rows: func() *sqlmock.Rows {
			return sqlmock.NewRows([]string{"device", "site", "window", "measurement"}).
				AddRow("device", "site", "2026-07-27 10:00:00", "99")
		}},
		{name: "query failure", queryErr: errors.New("offline"), want: "read aggregate rows"},
		{name: "scan failure", want: "scan aggregate row", rows: func() *sqlmock.Rows {
			return sqlmock.NewRows([]string{"device"}).AddRow("device")
		}},
		{name: "iteration failure after a row", want: "iterate aggregate rows", rows: func() *sqlmock.Rows {
			return sqlmock.NewRows([]string{"device", "site", "window", "measurement"}).
				AddRow("device", "site", "2026-07-27 10:00:00", "NULL").
				AddRow("device2", "site", "2026-07-27 10:00:00", "1").
				RowError(1, errors.New("read interrupted"))
		}},
		{name: "missing row", want: "aggregate rows changed", rows: func() *sqlmock.Rows {
			return sqlmock.NewRows([]string{"device", "site", "window", "measurement"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMongoDBE2ESQLMock(t)
			query := mock.ExpectQuery(regexp.QuoteMeta(aggregateRowsSQL))
			if tc.queryErr != nil {
				query.WillReturnError(tc.queryErr)
			} else {
				query.WillReturnRows(tc.rows()).RowsWillBeClosed()
			}
			err := expectAggregateRows(t.Context(), db, expected)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMongoDBAggregateFixtureRejectsWrongKeysOrValues(t *testing.T) {
	// First query checks total cardinality; the remaining five check every
	// independent fixture key/value. A correct total must not hide a wrong key.
	for failedQuery := 0; failedQuery < 6; failedQuery++ {
		t.Run(string(rune('0'+failedQuery)), func(t *testing.T) {
			db, mock := newMongoDBE2ESQLMock(t)
			for i := 0; i <= failedQuery; i++ {
				value := "1"
				query := regexp.QuoteMeta("select count(*) from mongodb_ci.minute_aggregate")
				if i == 0 {
					value = "5"
					query += "$"
				} else {
					query += " where "
				}
				if i == failedQuery {
					value = "0"
				}
				mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(value)).RowsWillBeClosed()
			}
			require.Error(t, verifyAggregateFixture(t.Context(), db))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
