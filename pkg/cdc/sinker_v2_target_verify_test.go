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

package cdc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCDCTargetGuardSQLRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"success", nil, false},
		{"bad connection", driver.ErrBadConn, true},
		{"lost MySQL connection", &gomysql.MySQLError{Number: 2013}, true},
		{"MO RC definition changed over SQL wire", &gomysql.MySQLError{Number: moerr.ErrTxnNeedRetryWithDefChanged}, true},
		{"missing SELECT privilege", &gomysql.MySQLError{Number: 1142}, false},
		{"cancelled", context.Canceled, false},
		{"wrapped MO definition changed", fmt.Errorf("source guard: %w", moerr.NewTxnNeedRetryWithDefChangedNoCtx()), true},
		{"unsupported with retry keyword", moerr.NewNotSupportedNoCtx("server UUID is unavailable"), false},
		{"wrapped RPC timeout", fmt.Errorf("backend: %w", moerr.NewRPCTimeoutNoCtx()), true},
		{"wrapped service unavailable", fmt.Errorf("backend: %w", moerr.NewServiceUnavailableNoCtx("temporary")), true},
		{"unknown", errors.New("unclassified failure"), false},
		{"wrapped cancellation", fmt.Errorf("backend: %w", context.Canceled), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classified := classifyCDCTargetSQLError(tc.err)
			require.Equal(t, tc.retryable, IsRetryableConnectionError(classified))
			require.ErrorIs(t, classified, tc.err)
			require.Equal(t, tc.retryable, (&TableChangeStream{}).determineRetryable(classified))
		})
	}
}

func TestCDCBackendTransportEquivalence(t *testing.T) {
	ctx := context.Background()
	for _, cause := range []*moerr.Error{
		moerr.NewRPCTimeoutNoCtx(), moerr.NewServiceUnavailableNoCtx("temporary"),
		moerr.NewConnectionReset(ctx), moerr.NewBackendClosed(ctx),
		moerr.NewNoAvailableBackend(ctx), moerr.NewBackendCannotConnect(ctx),
		moerr.NewTNShardNotFound(ctx, "shard", 1), moerr.NewRpcError(ctx, "temporary"),
		moerr.NewTxnNeedRetryNoCtx(), moerr.NewTxnNeedRetryWithDefChangedNoCtx(),
		moerr.NewClientClosed(ctx), moerr.NewStreamClosed(ctx),
	} {
		t.Run(fmt.Sprint(cause.ErrorCode()), func(t *testing.T) {
			want := cause.ErrorCode() != moerr.ErrClientClosed && cause.ErrorCode() != moerr.ErrStreamClosed
			for _, err := range []error{cause, &gomysql.MySQLError{Number: cause.ErrorCode()}} {
				err = fmt.Errorf("backend: %w", err)
				retryable, classified := ClassifyRetryableError(err)
				require.True(t, classified)
				require.Equal(t, want, retryable)
				require.Equal(t, want, (&TableChangeStream{}).determineRetryable(err))
			}
		})
	}
	unknown := &gomysql.MySQLError{Number: 999, Message: "legacy runtime timeout"}
	retryable, classified := ClassifyRetryableError(unknown)
	require.False(t, retryable)
	require.False(t, classified)
	require.True(t, (&TableChangeStream{}).determineRetryable(unknown))
}

func TestCDCTargetIdentityAdmissionAndGuard(t *testing.T) {
	ctx := context.Background()

	t.Run("capability rejects ambiguous identifiers", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		err = checkMySQLTargetIdentityCapability(ctx, conn, "bad-name", "t", false)
		require.ErrorContains(t, err, "unambiguous")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("capability checks engine and identity privilege", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@default_storage_engine")).WillReturnRows(
			sqlmock.NewRows([]string{"engine"}).AddRow("InnoDB"))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
			WithArgs("__mo_cdc_capability_probe__/__absent__").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_ID"}))
		require.NoError(t, checkMySQLTargetIdentityCapability(ctx, conn, "db", "t", true))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("capability rejects non InnoDB and probe failures", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			rows  *sqlmock.Rows
			query error
			want  string
		}{
			{name: "non innodb", rows: sqlmock.NewRows([]string{"engine"}).AddRow("MyISAM"), want: "InnoDB"},
			{name: "probe unavailable", rows: sqlmock.NewRows([]string{"engine"}).AddRow("InnoDB"), query: driver.ErrBadConn},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				mock.ExpectQuery(regexp.QuoteMeta("SELECT @@default_storage_engine")).WillReturnRows(tc.rows)
				if tc.query != nil {
					mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
						WithArgs("__mo_cdc_capability_probe__/__absent__").WillReturnError(tc.query)
				}
				got := checkMySQLTargetIdentityCapability(ctx, conn, "db", "t", true)
				if tc.query != nil {
					require.ErrorIs(t, got, tc.query)
					require.True(t, IsRetryableConnectionError(got))
				} else {
					require.ErrorContains(t, got, tc.want)
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("capability reports engine query failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@default_storage_engine")).
			WillReturnError(errors.New("engine query failed"))
		require.ErrorContains(t, checkMySQLTargetIdentityCapability(ctx, conn, "db", "t", true), "engine query failed")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("capability reports identity row scan and close errors", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			rows *sqlmock.Rows
			want string
		}{
			{name: "scan error", rows: sqlmock.NewRows([]string{"TABLE_ID"}).AddRow("not-a-number"), want: "converting driver.Value"},
			{name: "row error", rows: sqlmock.NewRows([]string{"TABLE_ID"}).AddRow(uint64(1)).RowError(0, errors.New("capability row failed")), want: "capability row failed"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
					WithArgs("__mo_cdc_capability_probe__/__absent__").WillReturnRows(tc.rows)
				require.ErrorContains(t, checkMySQLTargetIdentityCapability(ctx, conn, "db", "t", false), tc.want)
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("capability reports identity row close error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
			WithArgs("__mo_cdc_capability_probe__/__absent__").
			WillReturnRows(sqlmock.NewRows([]string{"TABLE_ID"}).CloseError(errors.New("capability close failed")))
		require.ErrorContains(t, checkMySQLTargetIdentityCapability(ctx, conn, "db", "t", false), "capability close failed")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("MO guard returns durable identity", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("CALL mo_cdc_target_identity('db', 't')")).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uint64(42)))
		mock.ExpectRollback()
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		identity, err := guardedCDCTargetIdentity(ctx, tx, CDCSinkType_MO, "db", "t")
		require.NoError(t, err)
		require.Equal(t, "mo:42", identity)
		require.NoError(t, tx.Rollback())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("MO guard rejects zero identity", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("CALL mo_cdc_target_identity('db', 't')")).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uint64(0)))
		mock.ExpectRollback()
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = guardedCDCTargetIdentity(ctx, tx, CDCSinkType_MO, "db", "t")
		require.ErrorContains(t, err, "zero table ID")
		require.NoError(t, tx.Rollback())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("MO guard reports query failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("CALL mo_cdc_target_identity('db', 't')")).
			WillReturnError(errors.New("identity query failed"))
		mock.ExpectRollback()
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = guardedCDCTargetIdentity(ctx, tx, CDCSinkType_MO, "db", "t")
		require.ErrorContains(t, err, "identity query failed")
		require.NoError(t, tx.Rollback())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("MySQL guard returns server and table identity", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM `db`.`t` LIMIT 0")).
			WillReturnRows(sqlmock.NewRows([]string{"one"}))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@server_uuid, TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
			WithArgs("db/t").
			WillReturnRows(sqlmock.NewRows([]string{"server_uuid", "TABLE_ID"}).AddRow("uuid", uint64(7)))
		mock.ExpectRollback()
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		identity, err := guardedCDCTargetIdentity(ctx, tx, CDCSinkType_MySQL, "db", "t")
		require.NoError(t, err)
		require.Equal(t, "mysql:uuid:7", identity)
		require.NoError(t, tx.Rollback())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("MySQL guard reports lock and metadata query errors", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			lockError error
			metaError error
			want      string
		}{
			{name: "lock query", lockError: driver.ErrBadConn, want: "bad connection"},
			{name: "metadata query", metaError: driver.ErrBadConn, want: "bad connection"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				mock.ExpectBegin()
				lock := mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM `db`.`t` LIMIT 0"))
				if tc.lockError != nil {
					lock.WillReturnError(tc.lockError)
				} else {
					lock.WillReturnRows(sqlmock.NewRows([]string{"one"}))
					mock.ExpectQuery(regexp.QuoteMeta("SELECT @@server_uuid, TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
						WithArgs("db/t").WillReturnError(tc.metaError)
				}
				mock.ExpectRollback()
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				_, err = guardedCDCTargetIdentity(ctx, tx, CDCSinkType_MySQL, "db", "t")
				require.ErrorContains(t, err, tc.want)
				require.True(t, IsRetryableConnectionError(err))
				require.NoError(t, tx.Rollback())
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("MySQL guard reports identity row scan and iteration errors", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			rows *sqlmock.Rows
			want string
		}{
			{name: "scan", rows: sqlmock.NewRows([]string{"server_uuid", "TABLE_ID"}).AddRow("uuid", "bad-id"), want: "converting driver.Value"},
			{name: "iteration", rows: sqlmock.NewRows([]string{"server_uuid", "TABLE_ID"}).AddRow("uuid", uint64(7)).AddRow("uuid", uint64(8)).RowError(1, errors.New("identity rows failed")), want: "identity rows failed"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				mock.ExpectBegin()
				mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM `db`.`t` LIMIT 0")).
					WillReturnRows(sqlmock.NewRows([]string{"one"}))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT @@server_uuid, TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
					WithArgs("db/t").WillReturnRows(tc.rows)
				mock.ExpectRollback()
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				_, err = guardedCDCTargetIdentity(ctx, tx, CDCSinkType_MySQL, "db", "t")
				require.ErrorContains(t, err, tc.want)
				require.NoError(t, tx.Rollback())
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("MySQL guard reports identity row close error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM `db`.`t` LIMIT 0")).
			WillReturnRows(sqlmock.NewRows([]string{"one"}))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@server_uuid, TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
			WithArgs("db/t").WillReturnRows(sqlmock.NewRows([]string{"server_uuid", "TABLE_ID"}).
			AddRow("uuid", uint64(7)).CloseError(errors.New("identity close failed")))
		mock.ExpectRollback()
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = guardedCDCTargetIdentity(ctx, tx, CDCSinkType_MySQL, "db", "t")
		require.ErrorContains(t, err, "identity close failed")
		require.NoError(t, tx.Rollback())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("guard rejects unsupported and malformed identities", func(t *testing.T) {
		tests := []struct {
			name   string
			sink   string
			db     string
			rows   *sqlmock.Rows
			server string
			id     uint64
			want   string
		}{
			{name: "unsupported sink", sink: "other", want: "unsupported"},
			{name: "bad mysql identifier", sink: CDCSinkType_MySQL, db: "bad-name", want: "unambiguous"},
			{name: "missing table identity", sink: CDCSinkType_MySQL, db: "db", rows: sqlmock.NewRows([]string{"server_uuid", "TABLE_ID"}), want: "no InnoDB table identity"},
			{name: "empty server uuid", sink: CDCSinkType_MySQL, db: "db", rows: sqlmock.NewRows([]string{"server_uuid", "TABLE_ID"}).AddRow("", uint64(1)), want: "server UUID"},
			{name: "duplicate identity", sink: CDCSinkType_MySQL, db: "db", rows: sqlmock.NewRows([]string{"server_uuid", "TABLE_ID"}).AddRow("uuid", uint64(1)).AddRow("uuid", uint64(2)), want: "unique InnoDB"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				mock.ExpectBegin()
				if tc.sink == CDCSinkType_MySQL && tc.want != "unambiguous" {
					mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM `db`.`t` LIMIT 0")).WillReturnRows(sqlmock.NewRows([]string{"one"}))
					mock.ExpectQuery(regexp.QuoteMeta("SELECT @@server_uuid, TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).
						WithArgs("db/t").WillReturnRows(tc.rows)
				}
				mock.ExpectRollback()
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				_, err = guardedCDCTargetIdentity(ctx, tx, tc.sink, tc.db, "t")
				require.ErrorContains(t, err, tc.want)
				require.NoError(t, tx.Rollback())
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("observe absent target", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'")).
			WithArgs("db", "t").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		identity, err := observeCDCTargetIdentity(ctx, conn, CDCSinkType_MO, "db", "t")
		require.NoError(t, err)
		require.Equal(t, absentCDCTargetIdentity, identity)
		require.NoError(t, conn.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("observe rejects ambiguous target metadata", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'")).
			WithArgs("db", "t").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		_, err = observeCDCTargetIdentity(ctx, conn, CDCSinkType_MO, "db", "t")
		require.ErrorContains(t, err, "ambiguous")
		require.NoError(t, conn.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("observe target identity guards an existing table", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'")).
			WithArgs("db", "t").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("CALL mo_cdc_target_identity('db', 't')")).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uint64(9)))
		mock.ExpectRollback()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		identity, err := observeCDCTargetIdentity(ctx, conn, CDCSinkType_MO, "db", "t")
		require.NoError(t, err)
		require.Equal(t, "mo:9", identity)
		require.NoError(t, conn.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("observe propagates catalog query failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'")).
			WithArgs("db", "t").WillReturnError(errors.New("catalog query failed"))
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		_, err = observeCDCTargetIdentity(ctx, conn, CDCSinkType_MO, "db", "t")
		require.ErrorContains(t, err, "catalog query failed")
		require.NoError(t, conn.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("observe reports begin failure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'")).
			WithArgs("db", "t").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		_, err = observeCDCTargetIdentity(ctx, conn, CDCSinkType_MO, "db", "t")
		require.ErrorContains(t, err, "begin failed")
		require.NoError(t, conn.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestVerifyCDCTargetTableStructure(t *testing.T) {
	const columnsSQL = "SELECT column_name, column_type, collation_name, numeric_scale FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position"
	const indexesSQL = "SELECT index_name, non_unique, seq_in_index, column_name, sub_part FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? ORDER BY index_name, seq_in_index"
	type column struct{ name, typ string }
	type index struct {
		name, column        string
		nonUnique, sequence int64
		prefix              any
	}
	tests := []struct {
		name    string
		columns []column
		indexes []index
		unique  bool
		wantErr string
	}{
		{"valid MySQL integer display width", []column{{"id", "int(11)"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}}, false, ""},
		{"valid unique index", []column{{"id", "int"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}, {"uk_v", "v", 0, 1, nil}}, true, ""},
		{"reordered columns", []column{{"v", "varchar(20)"}, {"id", "int"}}, nil, false, "column 1"},
		{"narrowed varchar", []column{{"id", "int"}, {"v", "varchar(5)"}}, nil, false, "column 2"},
		{"changed text collation", []column{{"id", "int"}, {"v", "varchar(20)"}}, nil, false, "collation differs"},
		{"missing column", []column{{"id", "int"}}, nil, false, "has 1 columns"},
		{"missing primary key", []column{{"id", "int"}, {"v", "varchar(20)"}}, nil, false, "primary key differs"},
		{"extra unique index", []column{{"id", "int"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}, {"uk_v", "v", 0, 1, nil}}, false, "unexpected unique key"},
		{"missing unique index", []column{{"id", "int"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}}, true, "missing a source unique key"},
		{"prefix unique index", []column{{"id", "int"}, {"v", "varchar(20)"}}, []index{{"PRIMARY", "id", 0, 1, nil}, {"uk_v", "v", 0, 1, int64(4)}}, true, "prefix unique key"},
		{"valid year display width", []column{{"id", "int"}, {"v", "year(4)"}}, []index{{"PRIMARY", "id", 0, 1, nil}}, false, ""},
		{"valid MO float scale", []column{{"id", "int"}, {"v", "float(5)"}}, []index{{"PRIMARY", "id", 0, 1, nil}}, false, ""},
		{"different MO float scale", []column{{"id", "int"}, {"v", "float(5)"}}, nil, false, "column 2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			executor := &Executor{targetLockConn: conn}
			def := &plan.TableDef{
				Cols: []*plan.ColDef{
					{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
					{Name: "v", Typ: plan.Type{Id: int32(types.T_varchar), Width: 20, Charset: uint32(types.CharsetUTF8MB4Bin)}},
				},
				Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
			}
			switch tc.name {
			case "valid year display width":
				def.Cols[1].Typ = plan.Type{Id: int32(types.T_year), Width: 4}
			case "valid MO float scale", "different MO float scale":
				def.Cols[1].Typ = plan.Type{Id: int32(types.T_float32), Width: 5, Scale: 2}
			}
			if tc.unique {
				def.Indexes = []*plan.IndexDef{{IndexName: "uk_v", Parts: []string{"v"}, Unique: true}}
			}
			columnRows := sqlmock.NewRows([]string{"column_name", "column_type", "collation_name", "numeric_scale"})
			for _, col := range tc.columns {
				var collation any
				var numericScale any
				if tc.name == "valid MO float scale" {
					numericScale = int64(2)
				} else if tc.name == "different MO float scale" {
					numericScale = int64(3)
				}
				if col.name == "v" {
					collation = "utf8mb4_bin"
					if tc.name == "changed text collation" {
						collation = "utf8mb4_general_ci"
					}
				}
				columnRows.AddRow(col.name, col.typ, collation, numericScale)
			}
			mock.ExpectQuery(regexp.QuoteMeta(columnsSQL)).WithArgs("dst", "t").WillReturnRows(columnRows)
			if len(tc.columns) == 2 && tc.columns[0].name == "id" &&
				(tc.columns[1].typ == "varchar(20)" && tc.name != "changed text collation" ||
					tc.name == "valid year display width" || tc.name == "valid MO float scale") {
				indexRows := sqlmock.NewRows([]string{"index_name", "non_unique", "seq_in_index", "column_name", "sub_part"})
				for _, idx := range tc.indexes {
					indexRows.AddRow(idx.name, idx.nonUnique, idx.sequence, idx.column, idx.prefix)
				}
				mock.ExpectQuery(regexp.QuoteMeta(indexesSQL)).WithArgs("dst", "t").WillReturnRows(indexRows)
			}
			err = verifyCDCTargetTable(context.Background(), executor,
				&DbTableInfo{SinkDbName: "dst", SinkTblName: "t", SourceTblId: 42}, def, CDCSinkType_MO)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestNormalizeCDCTargetColumnTypeFromMOAndMySQL(t *testing.T) {
	require.Equal(t, "utf8mb4_bin", expectedCDCTargetCollation(plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetLegacy)}))
	require.Equal(t, "utf8mb4_bin", expectedCDCTargetCollation(plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB4Bin)}))
	require.Equal(t, "utf8mb4_general_ci", expectedCDCTargetCollation(plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)}))
	for _, tc := range []struct{ source, target string }{
		{"INT", "INT(32)"},
		{"TINYINT UNSIGNED", "TINYINT UNSIGNED(8)"},
		{"SMALLINT UNSIGNED", "SMALLINT UNSIGNED(16)"},
		{"INT UNSIGNED", "INT UNSIGNED(32)"},
		{"BIGINT UNSIGNED", "BIGINT UNSIGNED(64)"},
		{"FLOAT", "FLOAT(0)"},
		{"DOUBLE", "DOUBLE(0)"},
		{"DATE", "DATE(0)"},
		{"DATETIME", "DATETIME(0)"},
		{"TIMESTAMP", "TIMESTAMP(0)"},
		{"BOOL", "BOOL(0)"},
		{"JSON", "JSON(0)"},
	} {
		require.Equal(t, normalizeCDCTargetColumnType(tc.source), normalizeCDCTargetColumnType(tc.target), tc)
	}
	require.NotEqual(t, normalizeCDCTargetColumnType("varchar(20)"), normalizeCDCTargetColumnType("varchar(5)"))
	require.NotEqual(t, normalizeCDCTargetColumnType("decimal(16,6)"), normalizeCDCTargetColumnType("decimal(8,2)"))
	require.NotEqual(t, normalizeCDCTargetColumnType("ENUM('a b')"), normalizeCDCTargetColumnType("ENUM('ab')"))
	require.NotEqual(t, normalizeCDCTargetColumnType("ENUM('integer')"), normalizeCDCTargetColumnType("ENUM('int')"))
	require.False(t, cdcTargetColumnTypeMatches(plan.Type{Id: int32(types.T_int8)}, "BOOL(0)", CDCSinkType_MO, sql.NullInt64{}))
	require.True(t, cdcTargetColumnTypeMatches(plan.Type{Id: int32(types.T_bool)}, "tinyint(1)", CDCSinkType_MySQL, sql.NullInt64{}))
	require.False(t, cdcTargetColumnTypeMatches(plan.Type{Id: int32(types.T_bool)}, "tinyint(1)", CDCSinkType_MO, sql.NullInt64{}))
}

func TestCDCTargetIdentityRetryContract(t *testing.T) {
	for _, point := range []string{"capability", "guard_metadata", "guard_lock_control"} {
		for _, code := range []uint16{2013, 1205, 1159, 1161, moerr.ErrRPCTimeout, moerr.ErrServiceUnavailable, 1044, 1045, 1142, 1143, 1227, moerr.ErrNotSupported} {
			t.Run(fmt.Sprintf("%s_%d", point, code), func(t *testing.T) {
				ctx := context.Background()
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				transient := &gomysql.MySQLError{Number: code, Message: "permission on relation unavailable_timeout"}
				var got error
				if point == "capability" {
					conn, err := db.Conn(ctx)
					require.NoError(t, err)
					defer conn.Close()
					mock.ExpectQuery(regexp.QuoteMeta("SELECT TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).WithArgs("__mo_cdc_capability_probe__/__absent__").WillReturnError(transient)
					got = checkMySQLTargetIdentityCapability(ctx, conn, "db", "t", false)
				} else {
					mock.ExpectBegin()
					q := mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM `db`.`t` LIMIT 0"))
					if point == "guard_lock_control" {
						q.WillReturnError(transient)
					} else {
						q.WillReturnRows(sqlmock.NewRows([]string{"one"}))
						mock.ExpectQuery(regexp.QuoteMeta("SELECT @@server_uuid, TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?")).WithArgs("db/t").WillReturnError(transient)
					}
					mock.ExpectRollback()
					tx, err := db.BeginTx(ctx, nil)
					require.NoError(t, err)
					_, got = guardedCDCTargetIdentity(ctx, tx, CDCSinkType_MySQL, "db", "t")
					require.NoError(t, tx.Rollback())
				}
				require.Error(t, got)
				require.NoError(t, mock.ExpectationsWereMet())
				// Same retry contract as the actual admission consumer. A transient
				// target failure must not become permanent watermark error metadata.
				require.ErrorIs(t, got, transient)
				require.Equal(t, code == 2013 || code == 1205 || code == 1159 || code == 1161 || code == moerr.ErrRPCTimeout || code == moerr.ErrServiceUnavailable, IsRetryableConnectionError(got), "retry classification must survive the query boundary")
				require.Equal(t, code == 2013 || code == 1205 || code == 1159 || code == 1161 || code == moerr.ErrRPCTimeout || code == moerr.ErrServiceUnavailable, (&TableChangeStream{}).determineRetryable(got))
			})
		}
	}
}
