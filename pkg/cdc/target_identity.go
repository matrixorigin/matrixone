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
	"errors"
	"fmt"
	"regexp"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cdc/retry"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const absentCDCTargetIdentity = "absent"

var mysqlInnoDBIdentityName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

var cdcTargetSQLRetryClassifier = retry.MultiClassifier{
	retry.DefaultClassifier{}, retry.MySQLErrorClassifier{},
}

func classifyCDCTargetSQLError(err error) error {
	if moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
		return newRetryableConnectionError(err)
	}
	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) && (mysqlErr.Number == moerr.ErrTxnNeedRetry ||
		mysqlErr.Number == moerr.ErrTxnNeedRetryWithDefChanged) {
		return newRetryableConnectionError(err)
	}
	if cdcTargetSQLRetryClassifier.IsRetryable(err) {
		return newRetryableConnectionError(err)
	}
	return err
}

func checkMySQLTargetIdentityCapability(ctx context.Context, conn *sql.Conn, db, table string, willCreate bool) (err error) {
	defer func() { err = classifyCDCTargetSQLError(err) }()
	if !mysqlInnoDBIdentityName.MatchString(db) || !mysqlInnoDBIdentityName.MatchString(table) {
		return moerr.NewNotSupported(ctx, "CDC target identity requires unambiguous MySQL InnoDB identifier encoding")
	}
	if willCreate {
		var engine string
		if err := conn.QueryRowContext(ctx, "SELECT @@default_storage_engine").Scan(&engine); err != nil {
			return err
		}
		if !strings.EqualFold(engine, "InnoDB") {
			return moerr.NewNotSupported(ctx, "CDC target identity requires an InnoDB default storage engine")
		}
	}
	rows, err := conn.QueryContext(ctx,
		"SELECT TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?", "__mo_cdc_capability_probe__/__absent__")
	if err != nil {
		return moerr.NewNotSupportedf(ctx, "CDC target InnoDB table identity is unavailable (PROCESS privilege required): %v", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		var id uint64
		if err = rows.Scan(&id); err != nil {
			return err
		}
	}
	return rows.Err()
}

func ObserveTargetIdentity(ctx context.Context, uri UriInfo, db, table, timeout string) (identity string, err error) {
	defer func() { err = classifyCDCTargetSQLError(err) }()
	connPool, err := OpenDbConn(ctx, uri.User, uri.Password, uri.Ip, uri.Port, timeout)
	if err != nil {
		return "", err
	}
	defer connPool.Close()
	conn, err := connPool.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return observeCDCTargetIdentity(ctx, conn, uri.SinkTyp, db, table)
}

// observeCDCTargetIdentity is an admission prestate observation. A later
// guarded read (or a non-idempotent CREATE) must close the race with DDL.
func observeCDCTargetIdentity(ctx context.Context, conn *sql.Conn, sinkType, db, table string) (string, error) {
	var count int
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'",
		db, table).Scan(&count); err != nil {
		return "", err
	}
	if count == 0 {
		return absentCDCTargetIdentity, nil
	}
	if count != 1 {
		return "", moerr.NewInternalErrorf(ctx, "CDC target %s.%s has ambiguous table metadata", db, table)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	return guardedCDCTargetIdentity(ctx, tx, sinkType, db, table)
}

// guardedCDCTargetIdentity holds the backend's table metadata lock until tx
// ends. It is intentionally called once per target DML transaction.
func guardedCDCTargetIdentity(ctx context.Context, tx *sql.Tx, sinkType, db, table string) (identity string, err error) {
	defer func() { err = classifyCDCTargetSQLError(err) }()
	switch sinkType {
	case CDCSinkType_MO:
		var id uint64
		guardSQL := fmt.Sprintf("CALL mo_cdc_target_identity('%s', '%s')", escapeSQLString(db), escapeSQLString(table))
		if err := tx.QueryRowContext(ctx, guardSQL).Scan(&id); err != nil {
			return "", err
		}
		if id == 0 {
			return "", moerr.NewInternalError(ctx, "CDC target returned a zero table ID")
		}
		return fmt.Sprintf("mo:%d", id), nil
	case CDCSinkType_MySQL:
		if !mysqlInnoDBIdentityName.MatchString(db) || !mysqlInnoDBIdentityName.MatchString(table) {
			return "", moerr.NewNotSupported(ctx, "CDC target identity requires unambiguous MySQL InnoDB identifier encoding")
		}
		var ignored int
		if err = tx.QueryRowContext(ctx, "SELECT 1 FROM "+quoteSQLIdentifier(db)+"."+quoteSQLIdentifier(table)+" LIMIT 0").Scan(&ignored); err != sql.ErrNoRows {
			if err == nil {
				return "", moerr.NewInternalError(ctx, "CDC empty target lock query returned a row")
			}
			return "", err
		}
		rows, err := tx.QueryContext(ctx,
			"SELECT @@server_uuid, TABLE_ID FROM information_schema.INNODB_TABLES WHERE NAME = ?",
			db+"/"+table)
		if err != nil {
			return "", moerr.NewNotSupportedf(ctx, "CDC target InnoDB table identity is unavailable (PROCESS privilege required): %v", err)
		}
		defer rows.Close()
		var uuid string
		var id uint64
		if !rows.Next() {
			if err = rows.Err(); err != nil {
				return "", err
			}
			return "", moerr.NewNotSupportedf(ctx, "CDC target %s.%s has no InnoDB table identity", db, table)
		}
		if err = rows.Scan(&uuid, &id); err != nil {
			return "", err
		}
		if uuid == "" {
			return "", moerr.NewNotSupported(ctx, "CDC target server UUID is unavailable")
		}
		if id == 0 || rows.Next() {
			return "", moerr.NewNotSupportedf(ctx, "CDC target %s.%s has no unique InnoDB table identity", db, table)
		}
		if err = rows.Err(); err != nil {
			return "", err
		}
		if err = rows.Close(); err != nil {
			return "", err
		}
		return fmt.Sprintf("mysql:%s:%d", uuid, id), nil
	default:
		return "", moerr.NewNotSupportedf(ctx, "CDC target identity is unsupported for sink %s", sinkType)
	}
}
