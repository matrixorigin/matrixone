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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"go.uber.org/zap"
)

var (
	errNotReady = moerr.NewInvalidStateNoCtx("SQL writer's DB conn not ready")
)

// sqlWriterDBUser holds the db user for logger
var (
	sqlWriterDBUser atomic.Value
	dbAddressFunc   atomic.Value

	db atomic.Value

	dbMux sync.Mutex

	DBConnErrCount atomic.Uint32
)

const MOLoggerUser = "mo_logger"
const MaxConnectionNumber = 1

const DBConnRetryThreshold = 8

const MAX_CHUNK_SIZE = 1024 * 1024 * 4

const MAX_INSERT_TIME = 3 * time.Second

type DBUser struct {
	UserName string
	Password string
}

func SetSQLWriterDBUser(userName string, password string) {
	user := &DBUser{
		UserName: userName,
		Password: password,
	}
	sqlWriterDBUser.Store(user)
}
func GetSQLWriterDBUser() (*DBUser, error) {
	dbUser := sqlWriterDBUser.Load()
	if dbUser == nil {
		return nil, errNotReady
	} else {
		return sqlWriterDBUser.Load().(*DBUser), nil

	}
}

func SetSQLWriterDBAddressFunc(f func(context.Context, bool) (string, error)) {
	dbAddressFunc.Store(f)
}

func GetSQLWriterDBAddressFunc() func(context.Context, bool) (string, error) {
	return dbAddressFunc.Load().(func(context.Context, bool) (string, error))
}
func SetDBConn(conn *sql.DB) {
	db.Store(conn)
}

func InitOrRefreshDBConn(forceNewConn bool, randomCN bool) (*sql.DB, error) {

	dbMux.Lock()
	defer dbMux.Unlock()

	initFunc := func() error {
		dbUser, _ := GetSQLWriterDBUser()
		if dbUser == nil {
			return errNotReady
		}

		addressFunc := GetSQLWriterDBAddressFunc()
		if addressFunc == nil {
			return errNotReady
		}
		dbAddress, err := addressFunc(context.Background(), randomCN)
		if err != nil {
			return err
		}
		dsn :=
			fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=10s&writeTimeout=15s&timeout=15s&maxAllowedPacket=0",
				dbUser.UserName,
				dbUser.Password,
				dbAddress)
		newDBConn, err := sql.Open("mysql", dsn)
		newDBConn.SetMaxOpenConns(MaxConnectionNumber)
		newDBConn.SetMaxIdleConns(MaxConnectionNumber)
		SetDBConn(newDBConn)
		return nil
	}

	if forceNewConn || db.Load() == nil {
		err := initFunc()
		if err != nil {
			logutil.Error("sqlWriter db init failed", zap.Error(err))
			return nil, err
		}
		logutil.Debug("sqlWriter db init", zap.Bool("force", forceNewConn), zap.Bool("randomCN", randomCN), zap.String("db", fmt.Sprintf("%v", db.Load())))
	}
	dbConn := db.Load().(*sql.DB)
	return dbConn, nil
}

func WriteRowRecords(records [][]string, tbl *table.Table) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	var err error

	var dbConn *sql.DB

	if DBConnErrCount.Load() > DBConnRetryThreshold {
		dbConn, err = InitOrRefreshDBConn(true, true)
		DBConnErrCount.Store(0)
	} else {
		dbConn, err = InitOrRefreshDBConn(false, false)
	}
	if err != nil {
		logutil.Error("sqlWriter db init failed", zap.Error(err))
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), MAX_INSERT_TIME)
	defer cancel()

	done := make(chan error)
	go bulkInsert(ctx, done, dbConn, records, tbl, MAX_CHUNK_SIZE)

	select {
	case err := <-done:
		if err != nil {
			DBConnErrCount.Add(1)
		} else {
			logutil.Debug("sqlWriter WriteRowRecords finished", zap.Int("cnt", len(records)))
			return len(records), nil
		}
	case <-ctx.Done():
		DBConnErrCount.Add(1)
		err = ctx.Err()
	}
	logutil.Warn("sqlWriter WriteRowRecords failed", zap.Error(err))

	return 0, err
}

func bulkInsert(ctx context.Context, done chan error, sqlDb *sql.DB, records [][]string, tbl *table.Table, maxLen int) {
	if records == nil || len(records) == 0 {
		done <- nil
		return
	}

	baseStr := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", tbl.Database, tbl.Table)

	sb := strings.Builder{}
	defer sb.Reset()

	tx, err := sqlDb.Begin()
	if err != nil {
		done <- err
		return
	}

	for idx, row := range records {
		if len(row) == 0 {
			continue
		}

		sb.WriteString("(")
		for i, field := range row {
			if i != 0 {
				sb.WriteString(",")
			}
			if tbl.Columns[i].ColType == table.TJson {
				var js interface{}
				escapedJSON, _ := json.Marshal(js)
				_ = json.Unmarshal([]byte(field), &js)
				sb.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(strings.ReplaceAll(string(escapedJSON), "\\", "\\\\"), "'", "\\'")))
			} else {
				escapedStr := strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\'"), "'", "\\'")
				if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr[:tbl.Columns[i].Scale-1]))
				} else {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr))
				}
			}
		}
		sb.WriteString(")")

		if sb.Len() >= maxLen || idx == len(records)-1 {
			stmt := baseStr + sb.String() + ";"
			_, err := tx.ExecContext(ctx, stmt)
			if err != nil {
				tx.Rollback()
				sb.Reset()
				done <- err
				return
			}
			select {
			case <-ctx.Done():
				// If context deadline is exceeded, rollback the transaction
				tx.Rollback()
				done <- ctx.Err()
				return
			default:
			}
			sb.Reset()
		} else {
			sb.WriteString(",")
		}
	}
	if err := tx.Commit(); err != nil {
		done <- err
		return
	}
	done <- nil
}
