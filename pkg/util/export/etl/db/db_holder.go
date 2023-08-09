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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
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

	prepareSQLMap sync.Map

	moLogger   *log.MOLogger
	loggerInit sync.Once
)

const MOLoggerUser = "mo_logger"
const MaxConnectionNumber = 1

const DBConnRetryThreshold = 8

const MaxInsertLen = 200
const MiddleInsertLen = 10

var maxRowBufPool sync.Pool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

var middleRowBufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}
var oneRowBufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

type prepareSQLs struct {
	maxRowNum int
	maxRows   string

	middleRowNum int
	middleRows   string

	oneRow  string
	columns int
}

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
	logger := getLogger()
	initFunc := func() error {
		dbMux.Lock()
		defer dbMux.Unlock()
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
		if err != nil {
			return err
		}
		newDBConn.SetMaxOpenConns(MaxConnectionNumber)
		newDBConn.SetMaxIdleConns(MaxConnectionNumber)
		SetDBConn(newDBConn)
		return nil
	}

	if forceNewConn || db.Load() == nil {
		err := initFunc()
		if err != nil {
			logger.Error("sqlWriter db init failed", zap.Error(err))
			return nil, err
		}
		logger.Debug("sqlWriter db init", zap.Bool("force", forceNewConn), zap.Bool("randomCN", randomCN), zap.String("db", fmt.Sprintf("%v", db.Load())))
	}
	dbConn := db.Load().(*sql.DB)
	return dbConn, nil
}

func WriteRowRecords(records [][]string, tbl *table.Table, timeout time.Duration) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	var err error

	var dbConn *sql.DB

	var logger = getLogger()

	if DBConnErrCount.Load() > DBConnRetryThreshold {
		logger.Error("sqlWriter WriteRowRecords failed above threshold")
		if dbConn != nil {
			dbConn.Close()
		}
		dbConn, err = InitOrRefreshDBConn(true, true)
		DBConnErrCount.Store(0)
	} else {
		dbConn, err = InitOrRefreshDBConn(false, false)
	}
	if err != nil {
		logger.Debug("sqlWriter db init failed", zap.Error(err))
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = bulkInsert(ctx, dbConn, records, tbl, MaxInsertLen, MiddleInsertLen)
	if err != nil {
		DBConnErrCount.Add(1)
		return 0, moerr.NewInternalError(ctx, err.Error())
	}

	logger.Debug("sqlWriter WriteRowRecords finished", zap.Int("cnt", len(records)))
	return len(records), nil
}

func getPrepareSQL(tbl *table.Table, columns int, batchLen int, middleBatchLen int) *prepareSQLs {
	columnNames := make([]string, len(tbl.Columns))

	for i, column := range tbl.Columns {
		columnNames[i] = fmt.Sprintf("`%s`", column.Name)
	}

	columnStr := strings.Join(columnNames, ",")

	prefix := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES ", tbl.Database, tbl.Table, columnStr)

	oneRowBuf := oneRowBufPool.Get().(*bytes.Buffer)
	for i := 0; i < columns; i++ {
		if i == 0 {
			oneRowBuf.WriteByte('?')
		} else {
			oneRowBuf.WriteString(",?")
		}
	}
	oneRow := fmt.Sprintf("%s (%s)", prefix, oneRowBuf.String())

	maxRowBuf := maxRowBufPool.Get().(*bytes.Buffer)
	maxRowBuf.WriteString(prefix)
	for i := 0; i < batchLen; i++ {
		if i == 0 {
			maxRowBuf.WriteByte('(')
		} else {
			maxRowBuf.WriteString(",(")
		}
		maxRowBuf.Write(oneRowBuf.Bytes())
		maxRowBuf.WriteByte(')')
	}

	middleRowBuf := middleRowBufPool.Get().(*bytes.Buffer)
	middleRowBuf.WriteString(prefix)
	for i := 0; i < middleBatchLen; i++ {
		if i == 0 {
			middleRowBuf.WriteByte('(')
		} else {
			middleRowBuf.WriteString(",(")
		}
		middleRowBuf.Write(oneRowBuf.Bytes())
		middleRowBuf.WriteByte(')')
	}

	prepareSQLS := &prepareSQLs{
		maxRowNum: batchLen,
		maxRows:   maxRowBuf.String(),

		middleRowNum: middleBatchLen,
		middleRows:   middleRowBuf.String(),

		oneRow:  oneRow,
		columns: columns,
	}
	oneRowBuf.Reset()
	maxRowBuf.Reset()
	middleRowBuf.Reset()
	oneRowBufPool.Put(oneRowBuf)
	maxRowBufPool.Put(maxRowBuf)
	middleRowBufPool.Put(middleRowBuf)
	return prepareSQLS
}

func bulkInsert(ctx context.Context, sqlDb *sql.DB, records [][]string, tbl *table.Table, batchLen int, middleBatchLen int) error {
	if len(records) == 0 {
		return nil
	}
	var logger = getLogger()
	var sqls *prepareSQLs
	key := fmt.Sprintf("%s_%s", tbl.Database, tbl.Table)
	if val, ok := prepareSQLMap.Load(key); ok {
		sqls = val.(*prepareSQLs)
		if sqls.columns != len(records[0]) {
			sqls = getPrepareSQL(tbl, len(records[0]), batchLen, middleBatchLen)
			prepareSQLMap.Store(key, sqls)
		}
	} else {
		sqls = getPrepareSQL(tbl, len(records[0]), batchLen, middleBatchLen)
		prepareSQLMap.Store(key, sqls)
	}

	tx, err := sqlDb.BeginTx(ctx, nil)
	if err != nil {
		return moerr.ConvertGoError(ctx, err)
	}

	var maxStmt *sql.Stmt
	var middleStmt *sql.Stmt
	var oneStmt *sql.Stmt

	for {
		if len(records) == 0 {
			break
		} else if len(records) >= batchLen {
			if maxStmt == nil {
				maxStmt, err = tx.PrepareContext(ctx, sqls.maxRows)
				if err != nil {
					tx.Rollback()
					return err
				}
			}
			vals := make([]any, sqls.columns*batchLen)
			idx := 0
			for _, row := range records[:batchLen] {
				for i, field := range row {
					escapedStr := field
					if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
						vals[idx] = field[:tbl.Columns[i].Scale-1]
					} else {
						vals[idx] = field
					}
					idx++
				}
			}
			_, err := maxStmt.ExecContext(ctx, vals...)
			if err != nil {
				logger.Error("sqlWriter batchInsert failed", zap.Error(err))
				tx.Rollback()
				return err
			}
			if ctx.Err() != nil {
				tx.Rollback()
				return ctx.Err()
			}

			records = records[batchLen:]
		} else if len(records) >= middleBatchLen {
			if maxStmt != nil {
				err = maxStmt.Close()
				if err != nil {
					tx.Rollback()
					return err
				}
				maxStmt = nil
			}
			if middleStmt == nil {
				middleStmt, err = tx.PrepareContext(ctx, sqls.middleRows)
				if err != nil {
					tx.Rollback()
					return err
				}
			}
			vals := make([]any, sqls.columns*middleBatchLen)
			idx := 0
			for _, row := range records[:middleBatchLen] {
				for i, field := range row {
					escapedStr := field
					if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
						vals[idx] = field[:tbl.Columns[i].Scale-1]
					} else {
						vals[idx] = field
					}
					idx++
				}
			}
			_, err := middleStmt.ExecContext(ctx, vals...)
			if err != nil {
				logger.Error("sqlWriter batchInsert failed", zap.Error(err))
				tx.Rollback()
				return err
			}
			if ctx.Err() != nil {
				tx.Rollback()
				return ctx.Err()
			}

			records = records[middleBatchLen:]
		} else {
			if maxStmt != nil {
				err = maxStmt.Close()
				if err != nil {
					tx.Rollback()
					return err
				}
				maxStmt = nil
			}
			if middleStmt != nil {
				err = middleStmt.Close()
				if err != nil {
					tx.Rollback()
					return err
				}
				middleStmt = nil
			}
			if oneStmt == nil {
				oneStmt, err = tx.PrepareContext(ctx, sqls.oneRow)
				if err != nil {
					tx.Rollback()
					return err
				}
			}
			vals := make([]any, sqls.columns)
			for _, row := range records {
				if err != nil {
					return moerr.ConvertGoError(ctx, err)
				}
				for i, field := range row {
					escapedStr := field
					if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
						vals[i] = field[:tbl.Columns[i].Scale-1]
					} else {
						vals[i] = field
					}
				}
				_, err = oneStmt.ExecContext(ctx, vals...)
				if err != nil {
					tx.Rollback()
					return moerr.ConvertGoError(ctx, err)
				}
			}
			err = oneStmt.Close()
			if err != nil {
				tx.Rollback()
				return moerr.ConvertGoError(ctx, err)
			}
			break
		}
	}

	if err = tx.Commit(); err != nil {
		logger.Error("sqlWriter commit failed", zap.Error(err))
		tx.Rollback()
		return moerr.ConvertGoError(ctx, err)
	}
	return nil
}

func getLogger() *log.MOLogger {
	loggerInit.Do(func() {
		rt := runtime.ProcessLevelRuntime()
		if rt == nil {
			moLogger = log.GetServiceLogger(logutil.Adjust(logutil.GetGlobalLogger()), metadata.ServiceType_CN, "uuid")
		} else {
			moLogger = rt.Logger()
		}
		moLogger = moLogger.Named("etl/db_holder").With(logutil.Discardable())
	})
	return moLogger
}
