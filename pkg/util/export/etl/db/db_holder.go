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

package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

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

	dbMux             sync.Mutex
	rawlogStmt        *sql.Stmt
	statementInfoStmt *sql.Stmt
	metricsStmt       *sql.Stmt

	stmtMux sync.Mutex
)

const MOLoggerUser = "mo_logger"

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

func SetSQLWriterDBAddressFunc(f func(context.Context) (string, error)) {
	dbAddressFunc.Store(f)
}

func GetSQLWriterDBAddressFunc() func(context.Context) (string, error) {
	return dbAddressFunc.Load().(func(context.Context) (string, error))
}

func InitOrRefreshDBConn(forceNewConn bool) (*sql.DB, error) {

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
		dbAddress, err := addressFunc(context.Background())
		if err != nil {
			return err
		}
		dsn :=
			fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=300s&writeTimeout=30m&timeout=3000s&maxAllowedPacket=0",
				dbUser.UserName,
				dbUser.Password,
				dbAddress)
		newDBConn, err := sql.Open("mysql", dsn)
		logutil.Info("sqlWriter db initialized", zap.String("address", dbAddress))
		if err != nil {
			logutil.Info("sqlWriter db initialized failed", zap.String("address", dbAddress), zap.Error(err))
			return err
		}
		db.Store(newDBConn)
		return nil
	}

	if forceNewConn || db.Load() == nil {
		logutil.Info("sqlWriter db init", zap.Bool("forceNewConn", forceNewConn))
		err := initFunc()
		if err != nil {
			return nil, err
		}
	}
	dbConn := db.Load().(*sql.DB)
	return dbConn, nil
}

func GetStatement(sqlDb *sql.DB, tbl *table.Table) (*sql.Stmt, error) {
	stmtMux.Lock()
	defer stmtMux.Unlock()
	var stmt *sql.Stmt
	var err error
	switch tbl.Table {
	case "rawlog":
		stmt = rawlogStmt
	case "statement_info":
		stmt = statementInfoStmt
	case "metrics":
		stmt = metricsStmt
	}
	if stmt != nil {
		return stmt, nil
	}

	placeholders := strings.Repeat("?,", len(tbl.Columns))
	placeholders = placeholders[:len(placeholders)-1] // Remove trailing comma
	stmtSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%s)",
		tbl.Database,
		tbl.Table,
		placeholders,
	)
	stmt, err = sqlDb.Prepare(stmtSQL)
	if err != nil {
		return nil, err
	}
	switch tbl.Table {
	case "rawlog":
		rawlogStmt = stmt
	case "statement_info":
		statementInfoStmt = stmt
	case "metrics":
		metricsStmt = stmt
	}
	return stmt, nil
}
