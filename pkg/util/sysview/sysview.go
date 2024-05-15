// Copyright 2022 Matrix Origin
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

package sysview

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const (
	MysqlDBConst         = "mysql"
	InformationDBConst   = "information_schema"
	CreateDatabaseFormat = "create database if not exists "
)

var (
	InitMysqlSysTables = []string{
		MysqlUserDDL,
		MysqlDbDDL,
		MysqlProcsPrivDDL,
		MysqlColumnsPrivDDL,
		MysqlTablesPrivDDL,
		MysqlRoleEdgesDDL,
	}
	InitInformationSchemaSysTables = []string{
		InformationSchemaKeyColumnUsageDDL,
		InformationSchemaColumnsDDL,
		InformationSchemaProfilingDDL,
		InformationSchemaProcesslistDDL,
		InformationSchemaUserPrivilegesDDL,
		InformationSchemaSchemataDDL,
		InformationSchemaCharacterSetsDDL,
		InformationSchemaTriggersDDL,
		InformationSchemaTablesDDL,
		InformationSchemaPartitionsDDL,
		InformationSchemaViewsDDL,
		InformationSchemaStatisticsDDL,
		InformationSchemaReferentialConstraintsDDL,
		InformationSchemaEnginesDDL,
		InformationSchemaRoutinesDDL,
		InformationSchemaParametersDDL,
		InformationSchemaKeywordsDDL,
		InformationSchemaSchemaPrivilegesDDL,
		InformationSchemaTablePrivilegesDDL,
		InformationSchemaColumnPrivilegesDDL,
		InformationSchemaCollationsDDL,
		InformationSchemaTableConstraintsDDL,
		InformationSchemaEventsDDL,
		informationSchemaKeywordsData,
	}
)

func InitSchema(ctx context.Context, txn executor.TxnExecutor) error {
	if err := initMysqlTables(ctx, txn); err != nil {
		return err
	}
	if err := initInformationSchemaTables(ctx, txn); err != nil {
		return err
	}
	return nil
}

// Initialize system tables under the `mysql` database for compatibility with MySQL
func initMysqlTables(ctx context.Context, txn executor.TxnExecutor) error {
	_, err := txn.Exec(CreateDatabaseFormat+MysqlDBConst, executor.StatementOption{})
	if err != nil {
		return err
	}

	txn.Use(MysqlDBConst)

	var timeCost time.Duration
	defer func() {
		logutil.Debugf("[Mysql] init mysql tables: create cost %d ms", timeCost.Milliseconds())
	}()

	begin := time.Now()
	for _, sql := range InitMysqlSysTables {
		if _, err = txn.Exec(sql, executor.StatementOption{}); err != nil {
			return moerr.NewInternalError(ctx, "[Mysql] init mysql tables error: %v, sql: %s", err, sql)
		}
	}
	timeCost = time.Since(begin)
	return nil
}

// Initialize the system view under the `information_schema` database for compatibility with MySQL
func initInformationSchemaTables(ctx context.Context, txn executor.TxnExecutor) error {
	_, err := txn.Exec(CreateDatabaseFormat+InformationDBConst, executor.StatementOption{})
	if err != nil {
		return err
	}

	txn.Use(InformationDBConst)

	var timeCost time.Duration
	defer func() {
		logutil.Debugf("[information_schema] init information_schema tables: create cost %d ms", timeCost.Milliseconds())
	}()

	begin := time.Now()
	for _, sql := range InitInformationSchemaSysTables {
		if _, err = txn.Exec(sql, executor.StatementOption{}); err != nil {
			return moerr.NewInternalError(ctx, fmt.Sprintf("[information_schema] init information_schema tables error: %v, sql: %s", err, sql))
		}
	}
	timeCost = time.Since(begin)
	return nil
}
