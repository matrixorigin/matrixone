// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	getSubscriptionInfoFormat = `select datname,dat_createsql from mo_catalog.mo_database where dat_type='subscription' and account_id=%d;`
)

var (
	showSubscriptionOutputColumns = [2]Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "From_Account",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
	}

	showPublicationOutputColumns = [2]Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Database",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
	}
)

func getSqlForShowSubscriptions(_ context.Context, accId uint32) (string, error) {
	return fmt.Sprintf(getSubscriptionInfoFormat, accId), nil
}

func doShowSubscriptions(ctx context.Context, ses *Session, sp *tree.ShowSubscriptions) (err error) {
	var rs = &MysqlResultSet{}
	var erArray []ExecResult
	var lower interface{}
	var lowerInt64 int64
	var createSql string
	var ast []tree.Statement
	var sql string
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}
	sql, err = getSqlForShowSubscriptions(ctx, ses.GetTenantInfo().TenantID)
	if err != nil {
		return err
	}
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	rs.AddColumn(showSubscriptionOutputColumns[0])
	rs.AddColumn(showSubscriptionOutputColumns[1])
	lower, err = ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return err
	}
	lowerInt64 = lower.(int64)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			row := make([]interface{}, 2)
			row[0], err = erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return err
			}
			createSql, err = erArray[0].GetString(ctx, i, 1)
			if err != nil {
				return err
			}

			ast, err = mysql.Parse(ctx, createSql, lowerInt64)
			if err != nil {
				return err
			}
			fromAccount := string(ast[0].(*tree.CreateDatabase).SubscriptionOption.From)
			row[1] = fromAccount
			rs.AddRow(row)
		}
	}
	ses.SetMysqlResultSet(rs)
	return nil
}
