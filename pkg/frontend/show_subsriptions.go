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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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
)

func getSqlForShowSubscriptions(_ context.Context, accId uint32) (string, error) {
	return fmt.Sprintf(getSubscriptionInfoFormat, accId), nil
}

func doShowSubscriptions(ctx context.Context, ses *Session, sp *tree.ShowSubscriptions) error {
	var err error
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
	if err != nil {
		goto handleFailed
	}
	sql, err = getSqlForShowSubscriptions(ctx, ses.GetTenantInfo().TenantID)
	if err != nil {
		goto handleFailed
	}
	err = bh.Exec(ctx, sql)
	if err != nil {
		goto handleFailed
	}
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		goto handleFailed
	}

	rs.AddColumn(showSubscriptionOutputColumns[0])
	rs.AddColumn(showSubscriptionOutputColumns[1])
	lower, err = ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		goto handleFailed
	}
	lowerInt64 = lower.(int64)
	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			row := make([]interface{}, 2)
			row[0], err = erArray[0].GetString(ctx, i, 0)
			if err != nil {
				goto handleFailed
			}
			createSql, err = erArray[0].GetString(ctx, i, 1)
			if err != nil {
				goto handleFailed
			}

			p := ses.GetCache().GetParser(dialect.MYSQL, createSql, lowerInt64)
			defer ses.GetCache().PutParser(p)
			ast, err = p.Parse(ctx)
			if err != nil {
				goto handleFailed
			}
			fromAccount := string(ast[0].(*tree.CreateDatabase).SubscriptionOption.From)
			row[1] = fromAccount
			rs.AddRow(row)
		}
	}
	ses.SetMysqlResultSet(rs)
	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}
	return nil
handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}
