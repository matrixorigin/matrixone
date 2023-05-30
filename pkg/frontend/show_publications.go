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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	getPublicationsInfoFormat       = "select pub_name as Name,database_name as `Database` from mo_catalog.mo_pubs;"
	getCreatePublicationsInfoFormat = `select pub_name,database_name,all_account,account_list,comment from mo_catalog.mo_pubs where pub_name='%s';`
	showCreatePublicationFormat     = "CREATE PUBLICATION `%s` DATABASE `%s` "
)

var (
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
	showCreatePublicationOutputColumns = [2]Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Publication",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "Create Publication",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
	}
)

func getSqlForShowCreatePub(ctx context.Context, pubName string) (string, error) {
	if nameIsInvalid(pubName) {
		return "", moerr.NewInternalError(ctx, "invalid publication name '%s'", pubName)
	}
	return fmt.Sprintf(getCreatePublicationsInfoFormat, pubName), nil
}

func doShowCreatePublications(ctx context.Context, ses *Session, scp *tree.ShowCreatePublications) (err error) {
	var (
		rs                                                                    = &MysqlResultSet{}
		erArray                                                               []ExecResult
		sql                                                                   string
		row                                                                   []interface{}
		pubName, allAccountStr, accountList, databaseName, comment, createSql string
		allAccount                                                            bool
	)
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}
	sql, err = getSqlForShowCreatePub(ctx, scp.Name)
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
	if !execResultArrayHasData(erArray) {
		return moerr.NewInternalError(ctx, "publication '%s' does not exist", scp.Name)
	}
	pubName, err = erArray[0].GetString(ctx, 0, 0)
	if err != nil {
		return err
	}
	databaseName, err = erArray[0].GetString(ctx, 0, 1)
	if err != nil {
		return err
	}
	allAccountStr, err = erArray[0].GetString(ctx, 0, 2)
	if err != nil {
		return err
	}
	allAccount = allAccountStr == "true"
	accountList, err = erArray[0].GetString(ctx, 0, 3)
	if err != nil {
		return err
	}
	comment, err = erArray[0].GetString(ctx, 0, 4)
	if err != nil {
		return err
	}

	createSql = fmt.Sprintf(showCreatePublicationFormat, pubName, databaseName)

	if !allAccount {
		createSql += "ACCOUNT "
		for i, account := range strings.Split(accountList, ",") {
			if i != 0 {
				createSql += ", "
			}
			createSql += fmt.Sprintf("`%s`", account)
		}
	} else {
		createSql += "ACCOUNT ALL"
	}
	if comment != "" {
		createSql += fmt.Sprintf(" COMMENT '%s'", comment)
	}

	rs.AddColumn(showCreatePublicationOutputColumns[0])
	rs.AddColumn(showCreatePublicationOutputColumns[1])
	row = make([]interface{}, 2)
	row[0] = scp.Name
	row[1] = createSql
	rs.AddRow(row)

	ses.SetMysqlResultSet(rs)
	return nil
}
