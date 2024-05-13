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
	"go/constant"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	getAccountIdNamesSql = "select account_id, account_name from mo_catalog.mo_account where status != 'suspend'"
	getPubsSql           = "select pub_name, database_name, account_list, created_time from mo_catalog.mo_pubs"
	getSubsFormat        = "select datname, dat_createsql, created_time from mo_catalog.mo_database where dat_type = 'subscription' and account_id = %d"
)

var (
	showSubscriptionOutputColumns = [6]Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_account",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_database",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_time",
				columnType: defines.MYSQL_TYPE_TIMESTAMP,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "sub_name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "sub_time",
				columnType: defines.MYSQL_TYPE_TIMESTAMP,
			},
		},
	}
)

type published struct {
	pubName     string
	pubAccount  string
	pubDatabase string
	pubTime     string
	subName     string
	subTime     string
}

type subscribed struct {
	pubName    string
	pubAccount string
	subName    string
	subTime    string
}

func getAccountIdByName(ctx context.Context, ses *Session, bh BackgroundExec, name string) int32 {
	if accountIds, _, err := getAccountIdNames(ctx, ses, bh, name); err == nil && len(accountIds) > 0 {
		return accountIds[0]
	}
	return -1
}

func getAccountIdNames(ctx context.Context, ses *Session, bh BackgroundExec, likeName string) ([]int32, []string, error) {
	bh.ClearExecResultBatches()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
	sql := getAccountIdNamesSql
	if len(likeName) > 0 {
		sql += fmt.Sprintf(" and account_name like '%s'", likeName)
	}
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, nil, err
	}

	var accountIds []int32
	var accountNames []string
	for _, batch := range bh.GetExecResultBatches() {
		mrs := &MysqlResultSet{
			Columns: make([]Column, len(batch.Vecs)),
		}
		oq := newFakeOutputQueue(mrs)
		for i := 0; i < batch.RowCount(); i++ {
			row, err := extractRowFromEveryVector(ctx, ses, batch, i, oq, true)
			if err != nil {
				return nil, nil, err
			}

			// column[0]: account_id
			accountIds = append(accountIds, row[0].(int32))
			// column[1]: account_name
			accountNames = append(accountNames, string(row[1].([]byte)[:]))
		}
	}
	return accountIds, accountNames, nil
}

func canSub(subAccount, subAccountListStr string) bool {
	if strings.ToLower(subAccountListStr) == "all" {
		return true
	}

	for _, acc := range strings.Split(subAccountListStr, ",") {
		if acc == subAccount {
			return true
		}
	}
	return false
}

func getPubs(ctx context.Context, ses *Session, bh BackgroundExec, accountId int32, accountName string, like string, subAccountName string) ([]*published, error) {
	bh.ClearExecResultBatches()
	sql := getPubsSql
	if len(like) > 0 {
		sql += fmt.Sprintf(" where pub_name like '%s';", like)
	}
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(accountId))
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}

	var pubs []*published
	for _, batch := range bh.GetExecResultBatches() {
		mrs := &MysqlResultSet{
			Columns: make([]Column, len(batch.Vecs)),
		}
		oq := newFakeOutputQueue(mrs)
		for i := 0; i < batch.RowCount(); i++ {
			row, err := extractRowFromEveryVector(ctx, ses, batch, i, oq, true)
			if err != nil {
				return nil, err
			}

			pubName := string(row[0].([]byte)[:])
			pubDatabase := string(row[1].([]byte)[:])
			subAccountListStr := string(row[2].([]byte)[:])
			pubTime := row[3].(string)
			if !canSub(subAccountName, subAccountListStr) {
				continue
			}

			pub := &published{
				pubName:     pubName,
				pubAccount:  accountName,
				pubDatabase: pubDatabase,
				pubTime:     pubTime,
			}
			pubs = append(pubs, pub)
		}
	}
	return pubs, nil
}

func getSubInfoFromSql(ctx context.Context, ses FeSession, sql string) (subName, pubAccountName, pubName string, err error) {
	var lowerAny interface{}
	if lowerAny, err = ses.GetGlobalVar(ctx, "lower_case_table_names"); err != nil {
		return
	}

	var ast []tree.Statement
	if ast, err = mysql.Parse(ctx, sql, lowerAny.(int64), 0); err != nil {
		return
	}
	defer func() {
		for _, s := range ast {
			s.Free()
		}
	}()

	subName = string(ast[0].(*tree.CreateDatabase).Name)
	pubAccountName = string(ast[0].(*tree.CreateDatabase).SubscriptionOption.From)
	pubName = string(ast[0].(*tree.CreateDatabase).SubscriptionOption.Publication)
	return
}

func getSubs(ctx context.Context, ses *Session, bh BackgroundExec, accountId uint32) ([]*subscribed, error) {
	bh.ClearExecResultBatches()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	sql := fmt.Sprintf(getSubsFormat, accountId)
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}

	var subs []*subscribed
	for _, batch := range bh.GetExecResultBatches() {
		mrs := &MysqlResultSet{
			Columns: make([]Column, len(batch.Vecs)),
		}
		oq := newFakeOutputQueue(mrs)
		for i := 0; i < batch.RowCount(); i++ {
			row, err := extractRowFromEveryVector(ctx, ses, batch, i, oq, true)
			if err != nil {
				return nil, err
			}

			subName := string(row[0].([]byte)[:])
			createSql := string(row[1].([]byte)[:])
			subTime := row[2].(string)
			_, pubAccountName, pubName, err := getSubInfoFromSql(ctx, ses, createSql)
			if err != nil {
				return nil, err
			}

			sub := &subscribed{
				pubName:    pubName,
				pubAccount: pubAccountName,
				subName:    subName,
				subTime:    subTime,
			}
			subs = append(subs, sub)
		}
	}
	return subs, nil
}

func doShowSubscriptions(ctx context.Context, ses *Session, ss *tree.ShowSubscriptions) (err error) {
	bh := GetRawBatchBackgroundExec(ctx, ses)
	defer bh.Close()

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	var like string
	if ss.Like != nil {
		right, ok := ss.Like.Right.(*tree.NumVal)
		if !ok || right.Value.Kind() != constant.String {
			return moerr.NewInternalError(ctx, "like clause must be a string")
		}
		like = constant.StringVal(right.Value)
	}

	// step 1. get all account
	var accountIds []int32
	var accountNames []string
	if accountIds, accountNames, err = getAccountIdNames(ctx, ses, bh, ""); err != nil {
		return err
	}

	// step 2. traversal all accounts, get all published pubs
	var allPublished []*published
	for i := 0; i < len(accountIds); i++ {
		var pubs []*published
		if pubs, err = getPubs(ctx, ses, bh, accountIds[i], accountNames[i], like, ses.GetTenantName()); err != nil {
			return err
		}

		allPublished = append(allPublished, pubs...)
	}

	// step 3. get current account's subscriptions
	allSubscribedMap := make(map[string]map[string]*subscribed)
	subs, err := getSubs(ctx, ses, bh, ses.GetTenantInfo().GetTenantID())
	if err != nil {
		return err
	}
	for _, sub := range subs {
		if _, ok := allSubscribedMap[sub.pubAccount]; !ok {
			allSubscribedMap[sub.pubAccount] = make(map[string]*subscribed)
		}
		allSubscribedMap[sub.pubAccount][sub.pubName] = sub
	}

	// step 4. join pubs && subs, and sort
	for _, pub := range allPublished {
		if sub := allSubscribedMap[pub.pubAccount][pub.pubName]; sub != nil {
			pub.subName = sub.subName
			pub.subTime = sub.subTime
		}
	}

	if len(like) > 0 {
		// sort by pub_name asc
		sort.SliceStable(allPublished, func(i, j int) bool {
			return allPublished[i].pubName < allPublished[j].pubName
		})
	} else {
		// sort by sub_time, pub_time desc
		sort.SliceStable(allPublished, func(i, j int) bool {
			if allPublished[i].subTime != allPublished[j].subTime {
				return allPublished[i].subTime > allPublished[j].subTime
			}
			return allPublished[i].pubTime > allPublished[j].pubTime
		})
	}

	// step 5. generate result set
	bh.ClearExecResultBatches()
	var rs = &MysqlResultSet{}
	for _, column := range showSubscriptionOutputColumns {
		rs.AddColumn(column)
	}
	for _, pub := range allPublished {
		if !ss.All && len(pub.subName) == 0 {
			continue
		}

		var subName, subTime interface{}
		subName, subTime = pub.subName, pub.subTime
		if len(pub.subName) == 0 {
			subName, subTime = nil, nil
		}
		rs.AddRow([]interface{}{pub.pubName, pub.pubAccount, pub.pubDatabase, pub.pubTime, subName, subTime})
	}
	ses.SetMysqlResultSet(rs)
	return nil
}
