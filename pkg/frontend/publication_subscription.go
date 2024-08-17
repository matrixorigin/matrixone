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
	"slices"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

const (
	// account
	getAccountIdNamesSql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1"
	// pub
	insertIntoMoPubsFormat         = `insert into mo_catalog.mo_pubs(pub_name,database_name,database_id,all_table,table_list,account_list,created_time,owner,creator,comment) values ('%s','%s',%d,%t,'%s','%s',now(),%d,%d,'%s');`
	getPubInfoSql                  = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1"
	updatePubInfoFormat            = `update mo_catalog.mo_pubs set account_list = '%s', comment = '%s', database_name = '%s', database_id = %d, update_time = now(), table_list = '%s' where pub_name = '%s';`
	updatePubInfoAccountListFormat = `update mo_catalog.mo_pubs set account_list = '%s' where pub_name = '%s';`
	dropPubFormat                  = `delete from mo_catalog.mo_pubs where pub_name = '%s';`
	// sub
	insertIntoMoSubsFormat                  = "insert into mo_catalog.mo_subs (sub_account_id, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status) values (%d, '%s', '%s', '%s', '%s', now(), '%s', %d)"
	batchInsertIntoMoSubsFormat             = "insert into mo_catalog.mo_subs (sub_account_id, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status) values %s"
	batchUpdateMoSubsFormat                 = "update mo_catalog.mo_subs set pub_database='%s', pub_tables='%s', pub_time=now(), pub_comment='%s', status=%d where pub_account_name = '%s' and pub_name = '%s' and sub_account_id in (%s)"
	batchDeleteMoSubsFormat                 = "delete from mo_catalog.mo_subs where pub_account_name = '%s' and pub_name = '%s' and sub_account_id in (%s)"
	getSubsSql                              = "select sub_account_id, sub_name, sub_time, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status from mo_catalog.mo_subs where 1=1"
	deleteMoSubsRecordsBySubAccountIdFormat = "delete from mo_catalog.mo_subs where sub_account_id = %d"

	getDbIdAndTypFormat = `select dat_id,dat_type from mo_catalog.mo_database where datname = '%s' and account_id = %d;`
)

var (
	showPublicationsOutputColumns = [8]Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "publication",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "database",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "tables",
				columnType: defines.MYSQL_TYPE_TEXT,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "sub_account",
				columnType: defines.MYSQL_TYPE_TEXT,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "subscribed_accounts",
				columnType: defines.MYSQL_TYPE_TEXT,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "create_time",
				columnType: defines.MYSQL_TYPE_TIMESTAMP,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "update_time",
				columnType: defines.MYSQL_TYPE_TIMESTAMP,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "comments",
				columnType: defines.MYSQL_TYPE_TEXT,
			},
		},
	}

	showSubscriptionsOutputColumns = [9]Column{
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
				name:       "pub_tables",
				columnType: defines.MYSQL_TYPE_TEXT,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "pub_comment",
				columnType: defines.MYSQL_TYPE_TEXT,
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
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "status",
				columnType: defines.MYSQL_TYPE_TINY,
			},
		},
	}
)

func doCreatePublication(ctx context.Context, ses *Session, cp *tree.CreatePublication) (err error) {
	start := time.Now()
	defer func() {
		v2.CreatePubHistogram.Observe(time.Since(start).Seconds())
	}()

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	tenantInfo := ses.GetTenantInfo()
	if !tenantInfo.IsAdminRole() {
		return moerr.NewInternalError(ctx, "only admin can create publication")
	}

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	ctx = defines.AttachAccount(ctx, tenantInfo.TenantID, tenantInfo.GetUserID(), tenantInfo.GetDefaultRoleID())
	return createPublication(ctx, bh, cp)
}

// createPublication creates a publication, bh should be in a transaction
func createPublication(ctx context.Context, bh BackgroundExec, cp *tree.CreatePublication) (err error) {
	var (
		sql             string
		dbId            uint64
		dbType          string
		accountNamesStr string
	)

	accIdInfoMap, accNameInfoMap, err := getAccounts(ctx, bh)
	if err != nil {
		return
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	accountName := accIdInfoMap[int32(accountId)].Name
	// delete current tenant
	delete(accIdInfoMap, int32(accountId))

	var subAccounts map[int32]*pubsub.AccountInfo
	if cp.AccountsSet.All {
		if accountId != sysAccountID && !pubsub.CanPubToAll(accountName, getGlobalPu().SV.PubAllAccounts) {
			return moerr.NewInternalError(ctx, "only sys account and authorized normal accounts can publish to all accounts")
		}
		subAccounts = accIdInfoMap
		accountNamesStr = pubsub.AccountAll
	} else {
		if subAccounts, err = getSetAccounts(ctx, cp.AccountsSet.SetAccounts, accNameInfoMap, accountName); err != nil {
			return err
		}
		accountNamesStr = pubsub.JoinAccounts(subAccounts)
	}

	pubName := string(cp.Name)
	dbName := string(cp.Database)
	comment := cp.Comment
	if _, ok := sysDatabases[dbName]; ok {
		err = moerr.NewInternalError(ctx, "invalid database name '%s', not support publishing system database", dbName)
		return
	}

	if dbId, dbType, err = getDbIdAndType(ctx, bh, dbName); err != nil {
		return
	}
	if dbType != "" { //TODO: check the dat_type
		return moerr.NewInternalError(ctx, "database '%s' is not a user database", cp.Database)
	}

	tablesStr := pubsub.TableAll
	if len(cp.Table) > 0 {
		if tablesStr, err = genPubTablesStr(ctx, bh, dbName, cp.Table); err != nil {
			return
		}
	}

	sql, err = getSqlForInsertIntoMoPubs(ctx, pubName, dbName, dbId, len(cp.Table) == 0, tablesStr, accountNamesStr, comment, true)
	if err != nil {
		return
	}

	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	subInfos, err := getSubInfosFromPub(ctx, bh, accountName, pubName, false)
	if err != nil {
		return err
	}

	updateNotAuthSubAccounts := make([]int32, 0, len(subInfos))
	updateNormalSubAccounts := make([]int32, 0, len(subInfos))
	for subAccId, subInfo := range subInfos {
		if subInfo.Status != pubsub.SubStatusDeleted {
			err = moerr.NewInternalError(ctx, "unexpected subInfo.Status, actual: %v, expect: %v", subInfo.Status, pubsub.SubStatusDeleted)
			return
		}

		if _, ok := subAccounts[subAccId]; !ok {
			// if it's recreated but not authed, update status -> not authorized
			updateNotAuthSubAccounts = append(updateNotAuthSubAccounts, subAccId)
		} else {
			// if it's recreated and authed, update status -> normal
			updateNormalSubAccounts = append(updateNormalSubAccounts, subAccId)
		}
	}

	if err = batchUpdateMoSubs(
		ctx, bh,
		dbName, tablesStr, comment,
		pubsub.SubStatusNotAuthorized,
		accountName, pubName,
		updateNotAuthSubAccounts,
	); err != nil {
		return
	}

	if err = batchUpdateMoSubs(
		ctx, bh,
		dbName, tablesStr, comment,
		pubsub.SubStatusNormal,
		accountName, pubName,
		updateNormalSubAccounts,
	); err != nil {
		return
	}

	insertSubAccounts := make([]int32, 0, len(subAccounts))
	for accId := range subAccounts {
		// if it's not existed, insert
		if _, ok := subInfos[accId]; !ok {
			insertSubAccounts = append(insertSubAccounts, accId)
		}
	}
	if err = batchInsertMoSubs(
		ctx, bh,
		dbName, tablesStr, comment,
		accountName, pubName,
		insertSubAccounts,
	); err != nil {
		return
	}

	return
}

func doAlterPublication(ctx context.Context, ses *Session, ap *tree.AlterPublication) (err error) {
	start := time.Now()
	defer func() {
		v2.AlterPubHistogram.Observe(time.Since(start).Seconds())
	}()

	var (
		accountNamesStr string
		dbType          string
		sql             string
	)
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	tenantInfo := ses.GetTenantInfo()
	if !tenantInfo.IsAdminRole() {
		return moerr.NewInternalError(ctx, "only admin can alter publication")
	}

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	accIdInfoMap, accNameInfoMap, err := getAccounts(ctx, bh)
	if err != nil {
		return
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	accountName := accIdInfoMap[int32(accountId)].Name
	// delete current tenant
	delete(accIdInfoMap, int32(accountId))

	pubName := string(ap.Name)
	pub, err := getPubInfo(ctx, bh, pubName)
	if err != nil {
		return
	}
	if pub == nil {
		err = moerr.NewInternalError(ctx, "publication '%s' does not exist", pubName)
		return
	}

	// alter account
	var oldSubAccounts, newSubAccounts map[int32]*pubsub.AccountInfo
	if pub.SubAccountsStr == pubsub.AccountAll {
		oldSubAccounts = accIdInfoMap
		accountNamesStr = pubsub.AccountAll
	} else {
		oldSubAccounts = make(map[int32]*pubsub.AccountInfo)
		for _, accountName := range pub.GetSubAccountNames() {
			if accInfo, ok := accNameInfoMap[accountName]; ok {
				oldSubAccounts[accInfo.Id] = accInfo
			}
		}
		accountNamesStr = pubsub.JoinAccounts(oldSubAccounts)
	}
	newSubAccounts = oldSubAccounts

	if ap.AccountsSet != nil {
		switch {
		case ap.AccountsSet.All:
			if accountId != sysAccountID && !pubsub.CanPubToAll(accountName, getGlobalPu().SV.PubAllAccounts) {
				return moerr.NewInternalError(ctx, "only sys account and authorized normal accounts can publish to all accounts")
			}
			newSubAccounts = accIdInfoMap
			accountNamesStr = pubsub.AccountAll
		case len(ap.AccountsSet.SetAccounts) > 0:
			if newSubAccounts, err = getSetAccounts(ctx, ap.AccountsSet.SetAccounts, accNameInfoMap, accountName); err != nil {
				return
			}
			accountNamesStr = pubsub.JoinAccounts(newSubAccounts)
		case len(ap.AccountsSet.DropAccounts) > 0:
			if pub.SubAccountsStr == pubsub.AccountAll {
				err = moerr.NewInternalError(ctx, "cannot drop accounts from all account option")
				return
			}
			if newSubAccounts, err = getDropAccounts(ap.AccountsSet.DropAccounts, oldSubAccounts, accNameInfoMap); err != nil {
				return
			}
			accountNamesStr = pubsub.JoinAccounts(newSubAccounts)
		case len(ap.AccountsSet.AddAccounts) > 0:
			if pub.SubAccountsStr == pubsub.AccountAll {
				err = moerr.NewInternalError(ctx, "cannot add account from all account option")
				return
			}
			if newSubAccounts, err = getAddAccounts(ctx, ap.AccountsSet.AddAccounts, oldSubAccounts, accNameInfoMap, accountName); err != nil {
				return
			}
			accountNamesStr = pubsub.JoinAccounts(newSubAccounts)
		}
	}

	// alter comment
	comment := pub.Comment
	if ap.Comment != "" {
		comment = ap.Comment
	}

	// alter db
	dbName := pub.DbName
	dbId := pub.DbId
	if ap.DbName != "" {
		dbName = ap.DbName
		if _, ok := sysDatabases[dbName]; ok {
			return moerr.NewInternalError(ctx, "invalid database name '%s', not support publishing system database", dbName)
		}

		if dbId, dbType, err = getDbIdAndType(ctx, bh, dbName); err != nil {
			return err
		}
		if dbType != "" { //TODO: check the dat_type
			return moerr.NewInternalError(ctx, "database '%s' is not a user database", dbName)
		}
	}

	// alter tables
	tablesStr := pub.TablesStr
	if len(ap.Table) > 0 {
		if tablesStr, err = genPubTablesStr(ctx, bh, dbName, ap.Table); err != nil {
			return
		}
	}

	if sql, err = getSqlForUpdatePubInfo(ctx, string(ap.Name), accountNamesStr, comment, dbName, dbId, tablesStr, false); err != nil {
		return
	}
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	// subAccountName -> SubInfo map
	subInfos, err := getSubInfosFromPub(ctx, bh, accountName, pubName, false)
	if err != nil {
		return err
	}

	deleteSubAccounts := make([]int32, 0, len(subInfos))
	updateNotAuthSubAccounts := make([]int32, 0, len(subInfos))
	updateNormalSubAccounts := make([]int32, 0, len(subInfos))
	for accName, subInfo := range subInfos {
		if subInfo.Status == pubsub.SubStatusDeleted {
			err = moerr.NewInternalError(ctx, "unexpected subInfo.Status: %v", subInfo.Status)
			return
		}

		if _, ok := newSubAccounts[accName]; !ok {
			if len(subInfo.SubName) == 0 {
				// if it's unsubscribed and not authorized, delete it
				deleteSubAccounts = append(deleteSubAccounts, subInfo.SubAccountId)
				continue
			}
			updateNotAuthSubAccounts = append(updateNotAuthSubAccounts, subInfo.SubAccountId)
		} else {
			updateNormalSubAccounts = append(updateNormalSubAccounts, subInfo.SubAccountId)
		}
	}

	if err = batchUpdateMoSubs(
		ctx, bh,
		dbName, tablesStr, comment,
		pubsub.SubStatusNotAuthorized,
		accountName, pubName,
		updateNotAuthSubAccounts,
	); err != nil {
		return
	}

	if err = batchUpdateMoSubs(
		ctx, bh,
		dbName, tablesStr, comment,
		pubsub.SubStatusNormal,
		accountName, pubName,
		updateNormalSubAccounts,
	); err != nil {
		return
	}

	if err = batchDeleteMoSubs(
		ctx, bh,
		accountName, pubName,
		deleteSubAccounts,
	); err != nil {
		return
	}

	insertSubAccounts := make([]int32, 0, len(newSubAccounts))
	for accId := range newSubAccounts {
		// if it's not existed, insert
		if _, ok := subInfos[accId]; !ok {
			insertSubAccounts = append(insertSubAccounts, accId)
		}
	}
	if err = batchInsertMoSubs(
		ctx, bh,
		dbName, tablesStr, comment,
		accountName, pubName,
		insertSubAccounts,
	); err != nil {
		return
	}

	return
}

func doDropPublication(ctx context.Context, ses *Session, dp *tree.DropPublication) (err error) {
	start := time.Now()
	defer func() {
		v2.DropPubHistogram.Observe(time.Since(start).Seconds())
	}()

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	tenantInfo := ses.GetTenantInfo()
	if !tenantInfo.IsAdminRole() {
		return moerr.NewInternalError(ctx, "only admin can drop publication")
	}

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	return dropPublication(ctx, bh, dp.IfExists, string(dp.Name))
}

// dropPublication drops a publication, bh should be in a transaction
func dropPublication(ctx context.Context, bh BackgroundExec, ifExists bool, pubName string) (err error) {
	var sql string

	accIdInfoMap, _, err := getAccounts(ctx, bh)
	if err != nil {
		return
	}
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	accountName := accIdInfoMap[int32(accountId)].Name

	pub, err := getPubInfo(ctx, bh, pubName)
	if err != nil {
		return err
	}
	if pub == nil {
		if !ifExists {
			err = moerr.NewInternalError(ctx, "publication '%s' does not exist", pubName)
		}
		return
	}

	if sql, err = getSqlForDropPubInfo(ctx, pubName, false); err != nil {
		return
	}
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	// subAccountName -> SubInfo map
	subInfos, err := getSubInfosFromPub(ctx, bh, accIdInfoMap[int32(accountId)].Name, pubName, false)
	if err != nil {
		return err
	}

	deleteSubAccounts := make([]int32, 0, len(subInfos))
	updateDeletedAccounts := make([]int32, 0, len(subInfos))
	for _, subInfo := range subInfos {
		if subInfo.Status == pubsub.SubStatusDeleted {
			err = moerr.NewInternalError(ctx, "unexpected subInfo.Status: %v", subInfo.Status)
			return
		}

		if len(subInfo.SubName) == 0 {
			// if it's unsubscribed, delete it
			deleteSubAccounts = append(deleteSubAccounts, subInfo.SubAccountId)
		} else {
			updateDeletedAccounts = append(updateDeletedAccounts, subInfo.SubAccountId)
		}
	}

	if err = batchUpdateMoSubs(
		ctx, bh,
		"", "", "",
		pubsub.SubStatusDeleted,
		accountName, pubName,
		updateDeletedAccounts,
	); err != nil {
		return
	}

	if err = batchDeleteMoSubs(
		ctx, bh,
		accountName, pubName,
		deleteSubAccounts,
	); err != nil {
		return
	}

	return
}

func getAccounts(ctx context.Context, bh BackgroundExec) (idInfoMap map[int32]*pubsub.AccountInfo, nameInfoMap map[string]*pubsub.AccountInfo, err error) {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := getAccountIdNamesSql

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	var (
		id            int64
		name          string
		status        string
		version       uint64
		suspendedTime string
		isNull        bool
	)
	idInfoMap = make(map[int32]*pubsub.AccountInfo)
	nameInfoMap = make(map[string]*pubsub.AccountInfo)
	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			// column[0]: account_id
			if id, err = result.GetInt64(ctx, i, 0); err != nil {
				return
			}
			// column[1]: account_name
			if name, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			// column[2]: status
			if status, err = result.GetString(ctx, i, 2); err != nil {
				return
			}
			// column[3]: version
			if version, err = result.GetUint64(ctx, i, 3); err != nil {
				return
			}
			// column[4]: suspended_time
			if isNull, err = result.ColumnIsNull(ctx, i, 4); err != nil {
				return
			} else if !isNull {
				if suspendedTime, err = result.GetString(ctx, i, 4); err != nil {
					return
				}
			} else {
				suspendedTime = ""
			}
			accountInfo := &pubsub.AccountInfo{
				Id:            int32(id),
				Name:          name,
				Status:        status,
				Version:       version,
				SuspendedTime: suspendedTime,
			}
			idInfoMap[int32(id)] = accountInfo
			nameInfoMap[name] = accountInfo
		}
	}
	return
}

func extractPubInfosFromExecResult(ctx context.Context, erArray []ExecResult) (pubInfos []*pubsub.PubInfo, err error) {
	var (
		pubName         string
		dbName          string
		dbId            uint64
		tablesStr       string
		accountNamesStr string
		createTime      string
		updateTime      string
		owner           uint64
		creator         uint64
		comment         string
		isNull          bool
	)
	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			if pubName, err = result.GetString(ctx, i, 0); err != nil {
				return
			}
			if dbName, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			if dbId, err = result.GetUint64(ctx, i, 2); err != nil {
				return
			}
			if tablesStr, err = result.GetString(ctx, i, 3); err != nil {
				return
			}
			if accountNamesStr, err = result.GetString(ctx, i, 4); err != nil {
				return
			}
			if createTime, err = result.GetString(ctx, i, 5); err != nil {
				return
			}
			if isNull, err = result.ColumnIsNull(ctx, i, 6); err != nil {
				return
			} else if !isNull {
				if updateTime, err = result.GetString(ctx, i, 6); err != nil {
					return
				}
			} else {
				updateTime = ""
			}
			if owner, err = result.GetUint64(ctx, i, 7); err != nil {
				return
			}
			if creator, err = result.GetUint64(ctx, i, 8); err != nil {
				return
			}
			if comment, err = result.GetString(ctx, i, 9); err != nil {
				return
			}

			pubInfos = append(pubInfos, &pubsub.PubInfo{
				PubName:        pubName,
				DbName:         dbName,
				DbId:           dbId,
				TablesStr:      tablesStr,
				SubAccountsStr: accountNamesStr,
				CreateTime:     createTime,
				UpdateTime:     updateTime,
				Owner:          uint32(owner),
				Creator:        uint32(creator),
				Comment:        comment,
			})
		}
	}
	return
}

func getPubInfo(ctx context.Context, bh BackgroundExec, pubName string) (pubInfo *pubsub.PubInfo, err error) {
	sql := getPubInfoSql + fmt.Sprintf(" and pub_name = '%s'", pubName)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	var pubInfos []*pubsub.PubInfo
	if pubInfos, err = extractPubInfosFromExecResult(ctx, erArray); err != nil {
		return
	} else if len(pubInfos) > 0 {
		pubInfo = pubInfos[0]
	}
	return
}

func getPubInfos(ctx context.Context, bh BackgroundExec, like string) (pubInfos []*pubsub.PubInfo, err error) {
	sql := getPubInfoSql
	if len(like) > 0 {
		sql += fmt.Sprintf(" and pub_name like '%s' order by pub_name", like)
	} else {
		sql += " order by update_time desc, created_time desc"
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractPubInfosFromExecResult(ctx, erArray)
}

func getPubInfosToAllAccounts(ctx context.Context, bh BackgroundExec) (pubInfos []*pubsub.PubInfo, err error) {
	sql := getPubInfoSql
	sql += fmt.Sprintf(" and account_list = '%s'", pubsub.AccountAll)

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractPubInfosFromExecResult(ctx, erArray)
}

func getPubInfosByDbname(ctx context.Context, bh BackgroundExec, dbName string) (pubInfo []*pubsub.PubInfo, err error) {
	sql := getPubInfoSql + fmt.Sprintf(" and database_name = '%s'", dbName)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractPubInfosFromExecResult(ctx, erArray)
}

func extractSubInfosFromExecResult(ctx context.Context, erArray []ExecResult) (subInfos []*pubsub.SubInfo, err error) {
	var (
		subAccountId   int64
		subName        string
		subTime        string
		pubAccountName string
		pubName        string
		pubDbName      string
		pubTables      string
		pubTime        string
		pubComment     string
		status         int64
		isNull         bool
	)
	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			if subAccountId, err = result.GetInt64(ctx, i, 0); err != nil {
				return
			}
			if isNull, err = result.ColumnIsNull(ctx, i, 1); err != nil {
				return
			} else if !isNull {
				if subName, err = result.GetString(ctx, i, 1); err != nil {
					return
				}
			} else {
				subName = ""
			}
			if isNull, err = result.ColumnIsNull(ctx, i, 2); err != nil {
				return
			} else if !isNull {
				if subTime, err = result.GetString(ctx, i, 2); err != nil {
					return
				}
			} else {
				subTime = ""
			}
			if pubAccountName, err = result.GetString(ctx, i, 3); err != nil {
				return
			}
			if pubName, err = result.GetString(ctx, i, 4); err != nil {
				return
			}
			if pubDbName, err = result.GetString(ctx, i, 5); err != nil {
				return
			}
			if pubTables, err = result.GetString(ctx, i, 6); err != nil {
				return
			}
			if pubTime, err = result.GetString(ctx, i, 7); err != nil {
				return
			}
			if pubComment, err = result.GetString(ctx, i, 8); err != nil {
				return
			}
			if status, err = result.GetInt64(ctx, i, 9); err != nil {
				return
			}

			subInfos = append(subInfos, &pubsub.SubInfo{
				SubAccountId:   int32(subAccountId),
				SubName:        subName,
				SubTime:        subTime,
				PubAccountName: pubAccountName,
				PubName:        pubName,
				PubDbName:      pubDbName,
				PubTables:      pubTables,
				PubTime:        pubTime,
				PubComment:     pubComment,
				Status:         pubsub.SubStatus(status),
			})
		}
	}
	return
}

// getSubInfosFromPub return subAccountId -> subInfo map for given (pubAccountName, pubName)
func getSubInfosFromPub(ctx context.Context, bh BackgroundExec, pubAccountName, pubName string, subscribed bool) (subInfoMap map[int32]*pubsub.SubInfo, err error) {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := getSubsSql
	if len(pubAccountName) > 0 {
		sql += fmt.Sprintf(" and pub_account_name = '%s'", pubAccountName)
	}
	if len(pubName) > 0 {
		sql += fmt.Sprintf(" and pub_name = '%s'", pubName)
	}
	if subscribed {
		sql += " and sub_name is not null"
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	subInfos, err := extractSubInfosFromExecResult(ctx, erArray)
	if err != nil {
		return
	}

	subInfoMap = make(map[int32]*pubsub.SubInfo)
	for _, subInfo := range subInfos {
		subInfoMap[subInfo.SubAccountId] = subInfo
	}
	return
}

// getSubInfosFromSub return subInfo map for given subName
func getSubInfosFromSub(ctx context.Context, bh BackgroundExec, subName string) (subInfo []*pubsub.SubInfo, err error) {
	subAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	sql := getSubsSql + fmt.Sprintf(" and sub_account_id = %d", subAccountId)
	if len(subName) > 0 {
		sql += fmt.Sprintf(" and sub_name = '%s'", subName)
	}
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractSubInfosFromExecResult(ctx, erArray)
}

func doShowPublications(ctx context.Context, ses *Session, sp *tree.ShowPublications) (err error) {
	start := time.Now()
	defer func() {
		v2.ShowPubHistogram.Observe(time.Since(start).Seconds())
	}()

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	tenantInfo := ses.GetTenantInfo()

	like := ""
	if sp.Like != nil {
		right, ok := sp.Like.Right.(*tree.NumVal)
		if !ok || right.Value.Kind() != constant.String {
			err = moerr.NewInternalError(ctx, "like clause must be a string")
			return
		}
		like = constant.StringVal(right.Value)
	}

	pubInfos, err := getPubInfos(ctx, bh, like)
	if err != nil {
		return
	}

	accIdInfoMap, _, err := getAccounts(ctx, bh)
	if err != nil {
		return
	}

	var rs = &MysqlResultSet{}
	for _, column := range showPublicationsOutputColumns {
		rs.AddColumn(column)
	}
	for _, pubInfo := range pubInfos {
		var updateTime interface{}
		if len(pubInfo.UpdateTime) > 0 {
			updateTime = pubInfo.UpdateTime
		}

		subAccountsStr := pubInfo.SubAccountsStr
		if subAccountsStr == pubsub.AccountAll {
			subAccountsStr = pubsub.AccountAllOutput
		}

		var subscribedAccountNames []string
		subscribedInfos, err := getSubInfosFromPub(ctx, bh, tenantInfo.Tenant, pubInfo.PubName, true)
		if err != nil {
			return err
		}
		for _, subInfo := range subscribedInfos {
			subscribedAccountNames = append(subscribedAccountNames, accIdInfoMap[subInfo.SubAccountId].Name)
		}
		slices.Sort(subscribedAccountNames)

		rs.AddRow([]interface{}{
			pubInfo.PubName,
			pubInfo.DbName,
			pubInfo.TablesStr,
			subAccountsStr,
			strings.Join(subscribedAccountNames, pubsub.Sep),
			pubInfo.CreateTime,
			updateTime,
			pubInfo.Comment,
		})
	}
	ses.SetMysqlResultSet(rs)

	return trySaveQueryResult(ctx, ses, rs)
}

func doShowSubscriptions(ctx context.Context, ses *Session, ss *tree.ShowSubscriptions) (err error) {
	start := time.Now()
	defer func() {
		v2.ShowSubHistogram.Observe(time.Since(start).Seconds())
	}()

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	sql := getSubsSql + fmt.Sprintf(" and sub_account_id = %d", accountId)
	if !ss.All {
		sql += " and sub_name is not null"
	}

	like := ss.Like
	if like != nil {
		right, ok := like.Right.(*tree.NumVal)
		if !ok || right.Value.Kind() != constant.String {
			err = moerr.NewInternalError(ctx, "like clause must be a string")
			return
		}
		sql += fmt.Sprintf(" and pub_name like '%s' order by pub_name;", constant.StringVal(right.Value))
	} else {
		sql += " order by sub_time desc, pub_time desc;"
	}

	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	subInfos, err := extractSubInfosFromExecResult(ctx, erArray)
	if err != nil {
		return
	}

	var rs = &MysqlResultSet{}
	for _, column := range showSubscriptionsOutputColumns {
		rs.AddColumn(column)
	}
	for _, subInfo := range subInfos {
		var pubDbName, pubTables, pubComment, pubTime interface{}
		if subInfo.Status == pubsub.SubStatusNormal {
			pubDbName = subInfo.PubDbName
			pubTables = subInfo.PubTables
			pubComment = subInfo.PubComment
			pubTime = subInfo.PubTime
		}

		var subName, subTime interface{}
		if len(subInfo.SubName) > 0 {
			subName = subInfo.SubName
		}
		if len(subInfo.SubTime) > 0 {
			subTime = subInfo.SubTime
		}

		rs.AddRow([]interface{}{
			subInfo.PubName,
			subInfo.PubAccountName,
			pubDbName,
			pubTables,
			pubComment,
			pubTime,
			subName,
			subTime,
			int(subInfo.Status),
		})
	}
	ses.SetMysqlResultSet(rs)

	return trySaveQueryResult(ctx, ses, rs)
}

func getDbIdAndType(ctx context.Context, bh BackgroundExec, dbName string) (dbId uint64, dbType string, err error) {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return 0, "", err
	}

	sql, err := getSqlForGetDbIdAndType(ctx, dbName, true, uint64(accountId))
	if err != nil {
		return
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	if !execResultArrayHasData(erArray) {
		err = moerr.NewInternalError(ctx, "database '%s' does not exist", dbName)
		return
	}

	if dbId, err = erArray[0].GetUint64(ctx, 0, 0); err != nil {
		return
	}

	if dbType, err = erArray[0].GetString(ctx, 0, 1); err != nil {
		return
	}

	return
}

func showTablesFromDb(ctx context.Context, bh BackgroundExec, dbName string) (tables map[string]bool, err error) {
	sql := "show tables from " + dbName

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	tables = make(map[string]bool)
	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			table, err := result.GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}
			tables[table] = true
		}
	}

	return
}

func genPubTablesStr(ctx context.Context, bh BackgroundExec, dbName string, table tree.TableNames) (pubTablesStr string, err error) {
	if len(table) == 1 && string(table[0].ObjectName) == pubsub.TableAll {
		return pubsub.TableAll, nil
	}

	tablesInDb, err := showTablesFromDb(ctx, bh, dbName)
	if err != nil {
		return
	}

	tablesNames := make([]string, 0, len(table))
	for _, tableName := range table {
		tblName := string(tableName.ObjectName)
		if !tablesInDb[tblName] {
			err = moerr.NewInternalError(ctx, "table '%s' not exists", tblName)
			return
		}
		tablesNames = append(tablesNames, tblName)
	}
	slices.Sort(tablesNames)

	pubTablesStr = strings.Join(tablesNames, pubsub.Sep)
	return
}

func getSetAccounts(
	ctx context.Context,
	setAccounts tree.IdentifierList,
	accNameInfoMap map[string]*pubsub.AccountInfo,
	curAccName string,
) (map[int32]*pubsub.AccountInfo, error) {
	accountMap := make(map[int32]*pubsub.AccountInfo)
	for _, acc := range setAccounts {
		accName := string(acc)

		if accName == curAccName {
			return nil, moerr.NewInternalError(ctx, "can't publish to self")
		}

		accInfo, ok := accNameInfoMap[accName]
		if !ok {
			return nil, moerr.NewInternalError(ctx, "not existed account name '%s'", accName)
		}
		accountMap[accInfo.Id] = accInfo
	}
	return accountMap, nil
}

func getAddAccounts(
	ctx context.Context,
	addAccounts tree.IdentifierList,
	oldSubAccounts map[int32]*pubsub.AccountInfo,
	accNameInfoMap map[string]*pubsub.AccountInfo,
	curAccName string,
) (map[int32]*pubsub.AccountInfo, error) {
	accountMap := make(map[int32]*pubsub.AccountInfo)
	for _, acc := range oldSubAccounts {
		accountMap[acc.Id] = acc
	}

	for _, acc := range addAccounts {
		accName := string(acc)

		if accName == curAccName {
			return nil, moerr.NewInternalError(ctx, "can't publish to self")
		}

		accInfo, ok := accNameInfoMap[accName]
		if !ok {
			return nil, moerr.NewInternalError(ctx, "not existed account name '%s'", accName)
		}
		accountMap[accInfo.Id] = accInfo
	}
	return accountMap, nil
}

func getDropAccounts(
	dropAccounts tree.IdentifierList,
	oldSubAccounts map[int32]*pubsub.AccountInfo,
	accNameInfoMap map[string]*pubsub.AccountInfo,
) (map[int32]*pubsub.AccountInfo, error) {
	accountMap := make(map[int32]*pubsub.AccountInfo)
	for _, acc := range oldSubAccounts {
		accountMap[acc.Id] = acc
	}

	for _, acc := range dropAccounts {
		accName := string(acc)
		if accInfo, ok := accNameInfoMap[accName]; ok {
			delete(accountMap, accInfo.Id)
		}
	}
	return accountMap, nil
}

func getSqlForUpdatePubInfo(ctx context.Context, pubName string, accountList string, comment string, dbName string, dbId uint64, tablesStr string, checkNameValid bool) (string, error) {
	if checkNameValid {
		err := inputNameIsInvalid(ctx, pubName)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf(updatePubInfoFormat, accountList, comment, dbName, dbId, tablesStr, pubName), nil
}

func insertMoSubs(ctx context.Context, bh BackgroundExec, subInfo *pubsub.SubInfo) (err error) {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := fmt.Sprintf(insertIntoMoSubsFormat, subInfo.SubAccountId, subInfo.PubAccountName, subInfo.PubName, subInfo.PubDbName, subInfo.PubTables, subInfo.PubComment, subInfo.Status)
	return bh.Exec(ctx, sql)
}

func batchInsertMoSubs(
	ctx context.Context,
	bh BackgroundExec,
	pubDbName, pubTables, pubComment string,
	pubAccountName, pubName string,
	accIds []int32,
) (err error) {
	if len(accIds) == 0 {
		return
	}

	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	values := make([]string, 0, len(accIds))
	// sub_account_id, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status
	valuesFormat := "(%d, '%s', '%s', '%s', '%s', now(), '%s', %d)"
	for _, accId := range accIds {
		values = append(values, fmt.Sprintf(valuesFormat,
			accId,
			pubAccountName, pubName,
			pubDbName, pubTables, pubComment,
			pubsub.SubStatusNormal,
		))
	}
	sql := fmt.Sprintf(batchInsertIntoMoSubsFormat, strings.Join(values, ","))
	return bh.Exec(ctx, sql)
}

func batchUpdateMoSubs(
	ctx context.Context,
	bh BackgroundExec,
	pubDbName, pubTables, pubComment string,
	status pubsub.SubStatus,
	pubAccountName, pubName string,
	accIds []int32,
) (err error) {
	if len(accIds) == 0 {
		return
	}

	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := fmt.Sprintf(batchUpdateMoSubsFormat,
		pubDbName, pubTables, pubComment,
		status,
		pubAccountName, pubName,
		pubsub.JoinAccountIds(accIds),
	)
	return bh.Exec(ctx, sql)
}

func deleteMoSubsBySubAccountId(ctx context.Context, bh BackgroundExec) (err error) {
	subAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := fmt.Sprintf(deleteMoSubsRecordsBySubAccountIdFormat, subAccountId)
	return bh.Exec(ctx, sql)
}

func batchDeleteMoSubs(
	ctx context.Context,
	bh BackgroundExec,
	pubAccountName, pubName string,
	accIds []int32,
) (err error) {
	if len(accIds) == 0 {
		return
	}

	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := fmt.Sprintf(batchDeleteMoSubsFormat,
		pubAccountName, pubName,
		pubsub.JoinAccountIds(accIds),
	)
	return bh.Exec(ctx, sql)
}

func getSubscriptionMeta(ctx context.Context, dbName string, ses FeSession, txn TxnOperator) (*plan.SubscriptionMeta, error) {
	dbMeta, err := getGlobalPu().StorageEngine.Database(ctx, dbName, txn)
	if err != nil {
		ses.Errorf(ctx, "Get Subscription database %s meta error: %s", dbName, err.Error())
		return nil, moerr.NewNoDB(ctx)
	}

	if dbMeta.IsSubscription(ctx) {
		return checkSubscriptionValid(ctx, ses, dbName)
	}
	return nil, nil
}

func checkSubscriptionValidCommon(ctx context.Context, ses FeSession, subName, pubAccountName, pubName string) (subMeta *plan.SubscriptionMeta, err error) {
	start := time.Now()
	defer func() {
		v2.CheckSubValidDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	var (
		sql, accStatus string
		erArray        []ExecResult
		accId          int64
	)

	tenantInfo := ses.GetTenantInfo()
	if tenantInfo != nil && pubAccountName == tenantInfo.GetTenant() {
		err = moerr.NewInternalError(ctx, "can not subscribe to self")
		return
	}

	newCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	//get pubAccountId from publication info
	if sql, err = getSqlForAccountIdAndStatus(newCtx, pubAccountName, true); err != nil {
		return
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(newCtx, sql); err != nil {
		return
	}

	if erArray, err = getResultSet(newCtx, bh); err != nil {
		return
	}

	if !execResultArrayHasData(erArray) {
		err = moerr.NewInternalError(newCtx, "there is no publication account %s", pubAccountName)
		return
	}
	if accId, err = erArray[0].GetInt64(newCtx, 0, 0); err != nil {
		return
	}

	if accStatus, err = erArray[0].GetString(newCtx, 0, 1); err != nil {
		return
	}

	if accStatus == tree.AccountStatusSuspend.String() {
		err = moerr.NewInternalError(newCtx, "the account %s is suspended", pubAccountName)
		return
	}

	//check the publication is already exist or not
	newCtx = defines.AttachAccountId(ctx, uint32(accId))
	pubInfo, err := getPubInfo(newCtx, bh, pubName)
	if err != nil {
		return
	}
	if pubInfo == nil {
		err = moerr.NewInternalError(newCtx, "there is no publication %s", pubName)
		return
	}

	if tenantInfo != nil && !pubInfo.InSubAccounts(tenantInfo.GetTenant()) {
		ses.Error(ctx,
			"checkSubscriptionValidCommon",
			zap.String("subName", subName),
			zap.String("pubAccountName", pubAccountName),
			zap.String("pubName", pubName),
			zap.String("databaseName", pubInfo.DbName),
			zap.String("accountList", pubInfo.SubAccountsStr),
			zap.String("tenant", tenantInfo.GetTenant()))
		err = moerr.NewInternalError(newCtx, "the account %s is not allowed to subscribe the publication %s", tenantInfo.GetTenant(), pubName)
		return
	}

	subMeta = &plan.SubscriptionMeta{
		Name:        pubName,
		AccountId:   int32(accId),
		DbName:      pubInfo.DbName,
		AccountName: pubAccountName,
		SubName:     subName,
		Tables:      pubInfo.TablesStr,
	}
	return
}

func checkSubscriptionValid(ctx context.Context, ses FeSession, dbName string) (*plan.SubscriptionMeta, error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	subInfos, err := getSubInfosFromSub(ctx, bh, dbName)
	if err != nil {
		return nil, err
	} else if len(subInfos) == 0 {
		return nil, moerr.NewInternalError(ctx, "there is no subscription for database %s", dbName)
	}

	subInfo := subInfos[0]
	return checkSubscriptionValidCommon(ctx, ses, subInfo.SubName, subInfo.PubAccountName, subInfo.PubName)
}

func isDbPublishing(ctx context.Context, dbName string, ses FeSession) (ok bool, err error) {
	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()
	var (
		sql     string
		erArray []ExecResult
		count   int64
	)

	if _, isSysDb := sysDatabases[dbName]; isSysDb {
		return false, err
	}

	sql, err = getSqlForDbPubCount(ctx, dbName)
	if err != nil {
		return false, err
	}

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return false, err
	}
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	if !execResultArrayHasData(erArray) {
		return false, moerr.NewInternalError(ctx, "there is no publication for database %s", dbName)
	}
	count, err = erArray[0].GetInt64(ctx, 0, 0)
	if err != nil {
		return false, err
	}

	return count > 0, err
}

func dropSubAccountNameInSubAccounts(ctx context.Context, bh BackgroundExec, pubAccountId int32, pubName, subAccountName string) (err error) {
	ctx = defines.AttachAccountId(ctx, uint32(pubAccountId))

	pubInfo, err := getPubInfo(ctx, bh, pubName)
	if err != nil {
		return err
	} else if pubInfo == nil {
		return
	}

	accountNames := pubInfo.GetSubAccountNames()
	idx := slices.Index(accountNames, subAccountName)
	// if not in accountNames, return
	if idx == -1 {
		return
	}

	var str string
	str1 := strings.Join(accountNames[:idx], pubsub.Sep)
	str2 := strings.Join(accountNames[idx+1:], pubsub.Sep)
	if len(str1) == 0 {
		str = str2
	} else if len(str2) == 0 {
		str = str1
	} else {
		str = str1 + pubsub.Sep + str2
	}

	sql := fmt.Sprintf(updatePubInfoAccountListFormat, str, pubName)
	return bh.Exec(ctx, sql)
}

func getSqlForInsertIntoMoPubs(ctx context.Context, pubName, databaseName string, databaseId uint64, allTable bool, tableList, accountList string, comment string, checkNameValid bool) (string, error) {
	if checkNameValid {
		err := inputNameIsInvalid(ctx, pubName, databaseName)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf(insertIntoMoPubsFormat, pubName, databaseName, databaseId, allTable, tableList, accountList, defines.GetRoleId(ctx), defines.GetUserId(ctx), comment), nil
}

func getSqlForGetDbIdAndType(ctx context.Context, dbName string, checkNameValid bool, accountId uint64) (string, error) {
	if checkNameValid {
		err := inputNameIsInvalid(ctx, dbName)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf(getDbIdAndTypFormat, dbName, accountId), nil
}

func getSqlForDropPubInfo(ctx context.Context, pubName string, checkNameValid bool) (string, error) {
	if checkNameValid {
		err := inputNameIsInvalid(ctx, pubName)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf(dropPubFormat, pubName), nil
}
