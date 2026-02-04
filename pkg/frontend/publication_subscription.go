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
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/publication"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	// account
	getAccountIdNamesSql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1"
	// pub
	insertIntoMoPubsFormat         = `insert into mo_catalog.mo_pubs (account_id, account_name, pub_name, database_name, database_id, all_table, table_list, account_list, created_time, owner, creator, comment) values (%d, '%s', '%s', '%s', %d, %t, '%s', '%s', now(), %d, %d, '%s');`
	getAllPubInfoSql               = "select account_id, account_name, pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs"
	getPubInfoSqlOld               = "select account_id, pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where account_id = %d"
	getPubInfoSql                  = "select account_id, account_name, pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where account_id = %d"
	updatePubInfoFormat            = `update mo_catalog.mo_pubs set account_list = '%s', comment = '%s', database_name = '%s', database_id = %d, update_time = now(), table_list = '%s' where account_id = %d and pub_name = '%s';`
	updatePubInfoAccountListFormat = `update mo_catalog.mo_pubs set account_list = '%s' where account_id = %d and pub_name = '%s';`
	dropPubFormat                  = `delete from mo_catalog.mo_pubs where account_id = %d and pub_name = '%s';`
	getDbPubCountFormat            = `select count(1) from mo_catalog.mo_pubs where account_id = %d and database_name = '%s';`
	// sub
	insertIntoMoSubsFormat                  = "insert into mo_catalog.mo_subs (sub_account_id, sub_account_name, pub_account_id, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status) values (%d, '%s', %d, '%s', '%s', '%s', '%s', now(), '%s', %d)"
	batchInsertIntoMoSubsFormat             = "insert into mo_catalog.mo_subs (sub_account_id, sub_account_name, pub_account_id, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status) values %s"
	batchUpdateMoSubsFormat                 = "update mo_catalog.mo_subs set pub_database='%s', pub_tables='%s', pub_time=now(), pub_comment='%s', status=%d where pub_account_name = '%s' and pub_name = '%s' and sub_account_id in (%s)"
	batchDeleteMoSubsFormat                 = "delete from mo_catalog.mo_subs where pub_account_name = '%s' and pub_name = '%s' and sub_account_id in (%s)"
	getSubsSqlOld                           = "select sub_account_id, sub_name, sub_time, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status from mo_catalog.mo_subs where 1=1"
	getSubsSql                              = "select sub_account_id, sub_account_name, sub_name, sub_time, pub_account_id, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status from mo_catalog.mo_subs where 1=1"
	deleteMoSubsRecordsBySubAccountIdFormat = "delete from mo_catalog.mo_subs where sub_account_id = %d"
	// database
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

	showCcprSubscriptionsOutputColumns = [7]Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "task_id",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "database_name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "table_name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "sync_level",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "state",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "error_message",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "watermark",
				columnType: defines.MYSQL_TYPE_TIMESTAMP,
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

	accIdInfoMap, accNameInfoMap, err := getAccounts(ctx, bh, false)
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

	// Check if trying to publish to self (before validating other accounts)
	if !cp.AccountsSet.All {
		for _, acc := range cp.AccountsSet.SetAccounts {
			if string(acc) == accountName {
				return moerr.NewInternalError(ctx, "can't publish to self")
			}
		}
	}

	var subAccounts map[int32]*pubsub.AccountInfo
	if cp.AccountsSet.All {
		if accountId != sysAccountID && !pubsub.CanPubToAll(accountName, getPu(bh.Service()).SV.PubAllAccounts) {
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
		err = moerr.NewInternalErrorf(ctx, "Unknown database name '%s', not support publishing system database", dbName)
		return
	}

	// Try to find database in current account first
	dbId, dbType, err = getDbIdAndType(ctx, bh, dbName)
	if err != nil {
		// If not found in current account and ACCOUNT clause is specified, try to find in target accounts
		if !cp.AccountsSet.All && len(cp.AccountsSet.SetAccounts) > 0 {
			for _, accName := range cp.AccountsSet.SetAccounts {
				if accInfo, ok := accNameInfoMap[string(accName)]; ok {
					var foundDbId uint64
					var foundDbType string
					foundDbId, foundDbType, err = getDbIdAndTypeForAccount(ctx, bh, dbName, uint32(accInfo.Id))
					if err == nil {
						// Found database in target account, use it
						dbId = foundDbId
						dbType = foundDbType
						// Update context to the account where database exists for subsequent operations
						ctx = defines.AttachAccountId(ctx, uint32(accInfo.Id))
						break
					}
				}
			}
			// If still not found after checking all target accounts, return error
			if err != nil {
				return
			}
		} else {
			// No target accounts specified or ACCOUNT ALL, return the original error
			return
		}
	}
	if dbType != "" { //TODO: check the dat_type
		return moerr.NewInternalErrorf(ctx, "database '%s' is not a user database", cp.Database)
	}

	tablesStr := pubsub.TableAll
	if len(cp.Table) > 0 {
		if tablesStr, err = genPubTablesStr(ctx, bh, dbName, cp.Table); err != nil {
			return
		}
	}

	sql, err = getSqlForInsertIntoMoPubs(ctx, accountId, accountName, pubName, dbName, dbId, len(cp.Table) == 0, tablesStr, accountNamesStr, comment, true)
	if err != nil {
		return
	}

	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
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
			err = moerr.NewInternalErrorf(ctx, "unexpected subInfo.Status, actual: %v, expect: %v", subInfo.Status, pubsub.SubStatusDeleted)
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
		int32(accountId), accountName,
		pubName, dbName, tablesStr, comment,
		insertSubAccounts, subAccounts,
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

	accIdInfoMap, accNameInfoMap, err := getAccounts(ctx, bh, false)
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
		err = moerr.NewInternalErrorf(ctx, "publication '%s' does not exist", pubName)
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
			if accountId != sysAccountID && !pubsub.CanPubToAll(accountName, getPu(ses.GetService()).SV.PubAllAccounts) {
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
			return moerr.NewInternalErrorf(ctx, "Unknown database name '%s', not support publishing system database", dbName)
		}

		if dbId, dbType, err = getDbIdAndType(ctx, bh, dbName); err != nil {
			return err
		}
		if dbType != "" { //TODO: check the dat_type
			return moerr.NewInternalErrorf(ctx, "database '%s' is not a user database", dbName)
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
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
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
			err = moerr.NewInternalErrorf(ctx, "unexpected subInfo.Status: %v", subInfo.Status)
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
		int32(accountId), accountName,
		pubName, dbName, tablesStr, comment,
		insertSubAccounts, newSubAccounts,
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

	return dropPublication(ctx, bh, dp.IfExists, tenantInfo.Tenant, string(dp.Name))
}

func doDropCcprSubscription(ctx context.Context, ses *Session, dcs *tree.DropCcprSubscription) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// Switch to system account context to update mo_catalog
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	taskID := dcs.TaskID
	escapedTaskID := strings.ReplaceAll(taskID, "'", "''")

	// Check if subscription exists
	checkSQL := fmt.Sprintf(
		"SELECT COUNT(1) FROM mo_catalog.mo_ccpr_log WHERE task_id = '%s' AND drop_at IS NULL",
		escapedTaskID,
	)
	if accountId != catalog.System_Account {
		checkSQL += fmt.Sprintf(" AND account_id = %d", accountId)
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, checkSQL); err != nil {
		return err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	var count int64
	if execResultArrayHasData(erArray) {
		count, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
		}
	}

	if count == 0 {
		if !dcs.IfExists {
			return moerr.NewInternalErrorf(ctx, "subscription with task_id '%s' does not exist", taskID)
		}
		return nil
	}

	// Update mo_ccpr_log to set drop_at = now()
	updateSQL := fmt.Sprintf(
		"UPDATE mo_catalog.mo_ccpr_log SET drop_at = now() WHERE task_id = '%s' AND drop_at IS NULL",
		escapedTaskID,
	)
	if accountId != catalog.System_Account {
		updateSQL += fmt.Sprintf(" AND account_id = %d", accountId)
	}

	if err = bh.Exec(ctx, updateSQL); err != nil {
		return err
	}

	return nil
}

func doResumeCcprSubscription(ctx context.Context, ses *Session, rcs *tree.ResumeCcprSubscription) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// Switch to system account context to update mo_catalog
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	taskID := rcs.TaskID
	escapedTaskID := strings.ReplaceAll(taskID, "'", "''")

	// Check if subscription exists
	checkSQL := fmt.Sprintf(
		"SELECT COUNT(1) FROM mo_catalog.mo_ccpr_log WHERE task_id = '%s' AND drop_at IS NULL",
		escapedTaskID,
	)
	if accountId != catalog.System_Account {
		checkSQL += fmt.Sprintf(" AND account_id = %d", accountId)
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, checkSQL); err != nil {
		return err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	var count int64
	if execResultArrayHasData(erArray) {
		count, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
		}
	}

	if count == 0 {
		return moerr.NewInternalErrorf(ctx, "subscription with task_id '%s' does not exist", taskID)
	}

	// Update mo_ccpr_log: set state to running (0), reset iteration_state and error_message
	updateSQL := fmt.Sprintf(
		"UPDATE mo_catalog.mo_ccpr_log SET state = 0, iteration_state = 0, error_message = NULL WHERE task_id = '%s' AND drop_at IS NULL",
		escapedTaskID,
	)
	if accountId != catalog.System_Account {
		updateSQL += fmt.Sprintf(" AND account_id = %d", accountId)
	}

	if err = bh.Exec(ctx, updateSQL); err != nil {
		return err
	}

	return nil
}

func doPauseCcprSubscription(ctx context.Context, ses *Session, pcs *tree.PauseCcprSubscription) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// Switch to system account context to update mo_catalog
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	taskID := pcs.TaskID
	escapedTaskID := strings.ReplaceAll(taskID, "'", "''")

	// Check if subscription exists
	checkSQL := fmt.Sprintf(
		"SELECT COUNT(1) FROM mo_catalog.mo_ccpr_log WHERE task_id = '%s' AND drop_at IS NULL",
		escapedTaskID,
	)
	if accountId != catalog.System_Account {
		checkSQL += fmt.Sprintf(" AND account_id = %d", accountId)
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, checkSQL); err != nil {
		return err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	var count int64
	if execResultArrayHasData(erArray) {
		count, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
		}
	}

	if count == 0 {
		return moerr.NewInternalErrorf(ctx, "subscription with task_id '%s' does not exist", taskID)
	}

	// Update mo_ccpr_log: set state to pause (2)
	updateSQL := fmt.Sprintf(
		"UPDATE mo_catalog.mo_ccpr_log SET state = 2 WHERE task_id = '%s' AND drop_at IS NULL",
		escapedTaskID,
	)
	if accountId != catalog.System_Account {
		updateSQL += fmt.Sprintf(" AND account_id = %d", accountId)
	}

	if err = bh.Exec(ctx, updateSQL); err != nil {
		return err
	}

	return nil
}

// dropPublication drops a publication, bh should be in a transaction
func dropPublication(ctx context.Context, bh BackgroundExec, ifExists bool, accountName string, pubName string) (err error) {
	var sql string

	pub, err := getPubInfo(ctx, bh, pubName)
	if err != nil {
		return err
	}
	if pub == nil {
		if !ifExists {
			err = moerr.NewInternalErrorf(ctx, "publication '%s' does not exist", pubName)
		}
		return
	}

	if sql, err = getSqlForDropPubInfo(ctx, pubName, false); err != nil {
		return
	}
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	// subAccountName -> SubInfo map
	subInfos, err := getSubInfosFromPub(ctx, bh, accountName, pubName, false)
	if err != nil {
		return err
	}

	deleteSubAccounts := make([]int32, 0, len(subInfos))
	updateDeletedAccounts := make([]int32, 0, len(subInfos))
	for _, subInfo := range subInfos {
		if subInfo.Status == pubsub.SubStatusDeleted {
			err = moerr.NewInternalErrorf(ctx, "unexpected subInfo.Status: %v", subInfo.Status)
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

func getAccounts(ctx context.Context, bh BackgroundExec, forUpdate bool) (idInfoMap map[int32]*pubsub.AccountInfo, nameInfoMap map[string]*pubsub.AccountInfo, err error) {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := getAccountIdNamesSql
	if forUpdate {
		sql += " for update"
	}

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

func extractPubInfosFromExecResultOld(ctx context.Context, erArray []ExecResult) (pubInfos []*pubsub.PubInfo, err error) {
	var (
		accountId       int64
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
			if accountId, err = result.GetInt64(ctx, i, 0); err != nil {
				return
			}
			if pubName, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			if dbName, err = result.GetString(ctx, i, 2); err != nil {
				return
			}
			if dbId, err = result.GetUint64(ctx, i, 3); err != nil {
				return
			}
			if tablesStr, err = result.GetString(ctx, i, 4); err != nil {
				return
			}
			if accountNamesStr, err = result.GetString(ctx, i, 5); err != nil {
				return
			}
			if createTime, err = result.GetString(ctx, i, 6); err != nil {
				return
			}
			if isNull, err = result.ColumnIsNull(ctx, i, 7); err != nil {
				return
			} else if !isNull {
				if updateTime, err = result.GetString(ctx, i, 7); err != nil {
					return
				}
			} else {
				updateTime = ""
			}
			if owner, err = result.GetUint64(ctx, i, 8); err != nil {
				return
			}
			if creator, err = result.GetUint64(ctx, i, 9); err != nil {
				return
			}
			if comment, err = result.GetString(ctx, i, 10); err != nil {
				return
			}

			pubInfos = append(pubInfos, &pubsub.PubInfo{
				PubAccountId:   uint32(accountId),
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

func extractPubInfosFromExecResult(ctx context.Context, erArray []ExecResult) (pubInfos []*pubsub.PubInfo, err error) {
	var (
		accountId       int64
		accountName     string
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
			if accountId, err = result.GetInt64(ctx, i, 0); err != nil {
				return
			}
			if accountName, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			if pubName, err = result.GetString(ctx, i, 2); err != nil {
				return
			}
			if dbName, err = result.GetString(ctx, i, 3); err != nil {
				return
			}
			if dbId, err = result.GetUint64(ctx, i, 4); err != nil {
				return
			}
			if tablesStr, err = result.GetString(ctx, i, 5); err != nil {
				return
			}
			if accountNamesStr, err = result.GetString(ctx, i, 6); err != nil {
				return
			}
			if createTime, err = result.GetString(ctx, i, 7); err != nil {
				return
			}
			if isNull, err = result.ColumnIsNull(ctx, i, 8); err != nil {
				return
			} else if !isNull {
				if updateTime, err = result.GetString(ctx, i, 8); err != nil {
					return
				}
			} else {
				updateTime = ""
			}
			if owner, err = result.GetUint64(ctx, i, 9); err != nil {
				return
			}
			if creator, err = result.GetUint64(ctx, i, 10); err != nil {
				return
			}
			if comment, err = result.GetString(ctx, i, 11); err != nil {
				return
			}

			pubInfos = append(pubInfos, &pubsub.PubInfo{
				PubAccountId:   uint32(accountId),
				PubAccountName: accountName,
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
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}

	accountNameColExists, err := checkColExists(ctx, bh, "mo_catalog", "mo_pubs", "account_name")
	if err != nil {
		return
	}

	var sql string
	if accountNameColExists {
		sql = fmt.Sprintf(getPubInfoSql, accountId) + fmt.Sprintf(" and pub_name = '%s'", pubName)
	} else {
		sql = fmt.Sprintf(getPubInfoSqlOld, accountId) + fmt.Sprintf(" and pub_name = '%s'", pubName)
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	var pubInfos []*pubsub.PubInfo
	if accountNameColExists {
		if pubInfos, err = extractPubInfosFromExecResult(ctx, erArray); err != nil {
			return
		}
	} else {
		if pubInfos, err = extractPubInfosFromExecResultOld(ctx, erArray); err != nil {
			return
		}
	}
	if len(pubInfos) > 0 {
		pubInfo = pubInfos[0]
	}
	return
}

// getPubInfoByName gets publication info by name only, without filtering by account_id.
// This allows non-sys tenants to query publications created by any account (e.g., sys tenant).
func getPubInfoByName(ctx context.Context, bh BackgroundExec, pubName string) (pubInfo *pubsub.PubInfo, err error) {
	accountNameColExists, err := checkColExists(ctx, bh, "mo_catalog", "mo_pubs", "account_name")
	if err != nil {
		return
	}

	// Query by pub_name only, without account_id filter
	var sql string
	if accountNameColExists {
		sql = getAllPubInfoSql + fmt.Sprintf(" where pub_name = '%s'", pubName)
	} else {
		// For old schema without account_name column
		sql = "select account_id, pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs" +
			fmt.Sprintf(" where pub_name = '%s'", pubName)
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	var pubInfos []*pubsub.PubInfo
	if accountNameColExists {
		if pubInfos, err = extractPubInfosFromExecResult(ctx, erArray); err != nil {
			return
		}
	} else {
		if pubInfos, err = extractPubInfosFromExecResultOld(ctx, erArray); err != nil {
			return
		}
	}
	if len(pubInfos) > 0 {
		pubInfo = pubInfos[0]
	}
	return
}

func getPubInfos(ctx context.Context, bh BackgroundExec, like string) (pubInfos []*pubsub.PubInfo, err error) {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}
	sql := fmt.Sprintf(getPubInfoSql, accountId)
	if len(like) > 0 {
		sql += fmt.Sprintf(" and pub_name like '%s' order by pub_name", like)
	} else {
		sql += " order by update_time desc, created_time desc"
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractPubInfosFromExecResult(ctx, erArray)
}

func getPubInfosToAllAccounts(ctx context.Context, bh BackgroundExec) (pubInfos []*pubsub.PubInfo, err error) {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}
	sql := fmt.Sprintf(getPubInfoSql, accountId) + fmt.Sprintf(" and account_list = '%s'", pubsub.AccountAll)

	bh.ClearExecResultSet()
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractPubInfosFromExecResult(ctx, erArray)
}

func getPubInfosByDbname(ctx context.Context, bh BackgroundExec, dbName string) (pubInfo []*pubsub.PubInfo, err error) {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}
	sql := fmt.Sprintf(getPubInfoSql, accountId) + fmt.Sprintf(" and database_name = '%s'", dbName)

	bh.ClearExecResultSet()
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractPubInfosFromExecResult(ctx, erArray)
}

func getAllPubInfosBySnapshotName(ctx context.Context, bh BackgroundExec, snapshotName string, restoreTs int64) (pubInfo []*pubsub.PubInfo, err error) {
	getLogger("").Info("getAllPubInfosBySnapshotName", zap.String("snapshotName", snapshotName), zap.Int64("restoreTs", restoreTs))
	sql := getAllPubInfoSql + fmt.Sprintf(" {MO_TS = %d}", restoreTs)

	bh.ClearExecResultSet()
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractPubInfosFromExecResult(ctx, erArray)
}

func extractSubInfosFromExecResultOld(ctx context.Context, erArray []ExecResult) (subInfos []*pubsub.SubInfo, err error) {
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

func extractSubInfosFromExecResult(ctx context.Context, erArray []ExecResult) (subInfos []*pubsub.SubInfo, err error) {
	var (
		subAccountId   int64
		subAccountName string
		subName        string
		subTime        string
		pubAccountId   int64
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
			if subAccountName, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			if isNull, err = result.ColumnIsNull(ctx, i, 2); err != nil {
				return
			} else if !isNull {
				if subName, err = result.GetString(ctx, i, 2); err != nil {
					return
				}
			} else {
				subName = ""
			}
			if isNull, err = result.ColumnIsNull(ctx, i, 3); err != nil {
				return
			} else if !isNull {
				if subTime, err = result.GetString(ctx, i, 3); err != nil {
					return
				}
			} else {
				subTime = ""
			}
			if pubAccountId, err = result.GetInt64(ctx, i, 4); err != nil {
				return
			}
			if pubAccountName, err = result.GetString(ctx, i, 5); err != nil {
				return
			}
			if pubName, err = result.GetString(ctx, i, 6); err != nil {
				return
			}
			if pubDbName, err = result.GetString(ctx, i, 7); err != nil {
				return
			}
			if pubTables, err = result.GetString(ctx, i, 8); err != nil {
				return
			}
			if pubTime, err = result.GetString(ctx, i, 9); err != nil {
				return
			}
			if pubComment, err = result.GetString(ctx, i, 10); err != nil {
				return
			}
			if status, err = result.GetInt64(ctx, i, 11); err != nil {
				return
			}

			subInfos = append(subInfos, &pubsub.SubInfo{
				SubAccountId:   int32(subAccountId),
				SubAccountName: subAccountName,
				SubName:        subName,
				SubTime:        subTime,
				PubAccountId:   int32(pubAccountId),
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
		return
	}

	subAccountNameColExists, err := checkColExists(ctx, bh, "mo_catalog", "mo_subs", "sub_account_name")
	if err != nil {
		return
	}

	var sql string
	if subAccountNameColExists {
		sql = getSubsSql
	} else {
		sql = getSubsSqlOld
	}
	sql += fmt.Sprintf(" and sub_account_id = %d", subAccountId)
	if len(subName) > 0 {
		sql += fmt.Sprintf(" and sub_name = '%s'", subName)
	}

	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	if subAccountNameColExists {
		return extractSubInfosFromExecResult(ctx, erArray)
	}
	return extractSubInfosFromExecResultOld(ctx, erArray)
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
		if !ok || right.Kind() != tree.Str {
			err = moerr.NewInternalError(ctx, "like clause must be a string")
			return
		}
		like = right.String()
	}

	pubInfos, err := getPubInfos(ctx, bh, like)
	if err != nil {
		return
	}

	accIdInfoMap, _, err := getAccounts(ctx, bh, false)
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
		if !ok || right.Kind() != tree.Str {
			err = moerr.NewInternalError(ctx, "like clause must be a string")
			return
		}
		sql += fmt.Sprintf(" and pub_name like '%s' order by pub_name;", right.String())
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
			int8(subInfo.Status),
		})
	}
	ses.SetMysqlResultSet(rs)

	return trySaveQueryResult(ctx, ses, rs)
}

// maskPasswordInUri masks the password in a MySQL URI
// Format: mysql://[account#]user:password@host:port
// Returns: mysql://[account#]user:***@host:port
func maskPasswordInUri(uri string) string {
	const uriPrefix = "mysql://"
	if !strings.HasPrefix(uri, uriPrefix) {
		return uri
	}

	rest := uri[len(uriPrefix):]
	parts := strings.Split(rest, "@")
	if len(parts) != 2 {
		return uri
	}

	credPart := parts[0]
	hostPortPart := parts[1]

	// Check if there's a # separator for account
	var userPassPart string
	if strings.Contains(credPart, "#") {
		credParts := strings.SplitN(credPart, "#", 2)
		accountPart := credParts[0]
		userPassPart = credParts[1]
		// Replace password with ***
		if strings.Contains(userPassPart, ":") {
			userPassParts := strings.SplitN(userPassPart, ":", 2)
			user := userPassParts[0]
			maskedCredPart := accountPart + "#" + user + ":***"
			return uriPrefix + maskedCredPart + "@" + hostPortPart
		}
	} else {
		// Replace password with ***
		if strings.Contains(credPart, ":") {
			userPassParts := strings.SplitN(credPart, ":", 2)
			user := userPassParts[0]
			maskedCredPart := user + ":***"
			return uriPrefix + maskedCredPart + "@" + hostPortPart
		}
	}

	return uri
}

// extractWatermarkFromContext extracts watermark timestamp from context JSON
func extractWatermarkFromContext(contextJSON string) interface{} {
	if contextJSON == "" {
		return nil
	}

	var contextData map[string]interface{}
	if err := json.Unmarshal([]byte(contextJSON), &contextData); err != nil {
		return nil
	}

	// Try to find watermark in various possible locations
	if watermark, ok := contextData["watermark"]; ok {
		if ts, ok := watermark.(float64); ok {
			// Convert timestamp to time.Time
			return time.Unix(0, int64(ts))
		}
		if tsStr, ok := watermark.(string); ok {
			// Try parsing as timestamp string
			if t, err := time.Parse(time.RFC3339, tsStr); err == nil {
				return t
			}
		}
	}

	// Check for current_snapshot_ts as watermark
	if snapshotData, ok := contextData["current_snapshot_ts"]; ok {
		if ts, ok := snapshotData.(float64); ok {
			return time.Unix(0, int64(ts))
		}
	}

	return nil
}

func doShowCcprSubscriptions(ctx context.Context, ses *Session, scs *tree.ShowCcprSubscriptions) (err error) {
	start := time.Now()
	defer func() {
		v2.ShowSubHistogram.Observe(time.Since(start).Seconds())
	}()

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// Switch to system account context to query mo_catalog
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	// Build SQL query
	sql := "SELECT task_id, db_name, table_name, sync_level, state, error_message, watermark FROM mo_catalog.mo_ccpr_log WHERE 1=1"

	// Filter by task_id if specified
	if scs.TaskId != "" {
		escapedTaskId := strings.ReplaceAll(scs.TaskId, "'", "''")
		sql += fmt.Sprintf(" AND task_id = '%s'", escapedTaskId)
	}

	// Filter by account_id: sys account sees all, others see only their own
	if accountId != catalog.System_Account {
		sql += fmt.Sprintf(" AND account_id = %d", accountId)
	}

	sql += " ORDER BY created_at DESC;"

	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	var rs = &MysqlResultSet{}
	for _, column := range showCcprSubscriptionsOutputColumns {
		rs.AddColumn(column)
	}

	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			var (
				taskId         string
				dbName         string
				tableName      string
				syncLevel      string
				iterationState int64
				errorMessage   string
				watermarkTs    int64
				isNull         bool
			)

			// Extract values from result
			// SELECT task_id, db_name, table_name, sync_level, iteration_state, error_message, watermark
			if taskId, err = result.GetString(ctx, i, 0); err != nil {
				return err
			}
			// Handle nullable db_name
			if isNull, err = result.ColumnIsNull(ctx, i, 1); err != nil {
				return err
			}
			if !isNull {
				if dbName, err = result.GetString(ctx, i, 1); err != nil {
					return err
				}
			}
			// Handle nullable table_name
			if isNull, err = result.ColumnIsNull(ctx, i, 2); err != nil {
				return err
			}
			if !isNull {
				if tableName, err = result.GetString(ctx, i, 2); err != nil {
					return err
				}
			}
			if syncLevel, err = result.GetString(ctx, i, 3); err != nil {
				return err
			}
			if iterationState, err = result.GetInt64(ctx, i, 4); err != nil {
				return err
			}
			// Handle nullable error_message
			if isNull, err = result.ColumnIsNull(ctx, i, 5); err != nil {
				return err
			}
			if !isNull {
				if errorMessage, err = result.GetString(ctx, i, 5); err != nil {
					return err
				}
			}
			// Handle nullable watermark
			if isNull, err = result.ColumnIsNull(ctx, i, 6); err != nil {
				return err
			}
			if !isNull {
				if watermarkTs, err = result.GetInt64(ctx, i, 6); err != nil {
					return err
				}
			}

			// Map iteration_state to state string (0=running, 1=error, 2=pause, 3=dropped)
			var stateStr string
			switch int8(iterationState) {
			case 0:
				stateStr = "running"
			case 1:
				stateStr = "error"
			case 2:
				stateStr = "pause"
			case 3:
				stateStr = "dropped"
			default:
				stateStr = "unknown"
			}

			// Convert watermark timestamp to time string
			var watermark interface{}
			if watermarkTs > 0 {
				// watermarkTs is physical timestamp in nanoseconds
				watermark = time.Unix(0, watermarkTs).Format("2006-01-02 15:04:05")
			}

			// Handle NULL values for db_name and table_name
			var dbNameVal interface{}
			if dbName != "" {
				dbNameVal = dbName
			}

			var tableNameVal interface{}
			if tableName != "" {
				tableNameVal = tableName
			}

			rs.AddRow([]interface{}{
				taskId,       // task_id
				dbNameVal,    // database_name
				tableNameVal, // table_name
				syncLevel,    // sync_level
				stateStr,     // state
				errorMessage, // error_message
				watermark,    // watermark
			})
		}
	}

	ses.SetMysqlResultSet(rs)

	return trySaveQueryResult(ctx, ses, rs)
}

func doShowPublicationCoverage(ctx context.Context, ses *Session, spc *tree.ShowPublicationCoverage) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// Get current account name from session
	tenantInfo := ses.GetTenantInfo()
	if tenantInfo == nil {
		return moerr.NewInternalError(ctx, "failed to get tenant info")
	}
	accountName := tenantInfo.GetTenant()

	// Get publication info by name only (without filtering by current account_id)
	// This allows non-sys tenants to query publications created by sys tenant
	pubInfo, err := getPubInfoByName(ctx, bh, spc.Name)
	if err != nil {
		return err
	}
	if pubInfo == nil {
		return moerr.NewInternalErrorf(ctx, "publication '%s' does not exist", spc.Name)
	}

	// Check if current account is in subscribed accounts
	if !pubInfo.InSubAccounts(accountName) {
		return moerr.NewInternalErrorf(ctx, "account '%s' is not allowed to access publication '%s'", accountName, spc.Name)
	}

	// Build result set with columns: Database, Table
	var rs = &MysqlResultSet{}
	rs.AddColumn(&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "Database",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	})
	rs.AddColumn(&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "Table",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	})

	// Get database name
	dbName := pubInfo.DbName

	// Check if this is account level (dbName == "*") or database level (dbName is specific but tables is "*")
	isAccountLevel := dbName == pubsub.TableAll
	isDatabaseLevel := dbName != pubsub.TableAll && pubInfo.TablesStr == pubsub.TableAll

	// Add marker row for account level or database level
	if isAccountLevel {
		// Account level: add db * table *
		rs.AddRow([]interface{}{pubsub.TableAll, pubsub.TableAll})
	} else if isDatabaseLevel {
		// Database level: add db dbname table *
		rs.AddRow([]interface{}{dbName, pubsub.TableAll})
	}

	// Get table list
	if pubInfo.TablesStr == pubsub.TableAll {
		// If table_list is "*", query all tables from mo_tables
		// For account level, we don't query tables since dbName is "*"
		if !isAccountLevel {
			systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
			querySQL := fmt.Sprintf(
				`SELECT relname FROM mo_catalog.mo_tables WHERE reldatabase = '%s' AND account_id = %d AND relkind != '%s' ORDER BY relname`,
				dbName, pubInfo.PubAccountId, catalog.SystemViewRel,
			)

			bh.ClearExecResultSet()
			if err = bh.Exec(systemCtx, querySQL); err != nil {
				return err
			}

			erArray, err := getResultSet(systemCtx, bh)
			if err != nil {
				return err
			}

			for _, result := range erArray {
				for i := uint64(0); i < result.GetRowCount(); i++ {
					tableName, err := result.GetString(systemCtx, i, 0)
					if err != nil {
						return err
					}
					rs.AddRow([]interface{}{dbName, tableName})
				}
			}
		}
	} else {
		// If table_list is specific tables, split by comma
		tableNames := strings.Split(pubInfo.TablesStr, pubsub.Sep)
		for _, tableName := range tableNames {
			tableName = strings.TrimSpace(tableName)
			if tableName != "" {
				rs.AddRow([]interface{}{dbName, tableName})
			}
		}
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
		err = moerr.NewInternalErrorf(ctx, "database '%s' does not exist", dbName)
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

// getDbIdAndTypeForAccount tries to find database in the specified account
func getDbIdAndTypeForAccount(ctx context.Context, bh BackgroundExec, dbName string, accountId uint32) (dbId uint64, dbType string, err error) {
	sql, err := getSqlForGetDbIdAndType(ctx, dbName, true, uint64(accountId))
	if err != nil {
		return
	}

	// Use System_Account context to query mo_catalog.mo_database, as it's a system table
	// but filter by the specified accountId in SQL
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	if !execResultArrayHasData(erArray) {
		return 0, "", moerr.NewInternalErrorf(ctx, "database '%s' does not exist", dbName)
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
			err = moerr.NewInternalErrorf(ctx, "table '%s' not exists", tblName)
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

		accInfo, ok := accNameInfoMap[accName]
		if !ok {
			return nil, moerr.NewInternalErrorf(ctx, "not existed account name '%s'", accName)
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

		accInfo, ok := accNameInfoMap[accName]
		if !ok {
			return nil, moerr.NewInternalErrorf(ctx, "not existed account name '%s'", accName)
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
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return "", err
	}
	if checkNameValid {
		if err = inputNameIsInvalid(ctx, pubName); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf(updatePubInfoFormat, accountList, comment, dbName, dbId, tablesStr, accountId, pubName), nil
}

func insertMoSubs(ctx context.Context, bh BackgroundExec, subInfo *pubsub.SubInfo) (err error) {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := fmt.Sprintf(insertIntoMoSubsFormat, subInfo.SubAccountId, subInfo.SubAccountName, subInfo.PubAccountId, subInfo.PubAccountName, subInfo.PubName, subInfo.PubDbName, subInfo.PubTables, subInfo.PubComment, subInfo.Status)
	return bh.Exec(ctx, sql)
}

func batchInsertMoSubs(
	ctx context.Context,
	bh BackgroundExec,
	pubAccountId int32, pubAccountName,
	pubName, pubDbName, pubTables, pubComment string,
	accIds []int32,
	accIdInfoMap map[int32]*pubsub.AccountInfo,
) (err error) {
	if len(accIds) == 0 {
		return
	}

	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	values := make([]string, 0, len(accIds))
	// sub_account_id, sub_account_name, pub_account_id, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status
	valuesFormat := "(%d, '%s', %d, '%s', '%s', '%s', '%s', now(), '%s', %d)"
	for _, accId := range accIds {
		values = append(values, fmt.Sprintf(valuesFormat,
			accId, accIdInfoMap[accId].Name,
			pubAccountId, pubAccountName,
			pubName, pubDbName, pubTables, pubComment,
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

func getSubscriptionMeta(ctx context.Context, dbName string, ses FeSession, txn TxnOperator, bh BackgroundExec) (*plan.SubscriptionMeta, error) {
	dbMeta, err := getPu(ses.GetService()).StorageEngine.Database(ctx, dbName, txn)
	if err != nil {
		ses.Errorf(ctx, "Get Subscription database %s meta error: %s", dbName, err.Error())
		return nil, moerr.NewNoDB(ctx)
	}

	if dbMeta.IsSubscription(ctx) {
		return checkSubscriptionValid(ctx, ses, dbName, bh)
	}
	return nil, nil
}

func checkSubscriptionValidCommon(ctx context.Context, ses FeSession, subName, pubAccountName, pubName string, bh BackgroundExec) (subMeta *plan.SubscriptionMeta, err error) {
	start := time.Now()
	defer func() {
		v2.CheckSubValidDurationHistogram.Observe(time.Since(start).Seconds())
	}()
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
		err = moerr.NewInternalErrorf(newCtx, "there is no publication account %s", pubAccountName)
		return
	}
	if accId, err = erArray[0].GetInt64(newCtx, 0, 0); err != nil {
		return
	}

	if accStatus, err = erArray[0].GetString(newCtx, 0, 1); err != nil {
		return
	}

	if accStatus == tree.AccountStatusSuspend.String() {
		err = moerr.NewInternalErrorf(newCtx, "the account %s is suspended", pubAccountName)
		return
	}

	//check the publication is already exist or not
	newCtx = defines.AttachAccountId(ctx, uint32(accId))
	pubInfo, err := getPubInfo(newCtx, bh, pubName)
	if err != nil {
		return
	}
	if pubInfo == nil {
		err = moerr.NewInternalErrorf(newCtx, "there is no publication %s", pubName)
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
		err = moerr.NewInternalErrorf(newCtx, "the account %s is not allowed to subscribe the publication %s", tenantInfo.GetTenant(), pubName)
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

func checkSubscriptionValid(ctx context.Context, ses FeSession, dbName string, bh BackgroundExec) (*plan.SubscriptionMeta, error) {
	subInfos, err := getSubInfosFromSub(ctx, bh, dbName)
	if err != nil {
		return nil, err
	} else if len(subInfos) == 0 {
		return nil, moerr.NewInternalErrorf(ctx, "there is no subscription for database %s", dbName)
	}

	subInfo := subInfos[0]
	return checkSubscriptionValidCommon(ctx, ses, subInfo.SubName, subInfo.PubAccountName, subInfo.PubName, bh)
}

func isDbPublishing(ctx context.Context, dbName string, ses FeSession) (ok bool, err error) {
	if _, isSysDb := sysDatabases[dbName]; isSysDb {
		return
	}

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	var (
		sql     string
		erArray []ExecResult
		count   int64
	)

	if sql, err = getSqlForDbPubCount(ctx, dbName); err != nil {
		return
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql); err != nil {
		return
	}

	if erArray, err = getResultSet(ctx, bh); err != nil {
		return
	}
	if !execResultArrayHasData(erArray) {
		err = moerr.NewInternalErrorf(ctx, "there is no publication for database %s", dbName)
		return
	}

	count, err = erArray[0].GetInt64(ctx, 0, 0)
	ok = count > 0
	return
}

func dropSubAccountNameInSubAccounts(ctx context.Context, bh BackgroundExec, pubAccountId int32, pubName, subAccountName string) (err error) {
	pubInfo, err := getPubInfo(defines.AttachAccountId(ctx, uint32(pubAccountId)), bh, pubName)
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
	sql := fmt.Sprintf(updatePubInfoAccountListFormat, str, pubAccountId, pubName)

	return bh.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql)
}

func getSqlForInsertIntoMoPubs(ctx context.Context, accountId uint32, accountName string, pubName, databaseName string, databaseId uint64, allTable bool, tableList, accountList string, comment string, checkNameValid bool) (string, error) {
	if checkNameValid {
		if err := inputNameIsInvalid(ctx, pubName, databaseName); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(insertIntoMoPubsFormat, accountId, accountName, pubName, databaseName, databaseId, allTable, tableList, accountList, defines.GetRoleId(ctx), defines.GetUserId(ctx), comment), nil
}

func getSqlForGetDbIdAndType(ctx context.Context, dbName string, checkNameValid bool, accountId uint64) (string, error) {
	if checkNameValid {
		if err := inputNameIsInvalid(ctx, dbName); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf(getDbIdAndTypFormat, dbName, accountId), nil
}

func getSqlForDropPubInfo(ctx context.Context, pubName string, checkNameValid bool) (string, error) {
	if checkNameValid {
		if err := inputNameIsInvalid(ctx, pubName); err != nil {
			return "", err
		}
	}

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(dropPubFormat, accountId, pubName), nil
}

func getSqlForDbPubCount(ctx context.Context, dbName string) (string, error) {
	if err := inputNameIsInvalid(ctx, dbName); err != nil {
		return "", err
	}

	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(getDbPubCountFormat, accountId, dbName), nil
}

func checkColExists(ctx context.Context, bh BackgroundExec, dbName, tblName, colName string) (exists bool, err error) {
	sql := fmt.Sprintf("select 1 from mo_catalog.mo_columns where att_database = '%s' and att_relname = '%s' and attname = '%s'", dbName, tblName, colName)

	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return execResultArrayHasData(erArray), nil
}

// parseSubscriptionUri parses URI in format: mysql://<account>#<user>:<password>@<host>:<port>
// Returns account, user, password, host, port
func parseSubscriptionUri(uri string) (account, user, password, host string, port int, err error) {
	const uriPrefix = "mysql://"
	if !strings.HasPrefix(uri, uriPrefix) {
		return "", "", "", "", 0, moerr.NewInternalErrorNoCtx("invalid URI format, must start with mysql://")
	}

	rest := uri[len(uriPrefix):]
	// Split by @ to separate credentials from host:port
	parts := strings.Split(rest, "@")
	if len(parts) != 2 {
		return "", "", "", "", 0, moerr.NewInternalErrorNoCtx("invalid URI format, missing @ separator")
	}

	// Parse credentials part: account#user:password or user:password
	credPart := parts[0]
	var accountPart, userPassPart string
	if strings.Contains(credPart, "#") {
		credParts := strings.SplitN(credPart, "#", 2)
		accountPart = credParts[0]
		userPassPart = credParts[1]
	} else {
		userPassPart = credPart
	}

	// Parse user:password
	userPassParts := strings.SplitN(userPassPart, ":", 2)
	if len(userPassParts) != 2 {
		return "", "", "", "", 0, moerr.NewInternalErrorNoCtx("invalid URI format, missing password")
	}
	user = userPassParts[0]
	password = userPassParts[1]

	// Parse host:port
	hostPortPart := parts[1]
	hostPortParts := strings.Split(hostPortPart, ":")
	if len(hostPortParts) != 2 {
		return "", "", "", "", 0, moerr.NewInternalErrorNoCtx("invalid URI format, missing port")
	}
	host = hostPortParts[0]
	portInt, err := strconv.Atoi(hostPortParts[1])
	if err != nil {
		return "", "", "", "", 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid port: %v", err))
	}
	port = portInt

	return accountPart, user, password, host, port, nil
}

// newUpstreamExecutorFunc is a variable to allow mocking in tests
var newUpstreamExecutorFunc = publication.NewUpstreamExecutor

// checkUpstreamPublicationCoverage checks if the database/table is covered by the publication in upstream cluster
func checkUpstreamPublicationCoverage(
	ctx context.Context,
	account, user, password, host string,
	port int,
	pubName string,
	syncLevel string,
	dbName string,
	tableName string,
) error {
	// Create upstream executor to connect to upstream cluster
	upstreamExecutor, err := newUpstreamExecutorFunc(account, user, password, host, port, 3, 30*time.Second, "30s", publication.NewUpstreamConnectionClassifier())
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to connect to upstream cluster: %v", err)
	}
	defer upstreamExecutor.Close()

	// Execute SHOW PUBLICATION COVERAGE on upstream
	coverageSQL := fmt.Sprintf("SHOW PUBLICATION COVERAGE %s", pubName)
	result, cancel, err := upstreamExecutor.ExecSQL(ctx, nil, coverageSQL, false, false, 0)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to check publication coverage on upstream: %v", err)
	}
	defer cancel()
	defer result.Close()
	// Read coverage results
	coverageMap := make(map[string]map[string]bool) // dbName -> tableName -> exists
	for result.Next() {
		var coverageDbName, coverageTableName string
		if err := result.Scan(&coverageDbName, &coverageTableName); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to read coverage results: %v", err)
		}

		if coverageMap[coverageDbName] == nil {
			coverageMap[coverageDbName] = make(map[string]bool)
		}
		coverageMap[coverageDbName][coverageTableName] = true
	}

	// Check for errors during iteration
	if err := result.Err(); err != nil {
		return moerr.NewInternalErrorf(ctx, "error while reading coverage results: %v", err)
	}

	// Check if the requested database/table is covered based on sync level
	if syncLevel == "account" {
		// For account level, must find db * table * marker row
		if tables, exists := coverageMap[pubsub.TableAll]; !exists || !tables[pubsub.TableAll] {
			return moerr.NewInternalErrorf(ctx, "publication '%s' is not account level in upstream cluster", pubName)
		}
	} else if syncLevel == "database" {
		// For database level, must find db dbname table * marker row
		if tables, exists := coverageMap[dbName]; !exists || !tables[pubsub.TableAll] {
			return moerr.NewInternalErrorf(ctx, "database '%s' is not covered by publication '%s' in upstream cluster", dbName, pubName)
		}
	} else if syncLevel == "table" {
		// Check if table is covered
		if tables, exists := coverageMap[dbName]; !exists {
			return moerr.NewInternalErrorf(ctx, "database '%s' is not covered by publication '%s' in upstream cluster", dbName, pubName)
		} else if !tables[tableName] {
			return moerr.NewInternalErrorf(ctx, "table '%s.%s' is not covered by publication '%s' in upstream cluster", dbName, tableName, pubName)
		}
	}

	return nil
}

func doCreateSubscription(ctx context.Context, ses *Session, cs *tree.CreateSubscription) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	tenantInfo := ses.GetTenantInfo()
	if !tenantInfo.IsAdminRole() {
		return moerr.NewInternalError(ctx, "only admin can create subscription")
	}

	if err = bh.Exec(ctx, "begin;"); err != nil {
		return
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	ctx = defines.AttachAccount(ctx, tenantInfo.TenantID, tenantInfo.GetUserID(), tenantInfo.GetDefaultRoleID())
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// Parse URI
	account, user, password, host, port, err := parseSubscriptionUri(cs.FromUri)
	if err != nil {
		return err
	}

	// Initialize AES key for encryption (similar to CDC)
	// Query the data key from mo_data_key table
	querySql := cdc.CDCSQLBuilder.GetDataKeySQL(uint64(catalog.System_Account), cdc.InitKeyId)
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, querySql); err != nil {
		return err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}
	if len(erArray) == 0 || erArray[0].GetRowCount() == 0 {
		return moerr.NewInternalError(ctx, "no data key")
	}
	encryptedKey, err := erArray[0].GetString(ctx, 0, 0)
	if err != nil {
		return err
	}
	// Get KeyEncryptionKey from service
	pu := getPu(ses.GetService())
	cdc.AesKey, err = cdc.AesCFBDecodeWithKey(
		ctx,
		encryptedKey,
		[]byte(pu.SV.KeyEncryptionKey),
	)
	if err != nil {
		return err
	}

	// Create UriInfo and encrypt password
	uriInfo := cdc.UriInfo{
		User:     user,
		Password: password,
		Ip:       host,
		Port:     port,
	}
	encodedPassword, err := uriInfo.GetEncodedPassword()
	if err != nil {
		return err
	}

	// Build encrypted URI (similar to CDC format)
	// Format: mysql://account#user:encrypted_password@host:port
	// Always use account#user format for consistency, even if account is empty
	var encryptedUri string
	if account != "" {
		encryptedUri = fmt.Sprintf("mysql://%s#%s:%s@%s:%d", account, user, encodedPassword, host, port)
	} else {
		// Use empty account prefix for consistency
		encryptedUri = fmt.Sprintf("mysql://#%s:%s@%s:%d", user, encodedPassword, host, port)
	}

	// Determine sync_level
	var syncLevel string
	if cs.IsDatabase {
		if string(cs.DbName) == "" {
			syncLevel = "account"
		} else {
			syncLevel = "database"
		}
	} else {
		syncLevel = "table"
	}

	// Validate level and corresponding names
	if syncLevel == "account" {
		// For account level, dbName and tableName should be empty
		// No validation needed as they are already empty
		// Use current account ID (already set above)
	} else if syncLevel == "database" {
		// For database level, check dbName is not empty
		if string(cs.DbName) == "" {
			return moerr.NewInternalError(ctx, "database name cannot be empty for database level subscription")
		}
	} else {
		// For table level, check tableName is not empty
		if cs.TableName == "" {
			return moerr.NewInternalError(ctx, "table name cannot be empty for table level subscription")
		}
	}

	// Build sync_config JSON
	syncConfig := map[string]interface{}{}
	if cs.SyncInterval > 0 {
		syncConfig["sync_interval"] = cs.SyncInterval
	}
	syncConfigJSON, err := json.Marshal(syncConfig)
	if err != nil {
		return err
	}

	// Build INSERT SQL
	var dbName, tableName string
	if syncLevel == "account" {
		// For account level, both dbName and tableName should be empty
		dbName = ""
		tableName = ""
	} else if syncLevel == "database" {
		// For database level, dbName is required, tableName is empty
		dbName = string(cs.DbName)
		tableName = ""
	} else {
		// For table level, both dbName and tableName are required
		tableName = cs.TableName
		// First try to use database name from table name (e.g., `t`.`t`)
		if string(cs.DbName) != "" {
			dbName = string(cs.DbName)
		} else {
			// Fall back to current database context if not specified in table name
			dbName = ses.GetDatabaseName()
		}
		// For table level, check dbName is not empty after getting current database
		if dbName == "" {
			return moerr.NewInternalError(ctx, "database name cannot be empty for table level subscription")
		}
	}

	// Check upstream publication coverage before inserting into mo_ccpr_log
	if err = checkUpstreamPublicationCoverage(ctx, account, user, password, host, port, string(cs.PubName), syncLevel, dbName, tableName); err != nil {
		return err
	}

	// Query upstream DDL and create local DB/Table using internal commands
	// Returns: tableIDs map[dbName.tableName] -> tableID, indexTableMappings map[upstreamIndexTableName] -> downstreamIndexTableName
	tableIDs, indexTableMappings, err := queryUpstreamAndCreateLocalDBTables(ctx, ses, bh, account, user, password, host, port, syncLevel, dbName, tableName, accountId, string(cs.PubName), cs.SubscriptionAccountName)
	if err != nil {
		return err
	}

	// Build context JSON with tableIDs and indexTableMappings
	contextJSON, err := buildSubscriptionContextJSON(tableIDs, indexTableMappings)
	if err != nil {
		return err
	}

	// Check for duplicate CCPR task based on sync level
	// - account level: check if same account_id exists
	// - database level: check if same account_id + db_name exists
	// - table level: check if same account_id + db_name + table_name exists
	var duplicateCheckSQL string
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	switch syncLevel {
	case "account":
		duplicateCheckSQL = fmt.Sprintf(
			"SELECT COUNT(1) FROM mo_catalog.mo_ccpr_log WHERE account_id = %d AND drop_at IS NULL",
			accountId,
		)
	case "database":
		escapedDbName := strings.ReplaceAll(dbName, "'", "''")
		// Check if account level subscription exists OR same db_name subscription exists
		duplicateCheckSQL = fmt.Sprintf(
			"SELECT COUNT(1) FROM mo_catalog.mo_ccpr_log WHERE account_id = %d AND (db_name = '' OR db_name = '%s') AND drop_at IS NULL",
			accountId, escapedDbName,
		)
	case "table":
		escapedDbName := strings.ReplaceAll(dbName, "'", "''")
		escapedTableName := strings.ReplaceAll(tableName, "'", "''")
		// Check if account level subscription exists OR same db level subscription exists OR same table subscription exists
		duplicateCheckSQL = fmt.Sprintf(
			"SELECT COUNT(1) FROM mo_catalog.mo_ccpr_log WHERE account_id = %d AND (db_name = '' OR (db_name = '%s' AND table_name = '') OR (db_name = '%s' AND table_name = '%s')) AND drop_at IS NULL",
			accountId, escapedDbName, escapedDbName, escapedTableName,
		)
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, duplicateCheckSQL); err != nil {
		return err
	}
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}
	if len(erArray) > 0 && erArray[0].GetRowCount() > 0 {
		count, err := erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
		}
		if count > 0 {
			switch syncLevel {
			case "account":
				return moerr.NewInternalErrorf(ctx, "a subscription with account level for account_id %d already exists", accountId)
			case "database":
				return moerr.NewInternalErrorf(ctx, "a subscription with database level for account_id %d and database '%s' already exists", accountId, dbName)
			case "table":
				return moerr.NewInternalErrorf(ctx, "a subscription with table level for account_id %d and table '%s.%s' already exists", accountId, dbName, tableName)
			}
		}
	}

	// iteration_state: 2 = complete (based on design.md: 0='pending', 1='running', 2='complete', 3='error', 4='cancel')
	iterationState := int8(2) // complete
	// state: 0 = running (subscription state: 0=running, 1=error, 2=pause, 3=dropped)
	subscriptionState := int8(0) // running

	// Generate task_id first for ccpr_db and ccpr_table records
	taskID := uuid.New().String()

	sql := fmt.Sprintf(
		`INSERT INTO mo_catalog.mo_ccpr_log (
			task_id,
			subscription_name,
			subscription_account_name,
			sync_level,
			account_id,
			db_name,
			table_name,
			upstream_conn,
			sync_config,
			state,
			iteration_state,
			iteration_lsn,
			context
		) VALUES (
			'%s',
			'%s',
			'%s',
			'%s',
			%d,
			'%s',
			'%s',
			'%s',
			'%s',
			%d,
			%d,
			0,
			'%s'
		)`,
		taskID,
		string(cs.PubName),
		cs.SubscriptionAccountName,
		syncLevel,
		accountId,
		dbName,
		tableName,
		encryptedUri,
		string(syncConfigJSON),
		subscriptionState,
		iterationState,
		strings.ReplaceAll(contextJSON, "'", "''"),
	)

	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	if err = bh.Exec(ctx, sql); err != nil {
		return err
	}

	// Insert records into mo_ccpr_dbs and mo_ccpr_tables
	if err = insertCCPRDbAndTableRecords(ctx, bh, tableIDs, taskID); err != nil {
		return err
	}

	return nil
}

// TableIDInfo contains table ID and database ID information
type TableIDInfo struct {
	TableID uint64
	DbID    uint64
}

// queryUpstreamAndCreateLocalDBTables queries upstream for DDL using internal commands and creates local DB/Tables
// Uses GetDatabasesSQL and GetDdlSQL internal commands with level, dbName, tableName parameters
// Returns: tableIDs map[dbName.tableName] -> TableIDInfo, indexTableMappings map[upstreamIndexTableName] -> downstreamIndexTableName
func queryUpstreamAndCreateLocalDBTables(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	account, user, password, host string,
	port int,
	syncLevel string,
	dbName string,
	tableName string,
	accountId uint32,
	pubName string,
	subscriptionAccountName string,
) (map[string]TableIDInfo, map[string]string, error) {
	tableIDs := make(map[string]TableIDInfo)
	indexTableMappings := make(map[string]string)

	// Create upstream executor to connect to upstream cluster
	upstreamExecutor, err := newUpstreamExecutorFunc(account, user, password, host, port, 3, 30*time.Second, "30s", publication.NewUpstreamConnectionClassifier())
	if err != nil {
		return nil, nil, moerr.NewInternalErrorf(ctx, "failed to connect to upstream cluster: %v", err)
	}
	defer upstreamExecutor.Close()

	// Set context for downstream operations
	downstreamCtx := defines.AttachAccountId(ctx, accountId)

	// Use "-" as snapshot name placeholder - frontend will use current timestamp if snapshot is "-"
	snapshotName := "-"

	// Step 1: Get databases from upstream using internal command
	getDatabasesSQL := publication.PublicationSQLBuilder.GetDatabasesSQL(snapshotName, subscriptionAccountName, pubName, syncLevel, dbName, tableName)
	dbResult, cancelDb, err := upstreamExecutor.ExecSQL(ctx, nil, getDatabasesSQL, false, true, time.Minute)
	if err != nil {
		return nil, nil, moerr.NewInternalErrorf(ctx, "failed to get databases from upstream: %v", err)
	}
	defer cancelDb()
	defer dbResult.Close()

	// Collect database names and check/create locally
	createdDbs := make(map[string]uint64) // dbName -> dbID
	for dbResult.Next() {
		var upstreamDbName string
		if err := dbResult.Scan(&upstreamDbName); err != nil {
			return nil, nil, moerr.NewInternalErrorf(ctx, "failed to scan database result: %v", err)
		}

		// Check if database exists locally
		dbExists, err := checkDatabaseExists(downstreamCtx, bh, upstreamDbName)
		if err != nil {
			return nil, nil, err
		}

		if dbExists {
			return nil, nil, moerr.NewInternalErrorf(ctx, "db '%s' already exists locally", upstreamDbName)
		}

		// Get CREATE DATABASE DDL from upstream and create locally
		createDbSQL, err := getUpstreamCreateDatabaseDDL(ctx, upstreamExecutor, upstreamDbName)
		if err != nil {
			return nil, nil, err
		}
		if err = bh.Exec(downstreamCtx, createDbSQL); err != nil {
			return nil, nil, moerr.NewInternalErrorf(ctx, "failed to create database '%s': %v", upstreamDbName, err)
		}

		// Get database ID
		dbID, err := getDatabaseID(downstreamCtx, bh, upstreamDbName)
		if err != nil {
			return nil, nil, err
		}
		createdDbs[upstreamDbName] = dbID
	}

	// Step 2: Get DDL from upstream using internal command
	// GetDdlSQL returns: dbname, tablename, tableid, tablesql
	getDdlSQL := publication.PublicationSQLBuilder.GetDdlSQL(snapshotName, subscriptionAccountName, pubName, syncLevel, dbName, tableName)
	ddlResult, cancelDdl, err := upstreamExecutor.ExecSQL(ctx, nil, getDdlSQL, false, true, time.Minute)
	if err != nil {
		return nil, nil, moerr.NewInternalErrorf(ctx, "failed to get DDL from upstream: %v", err)
	}
	defer cancelDdl()
	defer ddlResult.Close()

	// Iterate through DDL results and create tables locally
	for ddlResult.Next() {
		var upstreamDbName, upstreamTableName, tableSQL string
		var upstreamTableID int64
		if err := ddlResult.Scan(&upstreamDbName, &upstreamTableName, &upstreamTableID, &tableSQL); err != nil {
			return nil, nil, moerr.NewInternalErrorf(ctx, "failed to scan DDL result: %v", err)
		}

		// Skip if table SQL is empty (might be system table or filtered out)
		if tableSQL == "" {
			continue
		}

		// Check if table exists locally
		tableExists, err := checkTableExists(downstreamCtx, bh, upstreamDbName, upstreamTableName)
		if err != nil {
			return nil, nil, err
		}

		if tableExists {
			return nil, nil, moerr.NewInternalErrorf(ctx, "table '%s.%s' already exists locally", upstreamDbName, upstreamTableName)
		}

		// Create table locally (need to switch to the database first)
		useDbSQL := fmt.Sprintf("USE `%s`", upstreamDbName)
		if err = bh.Exec(downstreamCtx, useDbSQL); err != nil {
			return nil, nil, moerr.NewInternalErrorf(ctx, "failed to use database '%s': %v", upstreamDbName, err)
		}

		if err = bh.Exec(downstreamCtx, tableSQL); err != nil {
			return nil, nil, moerr.NewInternalErrorf(ctx, "failed to create table '%s.%s': %v", upstreamDbName, upstreamTableName, err)
		}

		// Get local table ID
		localTableID, err := getTableID(downstreamCtx, bh, upstreamDbName, upstreamTableName)
		if err != nil {
			return nil, nil, err
		}

		// Get database ID (should already be in createdDbs)
		dbID, ok := createdDbs[upstreamDbName]
		if !ok {
			dbID, err = getDatabaseID(downstreamCtx, bh, upstreamDbName)
			if err != nil {
				return nil, nil, err
			}
		}

		key := fmt.Sprintf("%s.%s", upstreamDbName, upstreamTableName)
		tableIDs[key] = TableIDInfo{TableID: localTableID, DbID: dbID}

		// Get index table mappings (upstream index table name -> downstream index table name)
		upstreamIndexTables, err := getUpstreamIndexTables(ctx, upstreamExecutor, uint64(upstreamTableID), subscriptionAccountName, pubName, snapshotName)
		if err != nil {
			return nil, nil, err
		}

		downstreamIndexTables, err := getDownstreamIndexTables(downstreamCtx, bh, upstreamDbName, upstreamTableName)
		if err != nil {
			return nil, nil, err
		}

		// Match index tables by index name
		for upstreamIndexTable, upstreamIndexName := range upstreamIndexTables {
			for downstreamIndexTable, downstreamIndexName := range downstreamIndexTables {
				if upstreamIndexName == downstreamIndexName {
					indexTableMappings[upstreamIndexTable] = downstreamIndexTable
				}
			}
		}
	}

	return tableIDs, indexTableMappings, nil
}

// checkDatabaseExists checks if a database exists locally
func checkDatabaseExists(ctx context.Context, bh BackgroundExec, dbName string) (bool, error) {
	sql := fmt.Sprintf("SELECT 1 FROM mo_catalog.mo_database WHERE datname = '%s' LIMIT 1", strings.ReplaceAll(dbName, "'", "''"))
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return false, err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	return len(erArray) > 0 && erArray[0].GetRowCount() > 0, nil
}

// checkTableExists checks if a table exists locally
func checkTableExists(ctx context.Context, bh BackgroundExec, dbName, tableName string) (bool, error) {
	sql := fmt.Sprintf(
		"SELECT 1 FROM mo_catalog.mo_tables WHERE reldatabase = '%s' AND relname = '%s' LIMIT 1",
		strings.ReplaceAll(dbName, "'", "''"),
		strings.ReplaceAll(tableName, "'", "''"),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return false, err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}
	return len(erArray) > 0 && erArray[0].GetRowCount() > 0, nil
}

// getDatabaseID gets the database ID
func getDatabaseID(ctx context.Context, bh BackgroundExec, dbName string) (uint64, error) {
	sql := fmt.Sprintf("SELECT dat_id FROM mo_catalog.mo_database WHERE datname = '%s'", strings.ReplaceAll(dbName, "'", "''"))
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return 0, err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return 0, err
	}
	if len(erArray) == 0 || erArray[0].GetRowCount() == 0 {
		return 0, moerr.NewInternalErrorf(ctx, "database '%s' not found", dbName)
	}
	return erArray[0].GetUint64(ctx, 0, 0)
}

// getTableID gets the table ID
func getTableID(ctx context.Context, bh BackgroundExec, dbName, tableName string) (uint64, error) {
	sql := fmt.Sprintf(
		"SELECT rel_id FROM mo_catalog.mo_tables WHERE reldatabase = '%s' AND relname = '%s'",
		strings.ReplaceAll(dbName, "'", "''"),
		strings.ReplaceAll(tableName, "'", "''"),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return 0, err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return 0, err
	}
	if len(erArray) == 0 || erArray[0].GetRowCount() == 0 {
		return 0, moerr.NewInternalErrorf(ctx, "table '%s.%s' not found", dbName, tableName)
	}
	return erArray[0].GetUint64(ctx, 0, 0)
}

// getUpstreamCreateDatabaseDDL gets CREATE DATABASE DDL from upstream
func getUpstreamCreateDatabaseDDL(ctx context.Context, executor publication.SQLExecutor, dbName string) (string, error) {
	sql := fmt.Sprintf("SHOW CREATE DATABASE `%s`", dbName)
	result, cancel, err := executor.ExecSQL(ctx, nil, sql, false, false, 0)
	if err != nil {
		return "", moerr.NewInternalErrorf(ctx, "failed to get CREATE DATABASE DDL for '%s': %v", dbName, err)
	}
	defer cancel()
	defer result.Close()

	if result.Next() {
		var dbNameResult, createSQL string
		if err := result.Scan(&dbNameResult, &createSQL); err != nil {
			return "", moerr.NewInternalErrorf(ctx, "failed to scan CREATE DATABASE result: %v", err)
		}
		return createSQL, nil
	}

	return "", moerr.NewInternalErrorf(ctx, "no CREATE DATABASE result for '%s'", dbName)
}

// getUpstreamCreateTableDDL gets CREATE TABLE DDL from upstream
func getUpstreamCreateTableDDL(ctx context.Context, executor publication.SQLExecutor, dbName, tableName string) (string, error) {
	sql := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", dbName, tableName)
	result, cancel, err := executor.ExecSQL(ctx, nil, sql, false, false, 0)
	if err != nil {
		return "", moerr.NewInternalErrorf(ctx, "failed to get CREATE TABLE DDL for '%s.%s': %v", dbName, tableName, err)
	}
	defer cancel()
	defer result.Close()

	if result.Next() {
		var tableNameResult, createSQL string
		if err := result.Scan(&tableNameResult, &createSQL); err != nil {
			return "", moerr.NewInternalErrorf(ctx, "failed to scan CREATE TABLE result: %v", err)
		}
		return createSQL, nil
	}

	return "", moerr.NewInternalErrorf(ctx, "no CREATE TABLE result for '%s.%s'", dbName, tableName)
}

// getUpstreamIndexTables gets index tables from upstream for a given table using internal command
// Uses __++__internal_get_mo_indexes <tableId> <subscriptionAccountName> <publicationName> <snapshotName>
// Returns map[indexTableName] -> indexName
func getUpstreamIndexTables(ctx context.Context, executor publication.SQLExecutor, tableID uint64, subscriptionAccountName, pubName, snapshotName string) (map[string]string, error) {
	indexTables := make(map[string]string)

	// Query mo_indexes using internal command
	sql := publication.PublicationSQLBuilder.QueryMoIndexesSQL(tableID, subscriptionAccountName, pubName, snapshotName)
	result, cancel, err := executor.ExecSQL(ctx, nil, sql, false, false, 0)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to query upstream index tables: %v", err)
	}
	defer cancel()
	defer result.Close()

	// QueryMoIndexesSQL returns: table_id, name, algo_table_type, index_table_name
	for result.Next() {
		var resTableID int64
		var indexName, algoTableType, indexTableName string
		if err := result.Scan(&resTableID, &indexName, &algoTableType, &indexTableName); err != nil {
			return nil, moerr.NewInternalErrorf(ctx, "failed to scan upstream index table result: %v", err)
		}
		if indexTableName != "" {
			indexTables[indexTableName] = indexName
		}
	}

	return indexTables, nil
}

// getDownstreamIndexTables gets index tables from downstream for a given table
// Returns map[indexTableName] -> indexName
func getDownstreamIndexTables(ctx context.Context, bh BackgroundExec, dbName, tableName string) (map[string]string, error) {
	indexTables := make(map[string]string)

	// First get table_id from mo_tables
	tableIdSql := fmt.Sprintf(
		"SELECT rel_id FROM mo_catalog.mo_tables WHERE reldatabase = '%s' AND relname = '%s'",
		strings.ReplaceAll(dbName, "'", "''"),
		strings.ReplaceAll(tableName, "'", "''"),
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, tableIdSql); err != nil {
		return nil, err
	}
	tableIdResult, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if len(tableIdResult) == 0 || tableIdResult[0].GetRowCount() == 0 {
		// Table not found, return empty map
		return indexTables, nil
	}

	tableId, err := tableIdResult[0].GetInt64(ctx, 0, 0)
	if err != nil {
		return nil, err
	}

	// Query mo_indexes using table_id
	sql := fmt.Sprintf(
		"SELECT index_table_name, name FROM mo_catalog.mo_indexes WHERE table_id = %d AND index_table_name != ''",
		tableId,
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if len(erArray) > 0 {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			indexTableName, err := erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}
			indexName, err := erArray[0].GetString(ctx, i, 1)
			if err != nil {
				return nil, err
			}
			indexTables[indexTableName] = indexName
		}
	}

	return indexTables, nil
}

// buildSubscriptionContextJSON builds the context JSON for mo_ccpr_log
func buildSubscriptionContextJSON(tableIDs map[string]TableIDInfo, indexTableMappings map[string]string) (string, error) {
	// Convert tableIDs map to string keys for JSON serialization
	tableIDsForJSON := make(map[string]uint64)
	for key, info := range tableIDs {
		tableIDsForJSON[key] = info.TableID
	}

	contextData := map[string]interface{}{
		"table_ids":            tableIDsForJSON,
		"index_table_mappings": indexTableMappings,
		"aobject_map":          map[string]interface{}{}, // Empty initially
	}

	contextBytes, err := json.Marshal(contextData)
	if err != nil {
		return "", err
	}

	return string(contextBytes), nil
}

// insertCCPRDbAndTableRecords inserts records into mo_ccpr_dbs and mo_ccpr_tables
func insertCCPRDbAndTableRecords(ctx context.Context, bh BackgroundExec, tableIDs map[string]TableIDInfo, taskID string) error {
	// Track which database IDs we've already inserted
	insertedDbIDs := make(map[uint64]bool)

	for _, info := range tableIDs {
		// Insert into mo_ccpr_dbs if not already inserted
		if !insertedDbIDs[info.DbID] {
			sql := fmt.Sprintf(
				"INSERT INTO %s.%s (dbid, taskid) VALUES (%d, '%s')",
				catalog.MO_CATALOG,
				catalog.MO_CCPR_DBS,
				info.DbID,
				taskID,
			)
			if err := bh.Exec(ctx, sql); err != nil {
				return moerr.NewInternalErrorf(ctx, "failed to insert ccpr db record: %v", err)
			}
			insertedDbIDs[info.DbID] = true
		}

		// Insert into mo_ccpr_tables
		sql := fmt.Sprintf(
			"INSERT INTO %s.%s (tableid, taskid) VALUES (%d, '%s')",
			catalog.MO_CATALOG,
			catalog.MO_CCPR_TABLES,
			info.TableID,
			taskID,
		)
		if err := bh.Exec(ctx, sql); err != nil {
			return moerr.NewInternalErrorf(ctx, "failed to insert ccpr table record: %v", err)
		}
	}

	return nil
}
