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
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	insertIntoMoSnapshots = `insert into mo_catalog.mo_snapshots(
		snapshot_id,
		sname,
		ts,
		level,
		account_name,
		database_name,
		table_name,
		obj_id ) values ('%s', '%s', %d, '%s', '%s', '%s', '%s', %d);`

	dropSnapshotFormat = `delete from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id;`

	checkSnapshotFormat = `select snapshot_id from mo_catalog.mo_snapshots where sname = "%s" order by snapshot_id;`

	getSnapshotTsWithSnapshotNameFormat = `select ts from mo_catalog.mo_snapshots where sname = "%s" order by snapshot_id;`

	checkSnapshotTsFormat = `select snapshot_id from mo_catalog.mo_snapshots where ts = %d order by snapshot_id;`
)

func doCreateSnapshot(ctx context.Context, ses *Session, stmt *tree.CreateSnapShot) error {
	var err error
	var snapshotLevel tree.SnapshotLevel
	var snapshotForAccount string
	var snapshotName string
	var snapshotExist bool
	var snapshotId string
	var databaseName string
	var tableName string
	var sql string
	var objId uint64

	// check create stage priv
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check create snapshot priv

	// 1.only admin can create tenant level snapshot
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}
	// 2.only sys can create cluster level snapshot
	tenantInfo := ses.GetTenantInfo()
	currentAccount := tenantInfo.GetTenant()
	snapshotLevel = stmt.Object.SLevel.Level
	if snapshotLevel == tree.SNAPSHOTLEVELCLUSTER && currentAccount != sysAccountName {
		return moerr.NewInternalError(ctx, "only sys tenant can create cluster level snapshot")
	}

	// 3.only sys can create tenant level snapshot for other tenant
	if snapshotLevel == tree.SNAPSHOTLEVELACCOUNT {
		snapshotForAccount = string(stmt.Object.ObjName)
		if currentAccount != sysAccountName && currentAccount != snapshotForAccount {
			return moerr.NewInternalError(ctx, "only sys tenant can create tenant level snapshot for other tenant")
		}

		// check account exists or not and get accountId
		getAccountIdFunc := func(accountName string) (accountId uint64, rtnErr error) {
			var erArray []ExecResult
			sql, rtnErr = getSqlForCheckTenant(ctx, accountName)
			if rtnErr != nil {
				return 0, rtnErr
			}
			bh.ClearExecResultSet()
			rtnErr = bh.Exec(ctx, sql)
			if rtnErr != nil {
				return 0, rtnErr
			}

			erArray, rtnErr = getResultSet(ctx, bh)
			if rtnErr != nil {
				return 0, rtnErr
			}

			if execResultArrayHasData(erArray) {
				for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
					accountId, rtnErr = erArray[0].GetUint64(ctx, i, 0)
					if rtnErr != nil {
						return 0, rtnErr
					}
				}
			} else {
				return 0, moerr.NewInternalError(ctx, "account %s does not exist", accountName)
			}
			return accountId, rtnErr
		}

		// if sys tenant create snapshots for other tenant, get the account id
		// otherwise, get the account id from tenantInfo
		if currentAccount == sysAccountName && currentAccount != snapshotForAccount {
			objId, err = getAccountIdFunc(snapshotForAccount)
			if err != nil {
				return err
			}
		} else {
			objId = uint64(tenantInfo.GetTenantID())
		}
	}

	// check snapshot exists or not
	snapshotName = string(stmt.Name)
	snapshotExist, err = checkSnapShotExistOrNot(ctx, bh, snapshotName)
	if err != nil {
		return err
	}
	if snapshotExist {
		if !stmt.IfNotExists {
			return moerr.NewInternalError(ctx, "snapshot %s already exists", snapshotName)
		} else {
			return nil
		}
	} else {
		// insert record to the system table

		// 1. get snapshot id
		newUUid, err := uuid.NewV7()
		if err != nil {
			return err
		}
		snapshotId = newUUid.String()

		// 2. get snapshot ts
		// ts := ses.proc.TxnOperator.SnapshotTS()
		// snapshotTs = ts.String()

		sql, err = getSqlForCreateSnapshot(ctx, snapshotId, snapshotName, time.Now().UTC().UnixNano(), snapshotLevel.String(), string(stmt.Object.ObjName), databaseName, tableName, objId)
		if err != nil {
			return err
		}

		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}

	// insert record to the system table

	return err
}

func doDropSnapshot(ctx context.Context, ses *Session, stmt *tree.DropSnapShot) (err error) {
	var sql string
	var stageExist bool
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check create stage priv
	// only admin can drop snapshot for himself
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check stage
	stageExist, err = checkSnapShotExistOrNot(ctx, bh, string(stmt.Name))
	if err != nil {
		return err
	}

	if !stageExist {
		if !stmt.IfExists {
			return moerr.NewInternalError(ctx, "snapshot %s does not exist", string(stmt.Name))
		} else {
			// do nothing
			return err
		}
	} else {
		sql = getSqlForDropSnapshot(string(stmt.Name))
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

func doRestoreSnapshot(ctx context.Context, ses *Session, stmt *tree.RestoreSnapShot) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	srcAccountName := string(stmt.AccountName)
	dbName := string(stmt.DatabaseName)
	tblName := string(stmt.TableName)
	toAccountName := string(stmt.ToAccountName)

	accountId := getAccountIdByName(ctx, ses, bh, srcAccountName)
	if accountId == -1 {
		return moerr.NewInternalError(ctx, fmt.Sprintf("account does not exist, account name: %s", srcAccountName))
	}
	// default restore to self
	toAccountId := accountId

	// can't restore to another account if cur account is not sys
	if len(toAccountName) > 0 {
		// get current tenant
		curAccount := ses.GetTenantInfo()
		if !curAccount.IsSysTenant() {
			err = moerr.NewInternalError(ctx, "non-sys tenant can't restore to another account")
			return
		}

		//newAccountName := string(stmt.ToAccountName)
		//an := tree.NewNumValWithType(constant.MakeString(newAccountName), newAccountName, false, tree.P_char)
		//// TODO
		//// AccountStatusSuspend
		//// AccountStatusRestricted
		//as := tree.NewAccountStatus()
		//ac := tree.NewAccountComment()
		//st := tree.NewCreateAccount(false, an, stmt.AuthOption, *as, *ac)
		//if err = HandleCreateAccount(ctx, ses, st, proc); err != nil {
		//	return err
		//}

		newAccountName := string(stmt.ToAccountName)
		if toAccountId = getAccountIdByName(ctx, ses, bh, newAccountName); toAccountId == -1 {
			return moerr.NewInternalError(ctx, fmt.Sprintf("account does not exist, account name: %s", newAccountName))
		}
	}

	// TODO stop toAccount
	// TODO defer open toAccount

	snapshotTs, err := doResolveSnapshotTsWithSnapShotName(ctx, ses, string(stmt.SnapShotName))
	if err != nil {
		return
	}

	switch stmt.Level {
	case tree.RESTORELEVELCLUSTER:
		// TODO
	case tree.RESTORELEVELACCOUNT:
		restoreToAccount(snapshotTs, uint32(accountId), uint32(toAccountId))
	case tree.RESTORELEVELDATABASE:
		restoreToDatabase(snapshotTs, uint32(accountId), dbName, uint32(toAccountId))
	case tree.RESTORELEVELTABLE:
		restoreToTable(snapshotTs, uint32(accountId), dbName, tblName, uint32(toAccountId))

	}
	return
}

func restoreToAccount(snapshotTs int64, accountId uint32, toAccountId uint32) {}

func restoreToDatabase(snapshotTs int64, accountId uint32, dbName string, toAccountId uint32) {}

func restoreToTable(snapshotTs int64, accountId uint32, dbName string, tblName string, toAccountId uint32) {
}

func checkSnapShotExistOrNot(ctx context.Context, bh BackgroundExec, snapshotName string) (bool, error) {
	var sql string
	var erArray []ExecResult
	var err error
	sql, err = getSqlForCheckSnapshot(ctx, snapshotName)
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

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

func doResolveSnapshotTsWithSnapShotName(ctx context.Context, ses FeSession, spName string) (snapshotTs int64, err error) {
	var sql string
	var erArray []ExecResult
	err = inputNameIsInvalid(ctx, spName)
	if err != nil {
		return 0, err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	sql, err = getSqlForGetSnapshotTsWithSnapshotName(ctx, spName)
	if err != nil {
		return 0, err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return 0, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return 0, err
	}

	if execResultArrayHasData(erArray) {
		snapshotTs, err := erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return 0, err
		}

		return snapshotTs, nil
	} else {
		return 0, moerr.NewInternalError(ctx, "snapshot %s does not exist", spName)
	}
}

func checkTimeStampValid(ctx context.Context, ses FeSession, snapshotTs int64) (bool, error) {
	var sql string
	var err error
	var erArray []ExecResult
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	sql = getSqlForCheckSnapshotTs(snapshotTs)

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
		return false, err
	}

	return true, nil
}

func getSqlForCheckSnapshot(ctx context.Context, snapshot string) (string, error) {
	err := inputNameIsInvalid(ctx, snapshot)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(checkSnapshotFormat, snapshot), nil
}

func getSqlForGetSnapshotTsWithSnapshotName(ctx context.Context, snapshot string) (string, error) {
	err := inputNameIsInvalid(ctx, snapshot)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(getSnapshotTsWithSnapshotNameFormat, snapshot), nil
}

func getSqlForCheckSnapshotTs(snapshotTs int64) string {
	return fmt.Sprintf(checkSnapshotTsFormat, snapshotTs)
}

func getSqlForCreateSnapshot(ctx context.Context, snapshotId, snapshotName string, ts int64, level, accountName, databaseName, tableName string, objectId uint64) (string, error) {
	err := inputNameIsInvalid(ctx, snapshotName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(insertIntoMoSnapshots, snapshotId, snapshotName, ts, level, accountName, databaseName, tableName, objectId), nil
}

func getSqlForDropSnapshot(snapshotName string) string {
	return fmt.Sprintf(dropSnapshotFormat, snapshotName)
}
