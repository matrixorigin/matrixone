// Copyright 2024 Matrix Origin
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

package versions

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func AddUpgradeTenantTask(
	upgradeID uint64,
	version string,
	fromAccountID int32,
	toAccountID int32,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf(`insert into %s (
			upgrade_id, 
			target_version, 
			from_account_id, 
			to_account_id, 
			ready, 
			create_at, 
			update_at) values (%d, '%s', %d, %d, %d, current_timestamp(), current_timestamp())`,
		catalog.MOUpgradeTenantTable,
		upgradeID,
		version,
		fromAccountID,
		toAccountID,
		No)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func UpdateUpgradeTenantTaskState(
	taskID uint64,
	state int32,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf("update %s set ready = %d where id = %d",
		catalog.MOUpgradeTenantTable,
		state,
		taskID)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func GetUpgradeTenantTasks(
	upgradeID uint64,
	txn executor.TxnExecutor) (uint64, []int32, []string, error) {
	taskID := uint64(0)
	tenants := make([]int32, 0)
	versions := make([]string, 0)
	after := int32(0)
	for {
		sql := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
			catalog.MOUpgradeTenantTable,
			after,
			upgradeID,
			No)
		res, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return 0, nil, nil, err
		}
		from := int32(-1)
		to := int32(-1)
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			taskID = vector.GetFixedAt[uint64](cols[0], 0)
			from = vector.GetFixedAt[int32](cols[1], 0)
			to = vector.GetFixedAt[int32](cols[2], 0)
			return true
		})
		res.Close()
		if from == -1 {
			return 0, nil, nil, nil
		}

		sql = fmt.Sprintf("select account_id, create_version from mo_account where account_id >= %d and account_id <= %d for update",
			from, to)
		res, err = txn.Exec(sql, executor.StatementOption{}.WithWaitPolicy(lock.WaitPolicy_FastFail))
		if err != nil {
			if isConflictError(err) {
				after = to + 1
				continue
			}
			return 0, nil, nil, err
		}
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				tenants = append(tenants, vector.GetFixedAt[int32](cols[0], i))
				versions = append(versions, cols[1].GetStringAt(i))
			}
			return true
		})
		res.Close()
		return taskID, tenants, versions, nil
	}
}

func GetTenantCreateVersionForUpdate(
	tenantID int32,
	txn executor.TxnExecutor) (string, error) {
	sql := fmt.Sprintf("select create_version from mo_account where account_id = %d for update", tenantID)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return "", err
	}
	defer res.Close()
	version := ""
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		version = cols[0].GetStringAt(0)
		return true
	})
	if version == "" {
		panic(fmt.Sprintf("BUG: missing tenant: %d", tenantID))
	}
	return version, nil
}

func UpgradeTenantVersion(
	tenantID int32,
	version string,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf("update mo_account set create_version = '%s' where account_id = %d",
		version,
		tenantID)
	res, err := txn.Exec(sql, executor.StatementOption{})
	if err != nil {
		return err
	}
	defer res.Close()
	if res.AffectedRows != 1 {
		panic(fmt.Sprintf("BUG: update tenant: %d failed with AffectedRows %d",
			tenantID, res.AffectedRows))
	}
	return nil
}

func isConflictError(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrLockConflict)
}
