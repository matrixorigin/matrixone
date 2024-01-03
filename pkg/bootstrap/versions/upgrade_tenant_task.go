package versions

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func AddUpgradeTenantTask(
	upgradeID uint64,
	version string,
	fromAccountID int32,
	toAccountID int32,
	txn executor.TxnExecutor) error {
	sql := fmt.Sprintf("insert into %s values (%d, '%s', %d, %d, %d, current_timestamp(), current_timestamp())",
		catalog.MOUpgradeTenantTable,
		upgradeID,
		version,
		fromAccountID,
		toAccountID,
		No)
	res, err := txn.Exec(sql)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func GetUpgradeTenantTasks(
	upgradeID uint64,
	txn executor.TxnExecutor) (uint64, []int32, error) {
	taskID := uint64(0)
	tenants := make([]int32, 0)
	after := int32(0)
	for {
		sql := fmt.Sprintf("select id, from_account_id, to_account_id from %s where from_account_id >= %d and upgrade_id = %d and ready = %d order by id limit 1",
			catalog.MOUpgradeTenantTable,
			after,
			upgradeID,
			No)
		res, err := txn.Exec(sql)
		if err != nil {
			return 0, nil, err
		}
		from := int32(-1)
		to := int32(-1)
		res.ReadRows(func(cols []*vector.Vector) bool {
			taskID = executor.GetFixedRows[uint64](cols[0])[0]
			from = executor.GetFixedRows[int32](cols[1])[0]
			to = executor.GetFixedRows[int32](cols[2])[0]
			return true
		})
		res.Close()
		if from == -1 {
			return 0, nil, nil
		}

		sql = fmt.Sprintf("select account_id from mo_account where account_id >= %d and account_id <= %d for update",
			from, to)
		res, err = txn.Exec(sql)
		if err != nil {
			if isConflictError(err) {
				after = to + 1
				continue
			}
			return 0, nil, err
		}
		res.ReadRows(func(cols []*vector.Vector) bool {
			tenants = append(tenants, executor.GetFixedRows[int32](cols[0])[0])
			return true
		})
		res.Close()
		return taskID, tenants, nil
	}
}

func isConflictError(err error) bool {
	return false
}
