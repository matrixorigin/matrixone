package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func (s *service) asyncUpgradeTenantTask(ctx context.Context) {
	fn := func() error {
		opts := executor.Options{}.
			WithMinCommittedTS(s.now()).
			WithWaitCommittedLogApplied()
		return s.exec.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				upgrade, ok, err := versions.GetUpgradeTenantVersions(txn)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				if upgrade.TotalTenant == upgrade.ReadyTenant {
					panic("BUG: invalid upgrade tenant")
				}

				// no upgrade logic on current cn, skip
				v := getFinalUpgradeHandle().Metadata().Version
				if versions.Compare(upgrade.ToVersion, v) > 0 {
					return nil
				}

				// select need upgrade tenants for update
				taskID, tenants, err := versions.GetUpgradeTenantTasks(upgrade.ID, txn)
				if err != nil {
					return err
				}

				h := getUpgradeHandle(upgrade.ToVersion)
				for _, id := range tenants {
					if err := h.HandleTenantUpgrade(ctx, id, txn); err != nil {
						return err
					}
				}
				if err := versions.UpdateUpgradeTenantTaskState(taskID, versions.Yes, txn); err != nil {
					return err
				}

				upgrade.ReadyTenant += int32(len(tenants))
				if upgrade.TotalTenant < upgrade.ReadyTenant {
					panic("BUG: invalid upgrade tenant")
				}
				return versions.UpdateVersionUpgradeTasks(upgrade, txn)
			},
			opts)
	}

	timer := time.NewTimer(checkUpgradeTenantDuration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := fn(); err != nil {
				continue
			}
			timer.Reset(checkUpgradeTenantDuration)
		}
	}
}

func fetchTenants(
	batch int,
	fn func([]int32) error,
	txn executor.TxnExecutor) error {
	last := int32(0)
	var ids []int32
	for {
		ids = ids[:0]
		sql := fmt.Sprintf("select account_id from mo_account where account_id > %d order by account_id limit %d",
			last,
			batch)
		res, err := txn.Exec(sql)
		if err != nil {
			return err
		}
		n := 0
		res.ReadRows(func(cols []*vector.Vector) bool {
			last = executor.GetFixedRows[int32](cols[0])[0]
			ids = append(ids, last)
			n++
			return true
		})
		res.Close()
		if n == 0 {
			return nil
		}
		if err := fn(ids); err != nil {
			return err
		}
	}
}
