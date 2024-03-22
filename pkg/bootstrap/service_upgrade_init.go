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

package bootstrap

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v1_2_0"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

func (s *service) initFramework(ctx context.Context) error {
	opts := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)
	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			final := s.getFinalVersionHandle().Metadata()

			// Deploy mo first time without 1.2.0, init framework first.
			// And upgrade to current version.
			created, err := versions.IsFrameworkTablesCreated(txn)
			if err != nil {
				getUpgradeLogger().Error("failed to check upgrade framework",
					zap.Error(err))
				return err
			}

			// First version as a genesis version, always need to be PREPARE.
			// Because the first version need to init upgrade framework tables.
			if len(s.handles) == 1 || !created {
				getUpgradeLogger().Info("init upgrade framework",
					zap.String("final-version", final.Version))

				// create new upgrade framework tables for the first time,
				// which means using v1.2.0 for the first time
				// NOTE: The `alter table` statements used for upgrading system table rely on `mo_foreign_keys`,
				// so preprocessing is performed first
				for _, upgEntry := range v1_2_0.TenantUpgPrepareEntres {
					err = upgEntry.Upgrade(txn, catalog.System_Account)
					if err != nil {
						getLogger().Error("prepare upgrade entry execute error", zap.Error(err), zap.String("upgrade entry", upgEntry.String()))
						return err
					}
				}

				// Many cn maybe create framework tables parallel, only one can create success.
				// Just return error, and upgrade framework will retry.
				err = createFrameworkTables(txn, final)
				if err != nil {
					getLogger().Error("create upgrade FrameworkTables error", zap.Error(err))
					return err
				}
				getLogger().Info("create upgrade FrameworkTables success")
			}
			return nil
		}, opts)
}

func createFrameworkTables(
	txn executor.TxnExecutor,
	final versions.Version) error {
	values := versions.FrameworkInitSQLs
	values = append(values, final.GetInitVersionSQL(versions.StateReady))

	for _, sql := range values {
		r, err := txn.Exec(sql, executor.StatementOption{})
		if err != nil {
			return err
		}
		r.Close()
	}
	return nil
}
