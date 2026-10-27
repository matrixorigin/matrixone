// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package versions

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
	"time"
)

// RunTenantUpgradeEntries executes and logs the legacy per-tenant upgrade sequence.
func RunTenantUpgradeEntries(tenantID int32, txn executor.TxnExecutor, entries []UpgradeEntry, logger func(string) *log.MOLogger, version func() string) error {

	var (
		err  error
		goon bool
	)

	for _, upgEntry := range entries {
		goon = false

		start := time.Now()
		err = upgEntry.Upgrade(txn, uint32(tenantID))

		duration := time.Since(start)

		if err == nil {
			goon = true
			logger(txn.Txn().TxnOptions().CN).Info("tenant upgrade",
				zap.String("cn", txn.Txn().TxnOptions().CN),
				zap.String("entry", upgEntry.String()),
				zap.Int32("tenantId", tenantID),
				zap.String("version", version()),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
		} else {
			logger(txn.Txn().TxnOptions().CN).Error("tenant upgrade",
				zap.String("cn", txn.Txn().TxnOptions().CN),
				zap.String("entry", upgEntry.String()),
				zap.Int32("tenantId", tenantID),
				zap.String("version", version()),
				zap.Duration("duration", duration),
				zap.Error(err),
			)
		}

		if goon {
			continue
		}
		return err
	}

	return nil
}

// RunClusterUpgradeEntries executes and logs the legacy cluster upgrade sequence.
func RunClusterUpgradeEntries(txn executor.TxnExecutor, entries []UpgradeEntry, logger func(string) *log.MOLogger, version func() string) error {
	for _, upgEntry := range entries {
		start := time.Now()

		err := upgEntry.Upgrade(txn, catalog.System_Account)
		if err != nil {
			logger(txn.Txn().TxnOptions().CN).Error("cluster upgrade entry execute error", zap.Error(err), zap.String("version", version()), zap.String("upgrade entry", upgEntry.String()))
			return err
		}

		duration := time.Since(start)
		logger(txn.Txn().TxnOptions().CN).Info("cluster upgrade entry complete",
			zap.String("upgrade entry", upgEntry.String()),
			zap.Int64("time cost(ms)", duration.Milliseconds()),
			zap.String("toVersion", version()))
	}
	return nil
}

// RunTenantUpgradeEntriesWithLogger executes the later tenant upgrade sequence.
func RunTenantUpgradeEntriesWithLogger(tenantID int32, txn executor.TxnExecutor, entries []UpgradeEntry, logger *log.MOLogger, version func() string) error {
	for _, entry := range entries {
		start := time.Now()
		if err := entry.Upgrade(txn, uint32(tenantID)); err != nil {
			logger.Error("tenant upgrade entry execute error",
				zap.Error(err),
				zap.Int32("tenantId", tenantID),
				zap.String("version", version()),
				zap.String("upgrade entry", entry.String()))
			return err
		}
		logger.Info("tenant upgrade entry complete",
			zap.Int32("tenantId", tenantID),
			zap.String("upgrade entry", entry.String()),
			zap.Int64("time cost(ms)", time.Since(start).Milliseconds()),
			zap.String("toVersion", version()))
	}
	return nil
}

// RunHistoricalTenantUpgradeEntries executes the historical tenant upgrade sequence.
func RunHistoricalTenantUpgradeEntries(tenantID int32, txn executor.TxnExecutor, entries []UpgradeEntry, logger func(string) *log.MOLogger, version func() string) error {

	for _, upgEntry := range entries {
		start := time.Now()

		err := upgEntry.Upgrade(txn, uint32(tenantID))
		if err != nil {
			logger(txn.Txn().TxnOptions().CN).Error("tenant upgrade entry execute error", zap.Error(err), zap.Int32("tenantId", tenantID), zap.String("version", version()), zap.String("upgrade entry", upgEntry.String()))
			return err
		}

		duration := time.Since(start)
		logger(txn.Txn().TxnOptions().CN).Info("tenant upgrade entry complete",
			zap.String("upgrade entry", upgEntry.String()),
			zap.Int64("time cost(ms)", duration.Milliseconds()),
			zap.Int32("tenantId", tenantID),
			zap.String("toVersion", version()))
	}

	return nil
}
