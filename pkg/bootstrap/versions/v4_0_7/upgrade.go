// Copyright 2026 Matrix Origin
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

package v4_0_7

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// 4.0.7 exists so the index metadata provenance migration reaches clusters ALREADY at
// 4.0.6. The upgrade chain queues only handlers strictly newer than a cluster's persisted
// version (service_upgrade.go: Compare(v.Version, from) > 0), and the checker short-circuits
// when version and offset both match, so appending the migration to the 4.0.6 handler left
// every 4.0.6 cluster skipping it. VersionOffset cannot substitute: it is derived from the
// entry count and this migration is not an entry.
var (
	Handler *versionHandle
)

func init() {
	Handler = &versionHandle{
		metadata: versions.Version{
			Version:           "4.0.7",
			MinUpgradeVersion: "4.0.6",
			UpgradeCluster:    versions.Yes,
			UpgradeTenant:     versions.Yes,
			VersionOffset:     uint32(len(tenantUpgEntries) + len(clusterUpgEntries)),
		},
	}
}

type versionHandle struct {
	metadata versions.Version
}

func (v *versionHandle) Metadata() versions.Version {
	return v.metadata
}

func (v *versionHandle) Prepare(ctx context.Context, txn executor.TxnExecutor, final bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	for _, upgEntry := range tenantUpgEntries {
		start := time.Now()
		if err := upgEntry.Upgrade(txn, uint32(tenantID)); err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("tenant upgrade entry execute error",
				zap.Error(err),
				zap.Int32("tenantId", tenantID),
				zap.String("version", v.Metadata().Version),
				zap.String("upgrade entry", upgEntry.String()))
			return err
		}
		getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade entry complete",
			zap.String("upgrade entry", upgEntry.String()),
			zap.Int64("time cost(ms)", time.Since(start).Milliseconds()),
			zap.String("toVersion", v.Metadata().Version))
	}
	// Work a fixed UpgSql cannot express: the metadata tables to widen are only known at
	// runtime. Same escape hatch v4_0_6 uses for upgradeLegacyForeignKeyMetadata.
	if err := upgradeIndexMetadataProvenance(ctx, tenantID, txn); err != nil {
		return err
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade success",
		zap.Int32("tenantId", tenantID),
		zap.String("toVersion", v.Metadata().Version))
	return nil
}

func (v *versionHandle) HandleClusterUpgrade(ctx context.Context, txn executor.TxnExecutor) error {
	for _, upgEntry := range clusterUpgEntries {
		start := time.Now()
		if err := upgEntry.Upgrade(txn, uint32(txn.Txn().TxnOptions().AccountID)); err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("cluster upgrade entry execute error",
				zap.Error(err),
				zap.String("version", v.Metadata().Version),
				zap.String("upgrade entry", upgEntry.String()))
			return err
		}
		getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade entry complete",
			zap.String("upgrade entry", upgEntry.String()),
			zap.Int64("time cost(ms)", time.Since(start).Milliseconds()),
			zap.String("toVersion", v.Metadata().Version))
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade success",
		zap.String("toVersion", v.Metadata().Version))
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(txn executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", Handler.metadata.Version)
}
