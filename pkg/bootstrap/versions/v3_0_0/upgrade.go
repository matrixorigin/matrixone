// Copyright 2025 Matrix Origin
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

package v3_0_0

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	Handler = &versionHandle{
		metadata: versions.Version{
			Version:           "3.0.0",
			MinUpgradeVersion: "2.2.0",
			UpgradeCluster:    versions.Yes,
			UpgradeTenant:     versions.Yes,
			VersionOffset:     uint32(len(clusterUpgEntries) + len(tenantUpgEntries)),
		},
	}
)

type versionHandle struct {
	metadata versions.Version
}

func (v *versionHandle) Metadata() versions.Version {
	return v.metadata
}

func (v *versionHandle) Prepare(
	ctx context.Context,
	txn executor.TxnExecutor,
	final bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(
	ctx context.Context,
	tenantID int32,
	txn executor.TxnExecutor,
) error {

	var (
		err    error
		goon   bool
		logger func(msg string, fields ...zap.Field)
	)

	for _, upgEntry := range tenantUpgEntries {
		goon = false
		logger = logutil.Error

		start := time.Now()
		err = upgEntry.Upgrade(txn, uint32(tenantID))

		duration := time.Since(start)

		if err == nil {
			logger = logutil.Info
			goon = true
		}

		logger("tenant upgrade",
			zap.String("cn", txn.Txn().TxnOptions().CN),
			zap.String("entry", upgEntry.String()),
			zap.Int32("tenantId", tenantID),
			zap.String("version", v.metadata.Version),
			zap.Duration("duration", duration),
			zap.Error(err),
		)

		if goon {
			continue
		}
		return err
	}

	return nil
}

func (v *versionHandle) HandleClusterUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor,
) error {
	for _, upgEntry := range clusterUpgEntries {
		start := time.Now()

		err := upgEntry.Upgrade(txn, catalog.System_Account)
		if err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("cluster upgrade entry execute error", zap.Error(err), zap.String("version", v.Metadata().Version), zap.String("upgrade entry", upgEntry.String()))
			return err
		}

		duration := time.Since(start)
		getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade entry complete",
			zap.String("upgrade entry", upgEntry.String()),
			zap.Int64("time cost(ms)", duration.Milliseconds()),
			zap.String("toVersion", v.Metadata().Version))
	}
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(txn executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", Handler.metadata.Version)
}
