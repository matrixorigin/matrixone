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

package v4_0_8

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

// Use a newer semantic version, not a 4.0.7 offset: old tenant workers compare only
// ToVersion and would complete an offset-only task with their old, empty entry list.
// Requiring 4.0.7 as the starting version also keeps its provenance migration in the
// upgrade chain for clusters that are still at 4.0.6.
var Handler = &versionHandle{
	metadata: versions.Version{
		Version:                 "4.0.8",
		MinUpgradeVersion:       "4.0.7",
		UpgradeCluster:          versions.Yes,
		UpgradeTenant:           versions.Yes,
		VersionOffset:           uint32(len(tenantUpgEntries) + len(clusterUpgEntries)),
		RequiredProtocolVersion: defines.MORPCVersion41,
	},
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
	logger := runtime.ServiceRuntime(txn.Txn().TxnOptions().CN).Logger()
	for _, entry := range tenantUpgEntries {
		start := time.Now()
		if err := entry.Upgrade(txn, uint32(tenantID)); err != nil {
			logger.Error("tenant upgrade entry execute error",
				zap.Error(err),
				zap.Int32("tenantId", tenantID),
				zap.String("version", v.metadata.Version),
				zap.String("upgrade entry", entry.String()))
			return err
		}
		logger.Info("tenant upgrade entry complete",
			zap.Int32("tenantId", tenantID),
			zap.String("upgrade entry", entry.String()),
			zap.Int64("time cost(ms)", time.Since(start).Milliseconds()),
			zap.String("toVersion", v.metadata.Version))
	}
	return nil
}

func (v *versionHandle) HandleClusterUpgrade(ctx context.Context, txn executor.TxnExecutor) error {
	for _, entry := range clusterUpgEntries {
		if err := entry.Upgrade(txn, catalog.System_Account); err != nil {
			return err
		}
	}
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(txn executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
