// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var Handler *versionHandle

func init() {
	Handler = &versionHandle{metadata: versions.Version{
		Version:           "4.0.6",
		MinUpgradeVersion: "4.0.5",
		UpgradeCluster:    versions.Yes,
		UpgradeTenant:     versions.Yes,
		VersionOffset:     uint32(len(tenantUpgEntries) + len(clusterUpgEntries)),
	}}
}

type versionHandle struct{ metadata versions.Version }

func (v *versionHandle) Metadata() versions.Version { return v.metadata }

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(_ context.Context, tenantID int32, txn executor.TxnExecutor) error {
	for _, entry := range tenantUpgEntries {
		start := time.Now()
		if err := entry.Upgrade(txn, uint32(tenantID)); err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("tenant upgrade entry execute error", zap.Error(err), zap.Int32("tenantId", tenantID), zap.String("version", v.metadata.Version), zap.String("upgrade entry", entry.String()))
			return err
		}
		getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade entry complete", zap.String("upgrade entry", entry.String()), zap.Int64("time cost(ms)", time.Since(start).Milliseconds()), zap.String("toVersion", v.metadata.Version))
	}
	return nil
}

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, txn executor.TxnExecutor) error {
	for _, entry := range clusterUpgEntries {
		if err := entry.Upgrade(txn, uint32(txn.Txn().TxnOptions().AccountID)); err != nil {
			return err
		}
	}
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
