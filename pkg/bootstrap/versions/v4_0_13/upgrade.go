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

package v4_0_13

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

// Use a new semantic version after the existing 4.0.11 CDC and 4.0.12
// CHARACTER_SETS migrations. An old worker must not treat this STATISTICS
// refresh as already handled merely because it knows one of those versions.
var Handler = &versionHandle{
	metadata: versions.Version{
		Version:                 "4.0.13",
		MinUpgradeVersion:       "4.0.12",
		UpgradeCluster:          versions.No,
		UpgradeTenant:           versions.Yes,
		VersionOffset:           uint32(len(tenantUpgEntries)),
		RequiredProtocolVersion: defines.MORPCVersion107,
	},
}

type versionHandle struct {
	metadata versions.Version
}

func (v *versionHandle) Metadata() versions.Version {
	return v.metadata
}

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(_ context.Context, tenantID int32, txn executor.TxnExecutor) error {
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

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, _ executor.TxnExecutor) error {
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(_ executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
