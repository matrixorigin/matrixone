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

package v4_0_19

import (
	"context"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// Handler refreshes the declared-label COLUMNS identities and the
// prefix-aware STATISTICS projection for accounts that already reached the
// frozen 4.0.7 stage. "4.0.19" is the next semantic catalog version after
// upstream "4.0.18" (docs/design/20261028-catalog-upgrade-407.md forbids
// registering the unpublished 4.0.8..4.0.17 identities as upgrade steps):
// below-4.0.7 sources cross the frozen 4.0.7 stage first and then step
// 4.0.18 and 4.0.19, a ready 4.0.7 cluster's background route is the
// single direct [4.0.7 -> 4.0.19] upgrade executed by this handler alone
// (final.CanDirectUpgrade admits it), tenants a main-era binary stamped
// 4.0.18 take the direct [4.0.18 -> 4.0.19] upgrade, and fresh clusters
// initialize at 4.0.19 with the corrected identities already installed.
var Handler = &versionHandle{
	metadata: versions.Version{
		Version:           "4.0.19",
		MinUpgradeVersion: "4.0.7",
		UpgradeCluster:    versions.No,
		UpgradeTenant:     versions.Yes,
		VersionOffset:     uint32(len(tenantUpgEntries)),
		// The identity publication writes require the consolidated 4.0.7
		// protocol floor; per-entry gates stay with the shared entries.
		RequiredProtocolVersion: defines.MORPCVersion118,
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
	return versions.RunTenantUpgradeEntriesWithLogger(tenantID, txn, tenantUpgEntries, logger, func() string { return v.metadata.Version })
}

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, _ executor.TxnExecutor) error {
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(_ executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
