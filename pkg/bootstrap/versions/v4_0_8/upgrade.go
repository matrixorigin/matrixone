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

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var clusterUpgEntries = []versions.UpgradeEntry{
	addSQLTaskRunClaim,
	addSQLTaskScheduleGeneration,
	addSQLTaskRunningIndex,
}

var Handler = &versionHandle{metadata: versions.Version{
	Version:                 "4.0.8",
	MinUpgradeVersion:       "4.0.7",
	UpgradeCluster:          versions.Yes,
	UpgradeTenant:           versions.No,
	VersionOffset:           uint32(len(clusterUpgEntries)),
	RequiredProtocolVersion: defines.MORPCVersion135,
}}

type versionHandle struct{ metadata versions.Version }

func (v *versionHandle) Metadata() versions.Version { return v.metadata }

func (*versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (*versionHandle) HandleClusterUpgrade(_ context.Context, txn executor.TxnExecutor) error {
	// The generic framework checks RequiredProtocolVersion only after cluster
	// DDL. This protocol protects the ownership migration itself.
	if err := versions.CheckCommonProtocolVersion(txn, defines.MORPCVersion135); err != nil {
		return err
	}
	for i := range clusterUpgEntries {
		if err := clusterUpgEntries[i].Upgrade(txn, catalog.System_Account); err != nil {
			return err
		}
	}
	return nil
}

// On-demand upgrades may traverse this version even though its changes are
// cluster-only.
func (*versionHandle) HandleTenantUpgrade(context.Context, int32, executor.TxnExecutor) error {
	return nil
}

func (*versionHandle) HandleCreateFrameworkDeps(executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtx("only v1.2.0 can initialize the upgrade framework")
}
