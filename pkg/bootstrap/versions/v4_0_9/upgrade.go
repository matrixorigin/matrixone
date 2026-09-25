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

package v4_0_9

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var Handler = &versionHandle{metadata: versions.Version{
	Version:                 "4.0.9",
	MinUpgradeVersion:       "4.0.8",
	UpgradeCluster:          versions.Yes,
	UpgradeTenant:           versions.No,
	VersionOffset:           uint32(len(clusterUpgEntries)),
	RequiredProtocolVersion: defines.MORPCVersion97,
}}

type versionHandle struct{ metadata versions.Version }

func (v *versionHandle) Metadata() versions.Version { return v.metadata }

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(context.Context, int32, executor.TxnExecutor) error {
	return nil
}

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, txn executor.TxnExecutor) error {
	for _, entry := range clusterUpgEntries {
		if err := entry.Upgrade(txn, catalog.System_Account); err != nil {
			return err
		}
	}
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtx("only v1.2.0 initializes the upgrade framework")
}
