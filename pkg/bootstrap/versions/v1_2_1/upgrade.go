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

package v1_2_1

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var (
	Handler = &versionHandle{
		metadata: versions.Version{
			Version:           "1.2.1",
			MinUpgradeVersion: "1.2.0",
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
	txn executor.TxnExecutor) error {

	for _, upgEntry := range tenantUpgEntries {
		err := upgEntry.Upgrade(txn, uint32(tenantID))
		if err != nil {
			return err
		}
	}

	return nil
}

func (v *versionHandle) HandleClusterUpgrade(
	ctx context.Context,
	txn executor.TxnExecutor) error {
	for i, upgEntry := range clusterUpgEntries {
		msg := upgEntry.String()
		fmt.Printf("--------wuxiliang------------the %d th %s", i, msg)
		err := upgEntry.Upgrade(txn, catalog.System_Account)
		if err != nil {
			return err
		}
	}

	return nil
}

func (v *versionHandle) createFrameworkTables(
	txn executor.TxnExecutor,
	final bool) error {
	return moerr.NewInternalErrorNoCtx("Only v1.2.0 can initialize upgrade framework, current version is:%s", Handler.metadata.Version)
}
