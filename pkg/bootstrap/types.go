// Copyright 2023 Matrix Origin
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

package bootstrap

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// Service is used to bootstrap and upgrade mo cluster.
//
// In bootstrap, it will create some internal databases and tables at the time of MO
// initialization according to a specific logic. It provides the necessary dependencies
// for other components to be launched later.
//
// In upgrade, it will upgrade the metadata between two MO versions. When there is a
// need to modify the original data (e.g. create a system table, modify the system table
// structure, etc.) between two MO versions, then an upgrade process is required. This
// process is used to ensure that each tenant's data is properly upgraded and updated.
//
// Note that, this service is not used to bootstrap logservice and dn. The internal
// databases and tables as below:
// 1. mo_indexes in mo_catalog
// 2. task infrastructure database
type Service interface {
	// Bootstrap try to bootstrap and upgrade mo cluster
	Bootstrap(ctx context.Context) error

	// AddTenantUpgrade upgrade logic handle func for tenant with special version.
	AddTenantUpgrade(Version, TenantUpgradeHandleFunc)
	// AddClusterUpgrade upgrade logic handle func for cluster with special version.
	AddClusterUpgrade(Version, ClusterUpgradeHandleFunc)
}

// Locker locker is used to get lock to bootstrap. Only one cn can get lock to bootstrap.
// Other cns need to wait bootstrap completed.
type Locker interface {
	// Get return true means get lock
	Get(ctx context.Context) (bool, error)
}

// Version version
type Version struct {
	// ID every mo version has a id.
	ID int
	// Version version string, like 1.0.0
	version string
	// prevVersion previous version string, like 0.9.0
	prevVersion string
	// upgrade need upgrade or not. 1: need.
	upgrade int
	// upgradeTenant upgrade tenant or not. 1: need.
	upgradeTenant int
	// UpgradeMetadata upgrade metadata
	upgradeMetadata string
	// upgradeStatus upgrade statue. 0. ready to upgrade. 1: upgrading. 2: upgraded.
	upgradeStatus int
}

type TenantUpgradeHandleFunc func(
	ctx context.Context,
	version Version,
	tenantID uint64,
	txn executor.TxnExecutor) error

type ClusterUpgradeHandleFunc func(
	ctx context.Context,
	version Version,
	txn executor.TxnExecutor) error
