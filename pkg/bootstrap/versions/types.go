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

package versions

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
)

var (
	FrameworkInitSQLs = []string{
		fmt.Sprintf(`create table %s.%s (
			version             varchar(50) not null primary key,
			state               int,
			create_at           timestamp not null,
			update_at           timestamp not null
		)`, catalog.MO_CATALOG, catalog.MOVersionTable),

		fmt.Sprintf(`create table %s.%s (
			id                  bigint unsigned not null primary key auto_increment,
			from_version        varchar(50) not null,
			to_version          varchar(50) not null,
			final_version       varchar(50) not null,
			state               int,
			upgrade_cluster     int,
			upgrade_tenant      int,
			upgrade_order       int,
			total_tenant        int,
			ready_tenant        int,
			create_at           timestamp not null,
			update_at           timestamp not null
		)`, catalog.MO_CATALOG, catalog.MOUpgradeTable),

		fmt.Sprintf(`create table %s.%s (
			id                  bigint unsigned not null primary key auto_increment,
			upgrade_id		    bigint unsigned not null,
			target_version      varchar(50) not null,
			from_account_id     int not null,
			to_account_id       int not null,
			ready               int,
			create_at           timestamp not null,
			update_at           timestamp not null
		)`, catalog.MO_CATALOG, catalog.MOUpgradeTenantTable),

		//`alter table mo_account add column create_version varchar(50) default '1.2.0' after suspended_time`,
	}
)

var (
	No  = int32(0)
	Yes = int32(1)
)

var (
	StateCreated         = int32(0)
	StateUpgradingTenant = int32(1)
	StateReady           = int32(2)
)

type Version struct {
	// Version version string, like 1.0.0
	Version string
	// State.
	State int32
	// MinUpgradeVersion the min version that can be directly upgraded to current version
	MinUpgradeVersion string
	// UpgradeCluster upgrade cluster or not.
	UpgradeCluster int32
	// UpgradeTenant tenant need upgrade. The upgrade framework is responsible for upgrading
	// all tenants in parallel.
	UpgradeTenant int32
}

type VersionUpgrade struct {
	// ID upgrade id
	ID uint64
	// FromVersion from version
	FromVersion string
	// ToVersion to version
	ToVersion string
	// FinalVersion upgrade final version
	FinalVersion string
	// State.
	State int32
	// UpgradeOrder upgrade order
	UpgradeOrder int32
	// UpgradeCluster upgrade cluster or not.
	UpgradeCluster int32
	// UpgradeTenant tenant need upgrade. The upgrade framework is responsible for upgrading
	// all tenants in parallel.
	UpgradeTenant int32
	// TotalTenant total tenant need upgrade
	TotalTenant int32
	// ReadyTenant ready tenant count
	ReadyTenant int32
}

func (v VersionUpgrade) String() string {
	return fmt.Sprintf("%dth: %s -> %s (%v, %v, %d, %d), state %d",
		v.UpgradeOrder,
		v.FromVersion,
		v.ToVersion,
		v.UpgradeCluster == Yes,
		v.UpgradeTenant == Yes,
		v.TotalTenant,
		v.ReadyTenant,
		v.State)
}
