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

package versions

import (
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// InformationSchemaViewsUpgradeEntry is shared by tenant upgrade and startup.
func InformationSchemaViewsUpgradeEntry() UpgradeEntry {
	return InformationSchemaViewUpgradeEntry("VIEWS", sysview.InformationSchemaViewsDDL, defines.MORPCVersion126)
}

// InformationSchemaTablesUpgradeEntry refreshes the current TABLES definition.
// Historical and forward handlers share the same idempotent view repair.
func InformationSchemaTablesUpgradeEntry() UpgradeEntry {
	return InformationSchemaViewUpgradeEntry("TABLES", sysview.InformationSchemaTablesCurrentDDL, defines.MORPCVersion118)
}

// InformationSchemaPartitionsUpgradeEntry refreshes the current PARTITIONS
// definition on the startup rail InitSystemViews already applies for TABLES.
// Tenants crossing versions keep using the v4_0_7 tenant upgrade entry; this
// entry makes a cluster already at the current version converge a persisted
// old PARTITIONS definition, with the same idempotent no-op definition check.
func InformationSchemaPartitionsUpgradeEntry() UpgradeEntry {
	return InformationSchemaViewUpgradeEntry("PARTITIONS", sysview.InformationSchemaPartitionsDDL, defines.MORPCVersion41)
}
