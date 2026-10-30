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
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// The hop carries the two idempotent current-definition refreshes on the
// shared builder: the COLUMNS leg keeps its historical 109 gate, the
// STATISTICS leg the historical 41 gate the earlier statistics refreshes
// use. Both publish the corrected identities to pre-existing accounts once;
// the historical hops keep publishing their own base templates.
var tenantUpgEntries = []versions.UpgradeEntry{
	versions.InformationSchemaColumnsUpgradeEntry(),
	versions.InformationSchemaViewUpgradeEntry(
		"STATISTICS",
		sysview.InformationSchemaStatisticsPrefixSubPartDDL,
		defines.MORPCVersion41),
}
