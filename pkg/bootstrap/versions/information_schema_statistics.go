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

package versions

import (
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// InformationSchemaStatisticsUpgradeEntry refreshes the current STATISTICS
// definition. Historical and forward handlers share the same idempotent view
// repair (#30910/#31164): the CARDINALITY projection publishes the engine's
// distinct-value estimates instead of a constant zero, riding the current
// definition that also reports the retained index prefix lengths (#30935).
// The historical template itself stays byte-stable as every pre-fix binary
// published it, so this entry must resolve the current text.
func InformationSchemaStatisticsUpgradeEntry() UpgradeEntry {
	return InformationSchemaViewUpgradeEntry("STATISTICS", sysview.InformationSchemaStatisticsPrefixSubPartDDL, defines.MORPCVersion107)
}
