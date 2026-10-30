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

package v4_0_18

import "github.com/matrixorigin/matrixone/pkg/bootstrap/versions"

// The sole tenant entry recreates information_schema.COLUMNS with the current
// (attributed-DECIMAL decorated) definition. Tenants crossing the consolidated
// 4.0.7 terminal on the new binary already receive it from the canonical
// entry shared by that terminal and the startup maintenance; this NEW forward
// handle exists because tenants READY at the frozen 4.0.7/41 terminal — the
// upgrade's exact predecessor state — cross no entry-carrying list beyond it,
// and the framework's version-equality short-circuit never re-fires an entry
// for a tenant whose create version equals the cluster final version. The
// canonical entry refreshes only a definition that diverges from the
// current template, so tenants already holding the decorated text are
// read-only content checks.
var tenantUpgEntries = []versions.UpgradeEntry{versions.InformationSchemaColumnsUpgradeEntry()}
