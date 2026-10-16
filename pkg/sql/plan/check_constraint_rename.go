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

package plan

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// RenameGeneratedCheckConstraintNames renames every generated CHECK constraint
// name of the form <oldTable>_chk_<n> to <newTable>_chk_<n>, keeping the
// ordinal digits. Explicitly named CHECKs are returned unchanged.
//
// Unnamed CHECKs receive their name from the table at CREATE TABLE time and
// must stay unique per schema. When the table is renamed, the names have to
// follow it; otherwise the renamed table keeps owning <oldTable>_chk_<n> and
// recreating a table under the old name trips ER_CHECK_CONSTRAINT_DUP_NAME.
//
// The returned slice is a copy; the input slice and its entries are never
// modified. When no entry matches, the original slice is returned as is.
func RenameGeneratedCheckConstraintNames(checks []*plan.CheckDef, oldTable, newTable string) []*plan.CheckDef {
	if len(checks) == 0 {
		return checks
	}
	oldPrefix := strings.ToLower(oldTable) + "_chk_"
	var renamed []*plan.CheckDef
	for i, check := range checks {
		if check == nil {
			continue
		}
		name := check.GetName()
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, oldPrefix) {
			continue
		}
		digits := lower[len(oldPrefix):]
		ordinal, err := strconv.Atoi(digits)
		if err != nil || ordinal < 1 || strconv.Itoa(ordinal) != digits {
			continue
		}
		if renamed == nil {
			renamed = make([]*plan.CheckDef, len(checks))
			copy(renamed, checks)
		}
		cloned := *check
		cloned.Name = newTable + "_chk_" + digits
		renamed[i] = &cloned
	}
	if renamed == nil {
		return checks
	}
	return renamed
}
