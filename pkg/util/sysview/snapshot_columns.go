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

package sysview

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

// SnapshotColumnsDDL selects the namespace of a resolved cross-account snapshot
// without changing the caller's principal. Only known system templates can be
// adapted. The planner must require a system caller and an account-level named
// snapshot; the runtime guard additionally requires the caller's moadmin role.
// Never compare caller role IDs with owners or grants in the target account.
// This is a read-time adapter, not a persisted catalog migration.
func SnapshotColumnsDDL(definition string, accountID uint32, derived bool) (string, bool) {
	adapted, known := ViewMetadataColumnsDDL(definition)
	if !known {
		return "", false
	}
	if derived {
		definition = adapted
	}
	namespace := strconv.FormatUint(uint64(accountID), 10)
	// These calls belong to the recognized COLUMNS template's namespace
	// predicates, not to user SQL. Install the caller guard only afterwards.
	definition = strings.ReplaceAll(definition, "current_account_id()", namespace)
	oldCTE := strings.ReplaceAll(informationSchemaMetadataVisibilityCTE(), "current_account_id()", namespace)
	tableFilter := ""
	if strings.Count(definition, oldCTE) != 1 {
		// Current COLUMNS filters physical index children before APPLY. Match
		// that authored CTE separately from frozen historical templates, and
		// retain its listing policy when replacing only role-based admission.
		tableFilter = " AND " + catalog.NonIndexRelationSQLPredicate("tbl")
		oldCTE = strings.ReplaceAll(informationSchemaMetadataVisibilityCTEWithTableFilter(
			"SELECT role_id FROM mo_current_roles() role_closure", tableFilter), "current_account_id()", namespace)
		if strings.Count(definition, oldCTE) != 1 {
			return "", false
		}
	}
	const callerGuard = "(current_account_id() = 0 AND current_role_id() = 0)"
	cte := "WITH __mo_visible_tables AS (" +
		"SELECT tbl.account_id, tbl.rel_id, tbl.relname, tbl.reldatabase, tbl.reldatabase_id, tbl.relkind, " +
		"tbl.rel_createsql, tbl.created_time, tbl.partitioned, tbl.rel_comment, tbl.extra_info, tbl.rel_logical_id, " +
		"tbl.owner, tbl.`constraint` FROM mo_catalog.mo_tables tbl " +
		"WHERE tbl.account_id = " + namespace + " AND " + callerGuard + tableFilter + ") "
	definition = strings.Replace(definition, oldCTE, cte, 1)
	definition = strings.NewReplacer(
		informationSchemaSubscriptionColumnAuthorizationPredicate(), callerGuard,
		informationSchemaSubscriptionViewAuthorizationPredicate(), callerGuard,
	).Replace(definition)
	return definition, true
}
