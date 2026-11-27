// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sysview

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// MetadataVisibilityCTE builds the local SHOW visibility policy for database.
func MetadataVisibilityCTE(protocolVersion int64, database string) string {
	active := "SELECT role_id FROM mo_current_roles() role_closure"
	if protocolVersion < defines.MORPCVersion12 {
		active = "SELECT current_role_id() UNION SELECT rg.granted_id FROM mo_catalog.mo_role_grant rg WHERE rg.grantee_id = current_role_id()"
	}
	return fmt.Sprintf(`WITH __mo_active_roles(role_id) AS (%s),
 __mo_role_privs AS (
 SELECT rp.obj_type, rp.obj_id, rp.privilege_name, rp.privilege_level
 FROM mo_catalog.mo_role_privs rp JOIN __mo_active_roles a ON rp.role_id = a.role_id
 ),
 __mo_owned_databases AS (
 SELECT DISTINCT db.dat_id FROM mo_catalog.mo_database db JOIN __mo_active_roles a ON db.owner = a.role_id
 WHERE db.account_id = current_account_id() AND db.datname = %s
 ),
 __mo_global_visibility AS (SELECT COUNT(*) AS n FROM __mo_role_privs rp WHERE %s),
 __mo_database_visibility AS (SELECT DISTINCT rp.obj_id FROM __mo_role_privs rp WHERE %s),
 __mo_object_visibility AS (SELECT DISTINCT rp.obj_id FROM __mo_role_privs rp WHERE %s),
 __mo_visible_tables AS (
 SELECT tbl.account_id, tbl.rel_id, tbl.relname, tbl.reldatabase, tbl.reldatabase_id, tbl.relkind,
 tbl.rel_createsql, tbl.created_time, tbl.partitioned, tbl.rel_comment, tbl.extra_info, tbl.rel_logical_id,
 tbl.owner, tbl.`+"`constraint`"+` FROM mo_catalog.mo_tables tbl
 LEFT JOIN __mo_active_roles a ON tbl.owner = a.role_id
 LEFT JOIN __mo_owned_databases o ON tbl.reldatabase_id = o.dat_id
 LEFT JOIN __mo_global_visibility g ON TRUE
 LEFT JOIN __mo_database_visibility d ON tbl.reldatabase_id = d.obj_id
 LEFT JOIN __mo_object_visibility e ON tbl.rel_logical_id = e.obj_id
 WHERE tbl.account_id = current_account_id() AND tbl.reldatabase = %s
 AND (tbl.reldatabase IN ('mo_catalog','information_schema','mysql','system','system_metrics','mo_task','mo_debug')
 OR a.role_id IS NOT NULL OR o.dat_id IS NOT NULL OR g.n > 0 OR d.obj_id IS NOT NULL OR e.obj_id IS NOT NULL)
 ) `, active, sqlquote.String(database), globalVisibilityPredicate, databaseVisibilityPredicate, objectVisibilityPredicate, sqlquote.String(database))
}

const globalVisibilityPredicate = `((rp.obj_type IN ('table','view') AND rp.privilege_level = '*.*' AND rp.obj_id = 0)
 OR (rp.obj_type = 'database' AND rp.privilege_name IN ('show tables','database all','database ownership') AND rp.privilege_level IN ('*','*.*') AND rp.obj_id = 0))`
const databaseVisibilityPredicate = `((rp.obj_type IN ('table','view') AND rp.privilege_level IN ('d.*','*'))
 OR (rp.obj_type = 'database' AND rp.privilege_name IN ('show tables','database all','database ownership') AND rp.privilege_level = 'd'))`
const objectVisibilityPredicate = `rp.obj_type IN ('table','view') AND rp.privilege_level IN ('d.t','t')`

func SubscriptionMetadataVisibilitySQL(subscriptionNames string, protocolVersion int64) string {
	activeRolesSQL := "SELECT role_id FROM mo_current_roles() role_closure"
	if protocolVersion < defines.MORPCVersion12 {
		// Compatibility mode has only the primary role and direct grants.
		// The full closure requires protocol 12.
		activeRolesSQL = "SELECT current_role_id() UNION " +
			"SELECT rg.granted_id FROM mo_catalog.mo_role_grant rg " +
			"WHERE rg.grantee_id = current_role_id()"
	}
	return fmt.Sprintf(`WITH __subscription_active_roles(role_id) AS (%s),
 __subscription_role_privs AS (
 SELECT rp.obj_type, rp.obj_id, rp.privilege_name, rp.privilege_level
 FROM mo_catalog.mo_role_privs rp JOIN __subscription_active_roles a ON rp.role_id = a.role_id
 ),
 __subscription_global_visibility AS (SELECT COUNT(*) AS n FROM __subscription_role_privs rp WHERE %s),
 __subscription_database_visibility AS (SELECT DISTINCT rp.obj_id FROM __subscription_role_privs rp WHERE %s),
 __subscription_broad_visibility AS (
 SELECT db.datname
 FROM mo_catalog.mo_database db
 LEFT JOIN __subscription_active_roles a ON db.owner = a.role_id
 LEFT JOIN __subscription_global_visibility g ON TRUE
 LEFT JOIN __subscription_database_visibility d ON db.dat_id = d.obj_id
 WHERE db.account_id = current_account_id() AND db.datname IN (%s)
 AND (a.role_id IS NOT NULL OR g.n > 0 OR d.obj_id IS NOT NULL))
 SELECT datname, 1 AS all_tables, 0 AS table_id FROM __subscription_broad_visibility`,
		activeRolesSQL, globalVisibilityPredicate, databaseVisibilityPredicate, subscriptionNames)
}
