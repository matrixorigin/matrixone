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

package v4_0_7

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// The index metadata provenance migration is NOT an entry: the set of tables it alters is
// only known at runtime (one metadata table per index, per account), which a fixed UpgSql
// cannot express. It runs from HandleTenantUpgrade, the same escape hatch v4_0_6 uses for
// upgradeLegacyForeignKeyMetadata.
// Keep the released v4.0.7 offset at zero. New catalog repairs belong to
// maintenance, not to an already-persisted historical upgrade identity.
var tenantUpgEntries = []versions.UpgradeEntry{}

// UpgradeInformationSchemaViewsAfterProtocolCheck replaces VIEWS for the
// bounded maintenance owner, independently of the historical upgrade list.
// The caller checks the protocol immediately before calling this function so
// an ErrNotSupported from the DDL cannot be mistaken for a retryable protocol gate.
func UpgradeInformationSchemaViewsAfterProtocolCheck(
	txn executor.TxnExecutor,
	accountID uint32,
) error {
	entry := upgradeInformationSchemaViews()
	return entry.UpgradeAfterProtocolCheck(txn, accountID)
}

// CheckInformationSchemaViewsDefinition recognizes the two catalog names
// emitted by the system DDL: VIEWS with case preservation, and views with the
// default lower_case_table_names setting. Catalog string predicates are case
// sensitive; missing the folded name would make every maintenance pass repeat
// DROP/CREATE even after the current definition was successfully installed.
func CheckInformationSchemaViewsDefinition(
	txn executor.TxnExecutor,
	accountID uint32,
) (bool, string, error) {
	exists, definition, err := versions.CheckViewDefinition(
		txn, accountID, sysview.InformationDBConst, "VIEWS")
	if err != nil || exists {
		return exists, definition, err
	}
	return versions.CheckViewDefinition(txn, accountID, sysview.InformationDBConst, "views")
}

// InformationSchemaViewsAuthoringProtocolReady reports the local write-side
// admission fence. The common protocol check only proves that every CN can
// decode v125; the VIEWS DDL also persists function IDs that must not be
// authored until this CN has consumed the catalog-fenced admission snapshot.
// A missing runtime or key is retained as ready for bootstrap unit tests and
// older standalone callers that predate the admission lifecycle.
func InformationSchemaViewsAuthoringProtocolReady(txn executor.TxnExecutor) bool {
	if txn == nil || txn.Txn() == nil {
		return true
	}
	rt := runtime.ServiceRuntime(txn.Txn().TxnOptions().CN)
	if rt == nil {
		return true
	}
	value, present := rt.GetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor)
	if !present {
		return true
	}
	floor, valid := value.(int64)
	return valid && floor >= defines.MORPCVersion125
}

// upgradeInformationSchemaViews describes the guarded maintenance repair. The
// account scan uses the persisted DDL as its work marker, so it also reaches
// tenants already at v4.0.6, v4.0.7, or the final version without changing any
// historical handler's offset or delaying its pending provenance migration.
func upgradeInformationSchemaViews() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "VIEWS",
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaViewsDDL,
		RequiredProtocolVersion: defines.MORPCVersion125,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, viewDef, err := CheckInformationSchemaViewsDefinition(txn, accountID)
			if err != nil {
				return false, err
			}
			return exists && viewDef == sysview.InformationSchemaViewsDDL, nil
		},
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;",
			sysview.InformationDBConst, "VIEWS"),
	}
}
