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

package catalog

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ValidateTableChangesSource validates the shared planner and runtime source contract.
// objectRef may be nil when subscription information is unavailable at runtime.
func ValidateTableChangesSource(objectRef *plan.ObjectRef, tableDef *plan.TableDef) error {
	if tableDef == nil {
		return moerr.NewInvalidInputNoCtx("table_changes source table does not exist")
	}
	if objectRef != nil && objectRef.PubInfo != nil {
		return moerr.NewNotSupportedNoCtx("table_changes does not support subscription tables")
	}
	if tableDef.IsTemporary {
		return moerr.NewNotSupportedNoCtx("table_changes does not support temporary tables")
	}
	if err := validateTableChangesColumnNames(tableDef); err != nil {
		return err
	}
	switch tableDef.TableType {
	case SystemOrdinaryRel:
	case SystemClusterRel:
		// Cluster tables have a physical account_id column. The executor applies
		// the same account filter as an ordinary SQL scan.
	default:
		return moerr.NewNotSupportedNoCtxf(
			"table_changes does not support table type %q",
			tableDef.TableType,
		)
	}
	if tableDef.Partition != nil {
		return moerr.NewNotSupportedNoCtx("table_changes does not support partitioned tables")
	}
	if tableDef.Pkey == nil ||
		len(tableDef.Pkey.Names) == 0 ||
		tableDef.Pkey.PkeyColName == FakePrimaryKeyColName {
		return moerr.NewNotSupportedNoCtx("table_changes requires an explicit primary key")
	}
	if tableDef.TableType == SystemClusterRel &&
		!containsChangeKey(tableDef.Pkey.Names, "account_id") {
		return moerr.NewNotSupportedNoCtx(
			"table_changes requires cluster table primary keys to include account_id",
		)
	}
	return nil
}

func validateTableChangesColumnNames(tableDef *plan.TableDef) error {
	for _, col := range tableDef.Cols {
		if col == nil {
			continue
		}
		if IsTableChangesMetadataColumn(col.Name) {
			return moerr.NewInvalidInputNoCtxf(
				"table_changes source column %q conflicts with reserved metadata column",
				col.Name,
			)
		}
	}
	return nil
}

func containsChangeKey(names []string, target string) bool {
	for _, name := range names {
		if strings.EqualFold(name, target) {
			return true
		}
	}
	return false
}
