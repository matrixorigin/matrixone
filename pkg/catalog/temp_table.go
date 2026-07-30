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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	legacyTemporaryTableNameRegexp = `^__mo_tmp_[0-9a-f]{32}_`
	legacyTemporaryTableFunction   = `mo_is_legacy_temporary_table`
	userVisibleRelationKindsSQL    = "'" + SystemOrdinaryRel + "', '" + SystemViewRel + "', '" + SystemExternalRel + "', '" + SystemMaterializedRel + "', '" + SystemSourceRel + "', '" + SystemClusterRel + "', '" + SystemPartitionRel + "', '" + SystemSequenceRel + "'"
)

// NonTemporaryTableSQLPredicate returns the catalog predicate used to exclude
// temporary objects from metadata and clone/restore queries.
//
// New temporary objects have SystemTemporaryTable in mo_tables.relkind. During
// an asynchronous upgrade, however, sessions started on an older version can
// still have base rows whose relkind is SystemOrdinaryRel. The compatibility
// function parses their request-wide rel_createsql and checks durable rename
// metadata before associating the creating statement with the logical alias
// encoded in the physical table name. Old derived objects added later do not
// reliably retain that statement, so they are recognized only when their
// relkind is not user-visible and their name has the exact physical session-ID
// shape. The relkind guard keeps permanent tables, views, and external objects
// with otherwise legal __mo_tmp_ names visible.
func NonTemporaryTableSQLPredicate(alias string) string {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	relKind := prefix + SystemRelAttr_Kind
	relName := prefix + SystemRelAttr_Name
	relDatabase := prefix + SystemRelAttr_DBName
	createSQL := prefix + SystemRelAttr_CreateSQL
	extraInfo := prefix + SystemRelAttr_ExtraInfo

	return fmt.Sprintf(
		"not (%s = '%s' or %s(coalesce(%s, ''), coalesce(%s, ''), coalesce(%s, ''), coalesce(%s, ''), coalesce(%s, '')) or (coalesce(%s, '') not in (%s) and regexp_like(%s, '%s')))",
		relKind,
		SystemTemporaryTable,
		legacyTemporaryTableFunction,
		relKind,
		relName,
		relDatabase,
		createSQL,
		extraInfo,
		relKind,
		userVisibleRelationKindsSQL,
		relName,
		legacyTemporaryTableNameRegexp,
	)
}

// MarkTableDefTemporary keeps the in-memory table type and the durable
// mo_tables.relkind property in sync. The catalog marker, rather than the
// generated physical name, is the canonical temporary-object classifier.
// IsTemporary is deliberately left untouched: it is session state populated
// when a logical temporary-table alias is resolved, not durable table metadata.
func MarkTableDefTemporary(tableDef *plan.TableDef) {
	if tableDef == nil {
		return
	}

	tableDef.TableType = SystemTemporaryTable

	var firstProperties *plan.PropertiesDef
	foundKind := false
	for _, def := range tableDef.Defs {
		properties, ok := def.GetDef().(*plan.TableDef_DefType_Properties)
		if !ok {
			continue
		}
		if properties.Properties == nil {
			properties.Properties = &plan.PropertiesDef{}
		}
		if firstProperties == nil {
			firstProperties = properties.Properties
		}
		filtered := properties.Properties.Properties[:0]
		for _, property := range properties.Properties.Properties {
			if property == nil {
				filtered = append(filtered, nil)
				continue
			}
			if strings.EqualFold(property.Key, SystemRelAttr_Kind) {
				if !foundKind {
					property.Key = SystemRelAttr_Kind
					property.Value = SystemTemporaryTable
					filtered = append(filtered, property)
					foundKind = true
				}
				continue
			}
			filtered = append(filtered, property)
		}
		properties.Properties.Properties = filtered
	}
	if foundKind {
		return
	}

	if firstProperties == nil {
		firstProperties = &plan.PropertiesDef{}
		tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{Properties: firstProperties},
		})
	}
	firstProperties.Properties = append(firstProperties.Properties, &plan.Property{
		Key:   SystemRelAttr_Kind,
		Value: SystemTemporaryTable,
	})
}
