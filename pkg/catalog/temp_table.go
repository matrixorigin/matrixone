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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// MarkTableDefTemporary keeps the in-memory table type and the durable
// mo_tables.relkind property in sync. The catalog marker, rather than the
// generated physical name, is the canonical temporary-object classifier.
func MarkTableDefTemporary(tableDef *plan.TableDef) {
	if tableDef == nil {
		return
	}

	tableDef.TableType = SystemTemporaryTable
	tableDef.IsTemporary = true

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
