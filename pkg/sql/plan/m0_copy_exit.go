// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func setM0CopyExitComment(table *TableDef, comment string) {
	for _, def := range table.Defs {
		properties := def.GetProperties()
		if properties == nil {
			continue
		}
		for _, property := range properties.Properties {
			if property.Key == catalog.SystemRelAttr_Comment {
				property.Value = comment
				return
			}
		}
	}
	table.Defs = append(table.Defs, &plan.TableDef_DefType{Def: &plan.TableDef_DefType_Properties{
		Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: catalog.SystemRelAttr_Comment, Value: comment}}},
	}})
}
