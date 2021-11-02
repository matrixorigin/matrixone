// Copyright 2021 Matrix Origin
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

package ftree

import "matrixone/pkg/sql/plan"

func buildRelation(rel *plan.Relation) *Relation {
	var vars []string

	varsMap := make(map[string]*Variable)
	vars = append(vars, rel.Attrs...)
	for _, v := range vars {
		attr := rel.AttrsMap[v]
		varsMap[v] = &Variable{
			Ref:  attr.Ref,
			Name: attr.Name,
			Type: attr.Type.Oid,
		}
	}
	for _, e := range rel.ProjectionExtends {
		vars = append(vars, e.Alias)
		varsMap[e.Alias] = &Variable{
			Ref:  e.Ref,
			Name: e.Alias,
			Type: e.E.ReturnType(),
		}
	}
	return &Relation{
		Rel:     rel,
		Vars:    vars,
		VarsMap: varsMap,
	}
}
