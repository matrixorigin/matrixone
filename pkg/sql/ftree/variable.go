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

import (
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func reorder(rel *Relation, vars []string) {
	for i, v := range vars {
		for j, w := range rel.Vars {
			if v == w {
				rel.Vars[i], rel.Vars[j] = rel.Vars[j], rel.Vars[i]
			}
		}
	}
}

func getVariables(rn string, conds []*plan.JoinCondition) []string {
	var vars []string

	for _, cond := range conds {
		vars = append(vars, cond.Sattr)
	}
	return vars
}

func getFreeVariables(rel *plan.Relation, vars []string) []string {
	var fvars []string

	for _, v := range vars {
		tbl, name := util.SplitTableAndColumn(v)
		if tbl != rel.Alias {
			continue
		}
		if _, ok := rel.AttrsMap[name]; ok {
			fvars = append(fvars, v)
		}
	}
	return fvars
}
