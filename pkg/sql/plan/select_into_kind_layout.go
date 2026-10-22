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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ValidateSelectIntoKindLayout validates the private physical result layout
// consumed by SELECT ... INTO. An empty mapping is the legacy exact-width
// layout; a present mapping exposes only Headings and accounts for every
// physical passenger after that visible prefix.
func ValidateSelectIntoKindLayout(query *planpb.Query) error {
	if query == nil || len(query.IntoUserVariableKindColumns) == 0 {
		return nil
	}
	if query.StmtType != planpb.Query_SELECT || query.HasReturning {
		return fmt.Errorf("SELECT INTO kind layout is only valid for a top-level SELECT")
	}
	visible := len(query.Headings)
	if visible == 0 || len(query.IntoUserVariableKindColumns) != visible {
		return fmt.Errorf("SELECT INTO kind layout has %d entries for %d visible columns", len(query.IntoUserVariableKindColumns), visible)
	}
	step := query.MainOutputStep()
	if step < 0 || int(step) >= len(query.Steps) {
		return fmt.Errorf("SELECT INTO kind layout has no main output step")
	}
	nodeID := query.Steps[step]
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) || query.Nodes[nodeID] == nil {
		return fmt.Errorf("SELECT INTO kind layout has an invalid output node")
	}
	projectList := query.Nodes[nodeID].ProjectList
	if visible > len(projectList) {
		return fmt.Errorf("SELECT INTO kind layout has %d visible columns but %d physical columns", visible, len(projectList))
	}
	used := make([]bool, len(projectList))
	for visibleColumn, physicalColumn := range query.IntoUserVariableKindColumns {
		if physicalColumn == -1 {
			continue
		}
		if physicalColumn < int32(visible) || int(physicalColumn) >= len(projectList) {
			return fmt.Errorf("SELECT INTO kind layout column %d references invalid passenger %d", visibleColumn, physicalColumn)
		}
		passenger := projectList[physicalColumn]
		if passenger == nil || passenger.Typ.Id != int32(types.T_bool) {
			return fmt.Errorf("SELECT INTO kind layout column %d references a non-BOOL passenger %d", visibleColumn, physicalColumn)
		}
		used[physicalColumn] = true
	}
	for physicalColumn := visible; physicalColumn < len(projectList); physicalColumn++ {
		if !used[physicalColumn] {
			return fmt.Errorf("SELECT INTO kind layout leaves physical passenger %d unaccounted", physicalColumn)
		}
	}
	return nil
}
