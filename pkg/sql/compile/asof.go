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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
)

func isMixedJSONNumericAsof(node *plan.Node) bool {
	return node != nil && (node.JoinType == plan.Node_ASOF || node.JoinType == plan.Node_ASOF_LEFT) &&
		colexec.HasMixedJSONNumericEquality(node.OnList...)
}

func queryHasMixedJSONNumericAsof(qry *plan.Query) bool {
	for _, node := range qry.Nodes {
		if isMixedJSONNumericAsof(node) {
			return true
		}
	}
	return false
}
