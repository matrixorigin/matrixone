// Copyright 2024 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (builder *QueryBuilder) applyIndicesForFiltersMasterIndex(id int32, node *plan.Node,
	cnt map[[2]int32]int, colMap map[[2]int32]*plan.Expr) int32 {
	return id
}

func isKeyPresentInList(key string, list []string) bool {
	for _, item := range list {
		if key == item {
			return true
		}
	}
	return false
}
