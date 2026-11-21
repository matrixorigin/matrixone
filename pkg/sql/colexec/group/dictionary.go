// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package group

import (
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

// AcceptsDictionary covers ordinary row aggregation. State/partial input,
// DISTINCT, ordered/rollup and unconverted aggregate contracts remain flat.
func (group *Group) AcceptsDictionary(child int) bool {
	if child != 0 || !group.NeedEval || group.SortRollup || group.DynamicGrouping || hasInactiveGroupingColumn(group.GroupingFlag) {
		return false
	}
	if !colexec.ExpressionsAcceptDictionary(group.ctr.groupByEvaluate.Executor) {
		return false
	}
	for _, args := range group.ctr.aggArgEvaluate {
		if !colexec.ExpressionsAcceptDictionary(args.Executor) {
			return false
		}
	}
	if len(group.ctr.aggList) != len(group.Aggs) {
		return false
	}
	for _, agg := range group.ctr.aggList {
		if !aggexec.SupportsDictionaryInput(agg) {
			return false
		}
	}
	return true
}
