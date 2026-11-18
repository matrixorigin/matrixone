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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// A backfill must not copy from another secondary index: that index may itself
// contain the old display-encoded keys. Check the optimized, reachable plan,
// including every query step, before compilation and on a plan rebuild.
func validateEnumIndexBackfillPlan(p *plan.Plan, spec *executor.EnumIndexBackfillSpec) error {
	if spec == nil {
		return nil
	}
	invalid := func() error { return moerr.NewInvalidInputNoCtx("invalid internal index backfill source plan") }
	if p == nil || p.GetQuery() == nil || len(p.GetQuery().Steps) == 0 {
		return invalid()
	}
	query := p.GetQuery()
	if len(query.BackgroundQueries) != 0 || len(query.DetectSqls) != 0 {
		return invalid()
	}
	seen := make(map[int32]bool)
	found := false
	var visit func(int32) error
	visit = func(id int32) error {
		if id < 0 || int(id) >= len(query.Nodes) || query.Nodes[id] == nil {
			return invalid()
		}
		if seen[id] {
			return nil
		}
		seen[id] = true
		node := query.Nodes[id]
		if node.IndexScanInfo.IsIndexScan {
			return invalid()
		}
		switch node.NodeType {
		case plan.Node_TABLE_SCAN:
			if node.ObjRef == nil || uint64(node.ObjRef.Obj) != spec.SourceID || node.TableDef == nil || node.TableDef.TblId != spec.SourceID {
				return invalid()
			}
			found = true
		case plan.Node_VALUE_SCAN, plan.Node_FUNCTION_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_EXTERNAL_FUNCTION,
			plan.Node_VECTOR_QUERY_TOP, plan.Node_VECTOR_QUERY_SOURCE:
			return invalid()
		}
		for _, child := range node.Children {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, step := range query.Steps {
		if err := visit(step); err != nil {
			return err
		}
	}
	if !found {
		return invalid()
	}
	return nil
}
