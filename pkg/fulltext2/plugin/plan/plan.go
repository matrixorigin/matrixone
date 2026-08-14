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

// Package plan holds the fulltext2 index's plan-layer hooks: hidden-table schema
// construction (BuildFullTextIndexDefs) and the inert vector-sort hooks (fulltext2
// is queried via MATCH, not the ORDER BY <distance> rewrite).
package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var _ planplugin.Hooks = Hooks{}

type Hooks struct{}

// BuildSecondaryIndexDefs — fulltext2 is created via CREATE FULLTEXT2 INDEX
// (*tree.FullTextIndex → BuildFullTextIndexDefs), not the vector `USING` path.
func (Hooks) BuildSecondaryIndexDefs(
	_ planplugin.CompilerContext, _ *tree.Index, _ map[string]*plan.ColDef, _ []*plan.IndexDef, _ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("fulltext2 index is created via CREATE FULLTEXT2 INDEX")
}

// CanApply — fulltext2 is queried via MATCH (the apply_indices_fulltext path),
// never the vector ORDER BY <distance> LIMIT rewrite.
func (Hooks) CanApply(planplugin.PlanBuilder, *planplugin.VectorSortContext, *planplugin.MultiTableIndexRef) (bool, error) {
	return false, nil
}

// ApplyForSort — inert (returns the node id unchanged).
func (Hooks) ApplyForSort(
	_ planplugin.PlanBuilder, _ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, nodeID int32, _ planplugin.ApplyForSortOpts,
) (int32, bool, error) {
	return nodeID, false, nil
}
