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

// Package plan implements the fulltext plugin's plan-layer hooks.
//
// Fulltext uses a parallel hook BuildFullTextIndexDefs (taking
// *tree.FullTextIndex) instead of BuildSecondaryIndexDefs (which
// takes *tree.Index, the vector-index parse-tree shape). The vector
// plugins stub BuildFullTextIndexDefs; the fulltext plugin stubs
// BuildSecondaryIndexDefs; the SQL-layer dispatch picks the right
// hook by parse-tree type.
//
// Phase 1: BuildFullTextIndexDefs returns an error. Inline path at
// pkg/sql/plan/build_ddl.go::buildFullTextIndexTable still handles
// fulltext. Phase 3 will lift the body here.
package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
)

// Compile-time interface check.
var _ planplugin.Hooks = Hooks{}

// Hooks implements plugin/plan.Hooks for fulltext indexes.
type Hooks struct{}

// BuildSecondaryIndexDefs is unreachable for fulltext — the
// plan-build dispatch routes *tree.Index parse trees to the vector
// plugins. Fulltext receives its parse tree via BuildFullTextIndexDefs.
func (Hooks) BuildSecondaryIndexDefs(
	_ planplugin.CompilerContext,
	_ *tree.Index,
	_ map[string]*plan.ColDef,
	_ []*plan.IndexDef,
	_ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("fulltext plugin uses BuildFullTextIndexDefs (not BuildSecondaryIndexDefs)")
}

// CanApply — fulltext has no ANN-style ORDER BY rewrite; report
// inapplicable.
func (Hooks) CanApply(_ planplugin.PlanBuilder, _ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	return false, nil
}

// ApplyForSort — same, never applies; passthrough.
func (Hooks) ApplyForSort(_ planplugin.PlanBuilder, _ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, nodeID int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	return nodeID, false, nil
}
