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

// Package plan implements the HNSW plugin's plan-layer hooks.
//
// Phase 6 split: bodies for ANN rewrite (CanApply, ApplyForSort) live
// in pkg/sql/plan/apply_indices_hnsw.go as methods on *QueryBuilder.
// Plugin Hooks are thin one-line redirects via the planplugin.PlanBuilder
// facade. Hidden-table schema (BuildSecondaryIndexDefs in schema.go) and
// the hnsw_create / hnsw_search table-function builders (tablefunc.go)
// stay here.
package plan

import (
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type Hooks struct{}

var _ planplugin.Hooks = Hooks{}

// CanApply redirects to (*plan.QueryBuilder).CanApplyHnsw.
func (Hooks) CanApply(pb planplugin.PlanBuilder, vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef) (bool, error) {
	return pb.CanApplyHnsw(vctx, mti)
}

// ApplyForSort redirects to (*plan.QueryBuilder).ApplyIndicesForSortUsingHnsw.
func (Hooks) ApplyForSort(pb planplugin.PlanBuilder, vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef, nodeID int32, opts planplugin.ApplyForSortOpts) (int32, bool, error) {
	return pb.ApplyIndicesForSortUsingHnsw(vctx, mti, nodeID, opts)
}

// ValidateViewDefinition: nothing to refuse. A hnsw index is an optimization, not a
// precondition for execution -- l2_distance and friends are real kernels, so a view whose
// plan does not reach the index still runs as a brute-force scan and sort. Measured with no
// index, with a mismatched op_type, and under mode=force: all three plan as Sort -> Table
// Scan and return correct rows.
//
// A consumer above the Top-K (outer ORDER BY, join) does drop the index and fall back that
// way, which is a real performance cliff -- but a planner one, tracked and fixed as
// #25967 / #25974, not something view DDL should reject.
func (Hooks) ValidateViewDefinition(_ planplugin.CompilerContext, _ *plan.Query) error {
	return nil
}
