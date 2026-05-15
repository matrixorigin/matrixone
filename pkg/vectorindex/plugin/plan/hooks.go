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

// Package plan defines the plan-layer hooks every vector index plugin must
// implement: query rewrite (ANN ORDER BY → index scan) and DML index sync.
//
// The interface receives the `vectorplan` facade from pkg/sql/plan/vectorplan
// — that's what lets the algorithm body live entirely inside the plugin
// without taking a dependency on pkg/sql/plan.
package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
)

// Hooks bundles every plan-layer callback for one algorithm.
type Hooks interface {
	// BuildSecondaryIndexDefs constructs the IndexDef and TableDef list
	// for this algorithm's hidden tables, given a CREATE INDEX statement.
	// Replaces buildXxxSecondaryIndexDef and one switch arm at
	// pkg/sql/plan/build_ddl.go:2081.
	//
	// ctx is *plan.CompilerContext expressed through vectorplan's narrow
	// re-export; the algorithm only needs ctx.GetContext() for error
	// messages and util.BuildIndexTableName.
	BuildSecondaryIndexDefs(ctx vectorplan.CompilerContext, idx *tree.Index,
		colMap map[string]*plan.ColDef, existedIndexes []*plan.IndexDef,
		pkeyName string) ([]*plan.IndexDef, []*plan.TableDef, error)

	// CanApply is a non-destructive probe — does this index look like a
	// candidate for the captured ORDER BY? Used by detectVectorGuard to
	// protect the scan node from other optimizers before ApplyForSort
	// runs. Replaces the inner-body prepare<X>IndexContext probes at
	// apply_indices.go:847-885.
	CanApply(pb vectorplan.PlanBuilder, vctx *vectorplan.VectorSortContext,
		mti *vectorplan.MultiTableIndexRef) (bool, error)

	// ApplyForSort rewrites the query plan to use this index for the
	// captured ORDER BY (distfn(col, v)) LIMIT k pattern. Returns:
	//   newNodeID — the root of the rewritten sub-plan
	//   applied   — true if the rewrite was performed; false if the index
	//               cannot satisfy the query (e.g. op_type mismatch)
	//   err       — non-nil only on hard errors; "cannot apply" is
	//               communicated via applied=false
	//
	// opts carries per-call state the algorithm may need; today only
	// IVF-FLAT's auto-mode rewrite consults ColRefCnt / IdxColMap.
	//
	// Replaces apply_indices.go:611 dispatch +
	// prepare<X>IndexContext + applyIndicesForSortUsing<X>.
	ApplyForSort(pb vectorplan.PlanBuilder, vctx *vectorplan.VectorSortContext,
		mti *vectorplan.MultiTableIndexRef, nodeID int32,
		opts vectorplan.ApplyForSortOpts) (newNodeID int32, applied bool, err error)

	// DMLSyncTableTypes returns the IndexAlgoTableType strings for the
	// hidden tables this algorithm syncs SYNCHRONOUSLY during INSERT /
	// DELETE / UPDATE plan construction (today: only IVF-FLAT's entries
	// table). Returns an empty slice for algorithms that use CDC
	// instead (HNSW / CAGRA / IVF-PQ — see
	// catalog.Hooks.SyncDescriptor().UsesCDC).
	//
	// Consumed by pkg/sql/plan/build_dml_util.go:1346 to gate which
	// hidden tables get their IndexTableName appended to
	// delNodeInfo.indexTableNames during DELETE plan construction.
	DMLSyncTableTypes() []string

	// BuildPreInsertSyncPlan emits the plan nodes that synchronously
	// populate the algorithm's hidden tables during INSERT. Called only
	// when DMLSyncTableTypes() is non-empty AND the index is not async.
	//
	// Replaces the IVFFLAT-hardcoded block at
	// build_dml_util.go:3590-3673. Algorithms without sync DML return
	// nil here.
	BuildPreInsertSyncPlan(pb vectorplan.PlanBuilder, ctx vectorplan.BindContext,
		dml vectorplan.DMLInsertContext, mti *vectorplan.MultiTableIndexRef) error

	// BuildDeleteSyncPlan emits the plan nodes for synchronous delete
	// from the algorithm's hidden tables. Replaces the IVFFLAT-hardcoded
	// block at build_dml_util.go:3675-3837. Algorithms without sync DML
	// return nil here.
	BuildDeleteSyncPlan(pb vectorplan.PlanBuilder, ctx vectorplan.BindContext,
		dml vectorplan.DMLDeleteContext, mti *vectorplan.MultiTableIndexRef) error
}
