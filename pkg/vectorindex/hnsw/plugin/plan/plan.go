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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

// BuildAlterReIndex is a no-op for HNSW. HNSW's reindex is an
// incremental update (HandleReindex re-runs HandleCreateIndex) and
// HnswSync derives its state from the event stream, so neither
// AlgoParamList nor ForceSync are honored.
func (Hooks) BuildAlterReIndex(_ planplugin.CompilerContext, _ *tree.AlterOptionAlterReIndex, _ *plan.AlterTableAlterReIndex) error {
	return nil
}
