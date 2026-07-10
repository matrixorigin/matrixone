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

// Package plan implements the bm25 plugin's plan-layer hooks.
//
// bm25 is queried through MATCH(col) AGAINST('query') — the fulltext-style
// query surface — NOT the vector ORDER BY <distance> LIMIT rewrite. So the
// vector ApplyForSort/CanApply hooks are inert (return false/no-op); the
// MATCH → bm25 rewrite lives in pkg/sql/plan/apply_indices_bm25.go (Phase 3).
// The hidden-table schema builder is BuildSecondaryIndexDefs (schema.go).
package plan

import (
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
)

// Compile-time interface check.
var _ planplugin.Hooks = Hooks{}

// Hooks implements plugin/plan.Hooks for bm25.
type Hooks struct{}

// CanApply — bm25 does not participate in ORDER BY <score> LIMIT rewrites
// (its query surface is MATCH/AGAINST), so it never applies for sort.
func (Hooks) CanApply(planplugin.PlanBuilder, *planplugin.VectorSortContext, *planplugin.MultiTableIndexRef) (bool, error) {
	return false, nil
}

// ApplyForSort — no-op for bm25 (see CanApply).
func (Hooks) ApplyForSort(_ planplugin.PlanBuilder, _ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, nodeID int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	return nodeID, false, nil
}
