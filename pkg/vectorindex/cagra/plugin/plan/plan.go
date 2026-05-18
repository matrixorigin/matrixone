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

// Package plan implements the Cagra plugin's plan-layer hooks.
// Phase 6 split: bodies live in pkg/sql/plan; this file is thin redirects.
// See pkg/vectorindex/hnsw/plugin/plan/plan.go for the canonical template.
package plan

import (
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
)

type Hooks struct{}

var _ planplugin.Hooks = Hooks{}

func (Hooks) CanApply(pb planplugin.PlanBuilder, vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef) (bool, error) {
	return pb.CanApplyCagra(vctx, mti)
}

func (Hooks) ApplyForSort(pb planplugin.PlanBuilder, vctx *planplugin.VectorSortContext, mti *planplugin.MultiTableIndexRef, nodeID int32, opts planplugin.ApplyForSortOpts) (int32, bool, error) {
	return pb.ApplyIndicesForSortUsingCagra(vctx, mti, nodeID, opts)
}
