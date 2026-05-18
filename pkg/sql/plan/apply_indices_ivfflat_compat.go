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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
)

// This file hosts compatibility shims for the IVF-FLAT lift (Phase 4e).
// The real bodies of prepareIvfIndexContext and applyIndicesForSortUsingIvfflat
// live in pkg/vectorindex/ivfflat/plugin/plan (PrepareContext and
// Hooks{}.ApplyForSort). Production dispatch routes through the plugin
// registry; these shims exist purely so the existing in-tree tests
// (apply_indices_ivfflat_test.go and apply_indices_ivfflat_optimize_test.go,
// ~2000 LoC) can continue exercising the rewrite without a full mechanical
// port. The shims add no behavior — every line forwards to the plugin.

// prepareIvfIndexContext bridges the old unexported method signature to
// ivfflatplan.PrepareContext. Returns the plugin's exported IndexContext
// type so tests can inspect fields by their CamelCase names (e.g.
// `result.MetaDef`, `result.NProbe`).
func (builder *QueryBuilder) prepareIvfIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*ivfflatplan.IndexContext, error) {
	return ivfflatplan.PrepareContext(builder, exportVectorSortContextForBridge(vecCtx), exportMultiTableIndexForBridge(multiTableIndex))
}

// applyIndicesForSortUsingIvfflat bridges the old unexported method to
// Hooks{}.ApplyForSort. Mirrors the original (int32, error) return —
// the plugin's `applied` bool collapses into "newNodeID != nodeID".
func (builder *QueryBuilder) applyIndicesForSortUsingIvfflat(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	newNodeID, _, err := (ivfflatplan.Hooks{}).ApplyForSort(
		builder,
		exportVectorSortContextForBridge(vecCtx),
		exportMultiTableIndexForBridge(multiTableIndex),
		nodeID,
		planplugin.ApplyForSortOpts{ColRefCnt: colRefCnt, IdxColMap: idxColMap},
	)
	return newNodeID, err
}

func exportVectorSortContextForBridge(v *vectorSortContext) *planplugin.VectorSortContext {
	if v == nil {
		return nil
	}
	return v.export()
}

func exportMultiTableIndexForBridge(m *MultiTableIndex) *planplugin.MultiTableIndexRef {
	if m == nil {
		return nil
	}
	return exportMultiTableIndex(m)
}

// Test bridges for the auto-mode mechanics that were originally
// QueryBuilder methods on apply_indices_ivfflat.go and now live in the
// plugin's context.go. Tests in apply_indices_ivfflat_test.go drive
// them through these shims.

func (builder *QueryBuilder) shouldUseForceMode(vecCtx *vectorSortContext) bool {
	return ivfflatplan.ShouldUseForceMode(exportVectorSortContextForBridge(vecCtx))
}

func (builder *QueryBuilder) resolveVectorSearchMode(
	vecCtx *vectorSortContext,
	enableVectorPrefilterByDefault, enableVectorAutoModeByDefault bool,
) (string, bool, bool) {
	return ivfflatplan.ResolveVectorSearchMode(
		exportVectorSortContextForBridge(vecCtx),
		enableVectorPrefilterByDefault, enableVectorAutoModeByDefault,
	)
}

func (builder *QueryBuilder) calculateAdaptiveNprobe(baseNprobe int64, stats *plan.Stats, totalLists int64) int64 {
	return ivfflatplan.CalculateAdaptiveNprobe(baseNprobe, stats, totalLists)
}

func (builder *QueryBuilder) findScanNodeByTag(nodeID, tag int32) int32 {
	return ivfflatplan.FindScanNodeByTag(builder.qry, nodeID, tag)
}

func clearLimitOffsetInSubtree(qry *plan.Query, nodeID int32) {
	ivfflatplan.ClearLimitOffsetInSubtree(qry, nodeID)
}
