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
	"math"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// IndexContext bundles every per-query state PrepareContext computes
// once and ApplyForSort reuses. Mirrors the pre-lift apply_indices_ivfflat
// struct of the same name.
type IndexContext struct {
	VecCtx          *vectorplan.VectorSortContext
	MetaDef         *plan.IndexDef
	IdxDef          *plan.IndexDef
	EntriesDef      *plan.IndexDef
	VecLitArg       *plan.Expr
	OrigFuncName    string
	PartPos         int32
	PartType        plan.Type
	PkPos           int32
	PkType          plan.Type
	Params          string
	NThread         int64
	NProbe          int64
	PushdownEnabled bool

	// Auto-mode bookkeeping. IsAutoMode is set when the user selected
	// auto (explicitly or via the session default); InitialStrategy is
	// the strategy auto-mode resolution picked ("pre" or "post"), which
	// downstream code uses to size over-fetch and re-emit RankOption
	// for the executor.
	IsAutoMode      bool
	InitialStrategy string
}

// shouldUseForceMode decides whether to bypass the index entirely and
// fall back to a full table scan. The rule of thumb is:
//
//	estimated rows after filtering < LIMIT × 2
//
// For tiny result sets, index overhead (metadata reads, distance recompute)
// dominates over the savings, and a full scan also guarantees 100% recall.
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:shouldUseForceMode.
func ShouldUseForceMode(vecCtx *vectorplan.VectorSortContext) bool {
	scanNode := vecCtx.ScanNode
	stats := scanNode.Stats

	var tableCnt float64
	selectivity := 1.0
	if stats != nil {
		tableCnt = stats.TableCnt
		if stats.Selectivity > 0 && stats.Selectivity < 1 {
			selectivity = stats.Selectivity
		}
	}
	if tableCnt <= 0 {
		return false
	}
	limitExpr := vecCtx.Limit
	if limitExpr == nil {
		return false
	}
	limitConst := limitExpr.GetLit()
	if limitConst == nil {
		return false
	}
	limitVal := float64(limitConst.GetU64Val())
	if limitVal <= 0 {
		return false
	}
	estimatedRows := tableCnt * selectivity
	threshold := limitVal * 2.0
	if tableCnt < threshold || estimatedRows < threshold {
		logutil.Debugf(
			"Auto mode: small dataset or high selectivity detected, table_rows=%.0f, selectivity=%.4f, estimated_rows=%.0f, limit=%.0f, threshold=%.0f",
			tableCnt, selectivity, estimatedRows, limitVal, threshold,
		)
		return true
	}
	return false
}

// resolveVectorSearchMode picks the search mode ("pre", "post", "force")
// from the user-supplied RankOption.Mode plus the two session defaults.
// Returns the chosen mode, whether auto mode is active, and whether the
// index should be disabled entirely (force mode).
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:resolveVectorSearchMode.
func ResolveVectorSearchMode(
	vecCtx *vectorplan.VectorSortContext,
	enableVectorPrefilterByDefault bool,
	enableVectorAutoModeByDefault bool,
) (mode string, isAutoMode bool, shouldDisableIndex bool) {
	var userMode string
	if vecCtx.RankOption != nil && vecCtx.RankOption.Mode != "" {
		userMode = vecCtx.RankOption.Mode
	}

	if userMode == "force" {
		return "force", false, true
	}

	if userMode == "auto" || (userMode == "" && enableVectorAutoModeByDefault) {
		isAutoMode = true
		if ShouldUseForceMode(vecCtx) {
			logutil.Debugf("Auto mode: small dataset, selected 'force'")
			return "force", isAutoMode, true
		}
		logutil.Debugf("Auto mode: normal case, selected 'post'")
		return "post", isAutoMode, false
	}

	if userMode == "pre" || userMode == "post" {
		return userMode, false, false
	}

	if enableVectorPrefilterByDefault {
		mode = "pre"
	} else {
		mode = "post"
	}
	return mode, false, false
}

// calculateAdaptiveNprobe scales the base nprobe up by 1/sqrt(selectivity)
// so highly-selective filters get more centroid coverage and don't starve
// post-filter for candidates. Only invoked in auto + post mode.
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:calculateAdaptiveNprobe.
func CalculateAdaptiveNprobe(baseNprobe int64, stats *plan.Stats, totalLists int64) int64 {
	if stats == nil || stats.Selectivity <= 0 || stats.Selectivity >= 1 {
		return baseNprobe
	}
	compensation := math.Sqrt(1.0 / stats.Selectivity)
	adaptiveNprobe := int64(math.Ceil(float64(baseNprobe) * compensation))
	if adaptiveNprobe < baseNprobe {
		adaptiveNprobe = baseNprobe
	}
	if adaptiveNprobe > totalLists {
		adaptiveNprobe = totalLists
	}
	return adaptiveNprobe
}

// PrepareContext validates that this MultiTableIndex can satisfy the
// captured ORDER BY, and packages every per-query input ApplyForSort
// needs into an IndexContext. Returns (nil, nil) when the index is
// not applicable (op_type mismatch, force mode, JOIN argument extraction
// failed, etc.) — callers fall back to the exact-sort path.
//
// Lifted from pkg/sql/plan/apply_indices_ivfflat.go:prepareIvfIndexContext.
func PrepareContext(
	pb vectorplan.PlanBuilder,
	vecCtx *vectorplan.VectorSortContext,
	mti *vectorplan.MultiTableIndexRef,
) (*IndexContext, error) {
	if vecCtx == nil || mti == nil {
		return nil, nil
	}
	if vecCtx.DistFnExpr == nil {
		return nil, nil
	}
	if vecCtx.RankOption != nil && vecCtx.RankOption.Mode == "force" {
		return nil, nil
	}

	rewriteAllowed, err := pb.ValidateVectorIndexSortRewrite(vecCtx)
	if err != nil || !rewriteAllowed {
		return nil, err
	}

	var enableVectorPrefilterByDefault bool
	if val, err := pb.ResolveVariable("enable_vector_prefilter_by_default", true, false); err == nil && val != nil {
		if v, ok := val.(int8); ok && v == 1 {
			enableVectorPrefilterByDefault = true
		}
	}
	var enableVectorAutoModeByDefault bool
	if val, err := pb.ResolveVariable("enable_vector_auto_mode_by_default", true, false); err == nil && val != nil {
		if v, ok := val.(int8); ok && v == 1 {
			enableVectorAutoModeByDefault = true
		}
	}

	mode, isAutoMode, shouldDisableIndex := ResolveVectorSearchMode(
		vecCtx,
		enableVectorPrefilterByDefault,
		enableVectorAutoModeByDefault,
	)
	if shouldDisableIndex {
		return nil, nil
	}
	if isAutoMode {
		logutil.Debugf("Vector search auto mode enabled, initial strategy: %s", mode)
	}

	metaDef := mti.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
	idxDef := mti.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids]
	entriesDef := mti.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries]
	if metaDef == nil || idxDef == nil || entriesDef == nil {
		return nil, nil
	}

	opTypeAst, err := sonic.Get([]byte(metaDef.IndexAlgoParams), catalog.IndexAlgoParamOpType)
	if err != nil {
		return nil, nil
	}
	opType, err := opTypeAst.StrictString()
	if err != nil {
		return nil, nil
	}

	var totalLists int64 = -1
	if listsAst, err2 := sonic.Get([]byte(metaDef.IndexAlgoParams), catalog.IndexAlgoParamLists); err2 == nil {
		if lists, err3 := listsAst.Int64(); err3 == nil {
			totalLists = lists
		}
	}

	origFuncName := vecCtx.DistFnExpr.Func.ObjName
	if opType != metric.DistFuncOpTypes[origFuncName] {
		return nil, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := vecCtx.ScanNode.TableDef.Name2ColIndex[keyPart]
	var vecLitArg *plan.Expr
	var found bool
	if vecCtx.VecArgExpr != nil {
		_, vecLitArg, found = pb.GetArgsFromDistFnForJoin(
			vecCtx.DistFnExpr, partPos, vecCtx.ScanNode.BindingTags[0])
	} else {
		_, vecLitArg, found = pb.GetArgsFromDistFn(vecCtx.DistFnExpr, partPos)
	}
	if !found {
		return nil, nil
	}

	nThread, err := pb.ResolveVariable("ivf_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	nProbe := int64(5)
	if nProbeIf, err := pb.ResolveVariable("probe_limit", true, false); err != nil {
		return nil, err
	} else if nProbeIf != nil {
		val, ok := nProbeIf.(int64)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx("ResolveVariable: probe_limit is not int64")
		}
		nProbe = val
	}

	// Dynamic nprobe amplification for auto mode + post strategy.
	if isAutoMode && mode == "post" && totalLists > 0 {
		oldNProbe := nProbe
		nProbe = CalculateAdaptiveNprobe(nProbe, vecCtx.ScanNode.Stats, totalLists)
		if nProbe != oldNProbe {
			logutil.Debugf("Auto mode: adjusted nprobe from %d to %d (selectivity: %.4f)",
				oldNProbe, nProbe, vecCtx.ScanNode.Stats.Selectivity)
		}
	}

	pkPos := vecCtx.ScanNode.TableDef.Name2ColIndex[vecCtx.ScanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.ScanNode.TableDef.Cols[pkPos].Typ
	partType := vecCtx.ScanNode.TableDef.Cols[partPos].Typ

	return &IndexContext{
		VecCtx:          vecCtx,
		MetaDef:         metaDef,
		IdxDef:          idxDef,
		EntriesDef:      entriesDef,
		VecLitArg:       vecLitArg,
		OrigFuncName:    origFuncName,
		PartPos:         partPos,
		PartType:        partType,
		PkPos:           pkPos,
		PkType:          pkType,
		Params:          idxDef.IndexAlgoParams,
		NThread:         nThread.(int64),
		NProbe:         nProbe,
		PushdownEnabled: (mode == "pre"),
		IsAutoMode:      isAutoMode,
		InitialStrategy: mode,
	}, nil
}
