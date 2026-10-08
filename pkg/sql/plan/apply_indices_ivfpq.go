// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	ivfpqplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type ivfpqIndexContext struct {
	vecCtx       *vectorSortContext
	metaDef      *plan.IndexDef
	idxDef       *plan.IndexDef
	vecLitArg    *plan.Expr
	origFuncName string
	partPos      int32
	partType     plan.Type
	pkPos        int32
	pkType       plan.Type
	nThread      int64
	batchWindow  int64
	nProbe       int64
	gpuMultiSim  int64
}

func (builder *QueryBuilder) prepareIvfpqIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*ivfpqIndexContext, error) {
	if vecCtx == nil || multiTableIndex == nil {
		return nil, nil
	}
	if vecCtx.distFnExpr == nil {
		return nil, nil
	}

	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode == "force" {
		return nil, nil
	}

	rewriteAllowed, err := builder.validateVectorIndexSortRewrite(vecCtx)
	if err != nil || !rewriteAllowed {
		return nil, err
	}

	metaDef := multiTableIndex.IndexDefs[catalog.Ivfpq_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.Ivfpq_TblType_Storage]
	if metaDef == nil || idxDef == nil {
		return nil, nil
	}

	params, err := decodeVectorIndexAlgoParams(metaDef.IndexAlgoParams)
	if err != nil {
		return nil, nil
	}
	opType, ok := vectorIndexStringParam(params, catalog.IndexAlgoParamOpType)
	if !ok {
		return nil, nil
	}

	origFuncName := vecCtx.distFnExpr.Func.ObjName
	// An index serves this distance function when its op_type is metric-equivalent to the
	// query's, not only when it is the canonical one — vector_l2_ops and vector_l2sq_ops
	// build the same index and both answer l2_distance / l2_distance_sq (#25966).
	if !metric.OpTypeServesDistFunc(opType, origFuncName) {
		return nil, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := vecCtx.scanNode.TableDef.Name2ColIndex[keyPart]
	partType := vecCtx.scanNode.TableDef.Cols[partPos].Typ
	_, vecLitArg, found := builder.getArgsFromDistFn(vecCtx.distFnExpr, partPos)
	if !found {
		return nil, nil
	}

	pkPos := vecCtx.scanNode.TableDef.Name2ColIndex[vecCtx.scanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.scanNode.TableDef.Cols[pkPos].Typ

	nThread, err := builder.compCtx.ResolveVariable("ivfpq_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	batchWindow, err := builder.compCtx.ResolveVariable("ivfpq_batch_window", true, false)
	if err != nil {
		return nil, err
	}

	nProbe := int64(20)
	if nProbeIf, err2 := builder.compCtx.ResolveVariable("probe_limit", true, false); err2 != nil {
		return nil, err2
	} else if nProbeIf != nil {
		nProbe = nProbeIf.(int64)
	}

	gpuMultiSim, err := builder.compCtx.ResolveVariable("gpu_multi_simulation", true, false)
	if err != nil {
		return nil, err
	}

	return &ivfpqIndexContext{
		vecCtx:       vecCtx,
		metaDef:      metaDef,
		idxDef:       idxDef,
		vecLitArg:    vecLitArg,
		origFuncName: origFuncName,
		partPos:      partPos,
		partType:     partType,
		pkPos:        pkPos,
		pkType:       pkType,
		nThread:      nThread.(int64),
		batchWindow:  batchWindow.(int64),
		nProbe:       nProbe,
		gpuMultiSim:  gpuMultiSim.(int64),
	}, nil
}

func (builder *QueryBuilder) applyIndicesForSortUsingIvfpq(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	if !hasCompleteVectorPagination(vecCtx) || vecCtx.sortNode == nil || vecCtx.scanNode == nil {
		return nodeID, nil
	}

	ivfpqCtx, err := builder.prepareIvfpqIndexContext(vecCtx, multiTableIndex)
	if err != nil || ivfpqCtx == nil {
		return nodeID, err
	}

	return builder.applyCuvsIndexSearchScan(nodeID, vecCtx, idxColMap, cuvsSearchScan{
		algo:         "IVF-PQ",
		alias:        "mo_ivfpq_alias_0",
		metaDef:      ivfpqCtx.metaDef,
		idxDef:       ivfpqCtx.idxDef,
		metaRole:     catalog.Ivfpq_TblType_Metadata,
		storageRole:  catalog.Ivfpq_TblType_Storage,
		cols:         ivfpqplan.IVFPQSearchColDefs,
		vecLitArg:    ivfpqCtx.vecLitArg,
		origFuncName: ivfpqCtx.origFuncName,
		partPos:      ivfpqCtx.partPos,
		pkPos:        ivfpqCtx.pkPos,
		pkType:       ivfpqCtx.pkType,
		algoOptions: func(filterJSON string) ([]byte, error) {
			return ivfpqplan.EncodeScanOptions(ivfpqplan.ScanOptions{
				ThreadsSearch:      ivfpqCtx.nThread,
				BatchWindow:        ivfpqCtx.batchWindow,
				GpuMultiSimulation: ivfpqCtx.gpuMultiSim,
				Nprobe:             uint(ivfpqCtx.nProbe),
				KeyPartType:        ivfpqCtx.partType.Id,
				FilterJSON:         filterJSON,
			})
		},
	})
}
