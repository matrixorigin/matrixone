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
	cagraplan "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type cagraIndexContext struct {
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
	gpuMultiSim  int64
}

func (builder *QueryBuilder) prepareCagraIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*cagraIndexContext, error) {
	if vecCtx == nil || multiTableIndex == nil {
		return nil, nil
	}
	if vecCtx.distFnExpr == nil {
		return nil, nil
	}

	// RankOption.Mode controls vector index behavior:
	// - "force": Disable vector index, force full table scan (for debugging/comparison)
	// - nil/other: Enable vector index with default behavior
	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode == "force" {
		return nil, nil
	}

	rewriteAllowed, err := builder.validateVectorIndexSortRewrite(vecCtx)
	if err != nil || !rewriteAllowed {
		return nil, err
	}

	metaDef := multiTableIndex.IndexDefs[catalog.Cagra_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.Cagra_TblType_Storage]
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

	nThread, err := builder.compCtx.ResolveVariable("cagra_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	batchWindow, err := builder.compCtx.ResolveVariable("cagra_batch_window", true, false)
	if err != nil {
		return nil, err
	}

	gpuMultiSim, err := builder.compCtx.ResolveVariable("gpu_multi_simulation", true, false)
	if err != nil {
		return nil, err
	}

	return &cagraIndexContext{
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
		gpuMultiSim:  gpuMultiSim.(int64),
	}, nil
}

func (builder *QueryBuilder) applyIndicesForSortUsingCagra(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	if !hasCompleteVectorPagination(vecCtx) || vecCtx.sortNode == nil || vecCtx.scanNode == nil {
		return nodeID, nil
	}

	cagraCtx, err := builder.prepareCagraIndexContext(vecCtx, multiTableIndex)
	if err != nil || cagraCtx == nil {
		return nodeID, err
	}

	return builder.applyCuvsIndexSearchScan(nodeID, vecCtx, idxColMap, cuvsSearchScan{
		algo:         "CAGRA",
		alias:        "mo_cagra_alias_0",
		metaDef:      cagraCtx.metaDef,
		idxDef:       cagraCtx.idxDef,
		metaRole:     catalog.Cagra_TblType_Metadata,
		storageRole:  catalog.Cagra_TblType_Storage,
		cols:         cagraplan.CAGRASearchColDefs,
		vecLitArg:    cagraCtx.vecLitArg,
		origFuncName: cagraCtx.origFuncName,
		partPos:      cagraCtx.partPos,
		pkPos:        cagraCtx.pkPos,
		pkType:       cagraCtx.pkType,
		algoOptions: func(filterJSON string) ([]byte, error) {
			return cagraplan.EncodeScanOptions(cagraplan.ScanOptions{
				ThreadsSearch:      cagraCtx.nThread,
				BatchWindow:        cagraCtx.batchWindow,
				GpuMultiSimulation: cagraCtx.gpuMultiSim,
				KeyPartType:        cagraCtx.partType.Id,
				FilterJSON:         filterJSON,
			})
		},
	})
}

/*
func (builder *QueryBuilder) getArgsFromDistFn(distFnExpr *plan.Function, partPos int32) (key *plan.Expr, value *plan.Expr, found bool) {

	if _, ok := metric.DistFuncOpTypes[distFnExpr.Func.ObjName]; !ok {
		return
	}

	distFnArgs := distFnExpr.Args
	if distFnArgs[0].Typ.GetId() != int32(types.T_array_float32) && distFnArgs[0].Typ.GetId() != int32(types.T_array_float64) {
		return
	}

	if distFnArgs[1].GetCol() != nil {
		if distFnArgs[0].GetCol() != nil {
			return
		}

		distFnArgs[0], distFnArgs[1] = distFnArgs[1], distFnArgs[0]
	}

	vecColArg, _ := ConstantFold(batch.EmptyForConstFoldBatch, distFnArgs[0], builder.compCtx.GetProcess(), false, true)
	if vecColArg != nil {
		distFnArgs[0] = vecColArg
	}
	vecLitArg, _ := ConstantFold(batch.EmptyForConstFoldBatch, distFnArgs[1], builder.compCtx.GetProcess(), false, true)
	if vecLitArg != nil {
		distFnArgs[1] = vecLitArg
	}

	if vecColArg.GetCol() == nil {
		return
	}
	if !rule.IsConstant(vecLitArg, true) {
		return
	}

	vecLitArg.Typ = vecColArg.Typ

	if vecColArg.GetCol().ColPos != partPos {
		return
	}

	return vecColArg, vecLitArg, true
}
*/
