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
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type ivfIndexContext struct {
	vecCtx       *vectorSortContext
	metaDef      *plan.IndexDef
	idxDef       *plan.IndexDef
	entriesDef   *plan.IndexDef
	vecLitArg    *plan.Expr
	origFuncName string
	partPos      int32
	partType     plan.Type
	pkPos        int32
	pkType       plan.Type
	params       string
	nThread      int64
	nProbe       int64
}

func (builder *QueryBuilder) prepareIvfIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*ivfIndexContext, error) {
	if vecCtx == nil || multiTableIndex == nil {
		return nil, nil
	}
	if vecCtx.distFnExpr == nil {
		return nil, nil
	}

	// RankOption.Mode controls vector index behavior:
	// - "pre": Enable vector index with BloomFilter pushdown for better filtering
	// - "force": Disable vector index, force full table scan (for debugging/comparison)
	// - nil/other: Enable vector index with default behavior
	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode == "force" {
		return nil, nil
	}

	metaDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids]
	entriesDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries]
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

	origFuncName := vecCtx.distFnExpr.Func.ObjName
	if opType != metric.DistFuncOpTypes[origFuncName] {
		return nil, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := vecCtx.scanNode.TableDef.Name2ColIndex[keyPart]
	_, vecLitArg, found := builder.getArgsFromDistFn(vecCtx.distFnExpr, partPos)
	if !found {
		return nil, nil
	}

	nThread, err := builder.compCtx.ResolveVariable("ivf_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	nProbe := int64(5)
	if nProbeIf, err := builder.compCtx.ResolveVariable("probe_limit", true, false); err != nil {
		return nil, err
	} else if nProbeIf != nil {
		val, ok := nProbeIf.(int64)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx("ResolveVariable: probe_limit is not int64")
		}
		nProbe = val
	}

	pkPos := vecCtx.scanNode.TableDef.Name2ColIndex[vecCtx.scanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.scanNode.TableDef.Cols[pkPos].Typ
	partType := vecCtx.scanNode.TableDef.Cols[partPos].Typ

	return &ivfIndexContext{
		vecCtx:       vecCtx,
		metaDef:      metaDef,
		idxDef:       idxDef,
		entriesDef:   entriesDef,
		vecLitArg:    vecLitArg,
		origFuncName: origFuncName,
		partPos:      partPos,
		partType:     partType,
		pkPos:        pkPos,
		pkType:       pkType,
		params:       idxDef.IndexAlgoParams,
		nThread:      nThread.(int64),
		nProbe:       nProbe,
	}, nil
}

func (builder *QueryBuilder) applyIndicesForSortUsingIvfflat(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	if vecCtx == nil || vecCtx.sortNode == nil || vecCtx.scanNode == nil {
		return nodeID, nil
	}

	ctx := builder.ctxByNode[nodeID]
	projNode := vecCtx.projNode
	sortNode := vecCtx.sortNode
	scanNode := vecCtx.scanNode
	childNode := vecCtx.childNode
	orderExpr := vecCtx.orderExpr
	rankOption := vecCtx.rankOption
	limit := vecCtx.limit

	ivfCtx, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	if err != nil || ivfCtx == nil {
		return nodeID, err
	}

	tblCfgStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d,
			"entries": "%s", "nprobe" : %d, "pktype" : %d, "pkey" : "%s", "part" : "%s", "parttype" : %d, "orig_func_name": "%s"}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		ivfCtx.metaDef.IndexTableName,
		ivfCtx.idxDef.IndexTableName,
		ivfCtx.nThread,
		ivfCtx.entriesDef.IndexTableName,
		uint(ivfCtx.nProbe),
		ivfCtx.pkType.Id,
		scanNode.TableDef.Pkey.PkeyColName,
		ivfCtx.idxDef.Parts[0],
		ivfCtx.partType.Id,
		ivfCtx.origFuncName)

	// build ivf_search table function node
	tableFuncTag := builder.genNewBindTag()
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  kIVFSearchFuncName,
				Param: []byte(ivfCtx.params),
			},
			Cols: DeepCopyColDefList(kIVFSearchColDefs),
		},
		BindingTags: []int32{tableFuncTag},
		TblFuncExprList: []*plan.Expr{
			{
				Typ: plan.Type{
					Id: int32(types.T_varchar),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_Sval{
							Sval: tblCfgStr,
						},
					},
				},
			},
			DeepCopyExpr(ivfCtx.vecLitArg),
		},
	}
	tableFuncNodeID := builder.appendNode(tableFuncNode, ctx)

	err = builder.addBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_ivf_alias_0")}, ctx)
	if err != nil {
		return 0, err
	}

	// change doc_id type to the primary type here
	tableFuncNode.TableDef.Cols[0].Typ = ivfCtx.pkType

	// pushdown limit to Table Function
	// When there are filters, over-fetch to get more candidates
	// This ensures we have enough candidates after filtering
	if len(scanNode.FilterList) > 0 {
		// Over-fetch strategy: dynamically adjust factor based on limit size
		// Smaller limits need more over-fetching due to higher variance
		if limitConst := limit.GetLit(); limitConst != nil {
			originalLimit := limitConst.GetU64Val()

			// Use shared function to calculate over-fetch factor
			overFetchFactor := calculatePostFilterOverFetchFactor(originalLimit)

			newLimit := max(uint64(float64(originalLimit)*overFetchFactor), originalLimit+10)
			tableFuncNode.Limit = &Expr{
				Typ: limit.Typ,
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_U64Val{
							U64Val: newLimit,
						},
					},
				},
			}
		} else {
			// If limit is not a constant, just copy it
			tableFuncNode.Limit = DeepCopyExpr(limit)
		}
	} else {
		// No filters, use original limit
		tableFuncNode.Limit = DeepCopyExpr(limit)
	}

	// Determine join structure based on rankOption.mode:
	//   mode != "pre": JOIN( scanNode, ivf_search )
	//   mode == "pre": JOIN( scanNode, JOIN(ivf_search, secondScan) )
	var joinRootID int32

	pushdownEnabled := rankOption != nil && rankOption.Mode == "pre"

	if pushdownEnabled {
		// secondScanNode: copy original scanNode for JOIN(ivf, table)
		secondScanNodeID := builder.copyNode(ctx, scanNode.NodeId)
		secondScanNode := builder.qry.Nodes[secondScanNodeID]
		baseSecondScan := secondScanNode

		oldTag := secondScanNode.BindingTags[0]
		builder.rebindScanNode(secondScanNode)
		newTag := secondScanNode.BindingTags[0]

		// Update colRefCnt and idxColMap to reflect the new binding tag
		// This is essential for index optimization to work correctly on the rebound node
		if oldTag != newTag {
			for key, value := range colRefCnt {
				if key[0] == oldTag {
					colRefCnt[[2]int32{newTag, key[1]}] = value
				}
			}
			for key, value := range idxColMap {
				if key[0] == oldTag {
					idxColMap[[2]int32{newTag, key[1]}] = DeepCopyExpr(value)
				}
			}
		}

		if builder.canApplyRegularIndex(secondScanNode) {
			optimizedSecondScanID := builder.applyIndicesForFilters(secondScanNodeID, secondScanNode, colRefCnt, idxColMap)
			secondScanNodeID = optimizedSecondScanID
			secondScanNode = builder.qry.Nodes[secondScanNodeID]
		}

		// Otherwise BloomFilter will only see the truncated primary key set, causing data loss.
		secondScanNode.Limit = nil
		secondScanNode.Offset = nil

		// Add a PROJECT node above secondScanNode to output only the primary key column
		secondProjectTag := builder.genNewBindTag()
		secondPkExpr := builder.buildPkExprFromNode(secondScanNodeID, ivfCtx.pkType, scanNode.TableDef.Pkey.PkeyColName)
		if secondPkExpr == nil {
			secondPkExpr = &plan.Expr{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: baseSecondScan.BindingTags[0],
						ColPos: ivfCtx.pkPos,
						Name:   scanNode.TableDef.Cols[ivfCtx.pkPos].Name,
					},
				},
			}
		}
		secondProjectNodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{secondScanNodeID},
			ProjectList: []*plan.Expr{secondPkExpr},
			BindingTags: []int32{secondProjectTag},
		}, ctx)

		// inner join: (ivf_search table function JOIN second table project)
		innerJoinOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 0, // tf.pkid
					},
				},
			},
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: secondProjectTag,
						ColPos: 0, // only pk column from second scan
					},
				},
			},
		})

		innerJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{tableFuncNodeID, secondProjectNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{innerJoinOn},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// Construct BloomFilter type runtime filter for inner join + table function
		rfTag := builder.genNewMsgTag()

		// build side: primary key from secondScanNode (consistent with BloomFilter build column)
		buildExpr := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: secondProjectTag,
					ColPos: 0,
				},
			},
		}
		buildSpec := MakeRuntimeFilter(rfTag, false, 0, buildExpr, false)
		buildSpec.UseBloomFilter = true
		innerJoinNode := builder.qry.Nodes[innerJoinNodeID]
		innerJoinNode.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{buildSpec}

		// probe side: pkid column from table function
		probeExpr := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
				},
			},
		}
		probeSpec := MakeRuntimeFilter(rfTag, false, 0, probeExpr, false)
		probeSpec.UseBloomFilter = true
		tableFuncNode.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{probeSpec}

		// outer join: original table JOIN (inner ivf join)
		outerOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: ivfCtx.pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag, // tf pkid from inner join subtree
						ColPos: 0,
					},
				},
			},
		})

		outerJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{scanNode.NodeId, innerJoinNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{outerOn},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// Manually construct a runtime filter for outer join:
		//   - build side: right child inner join (smaller set, contains actual pkid)
		//   - probe side: left child table scan (original table), performs block/row pruning at scan stage.
		// Note:
		//   1) We don't use BloomFilter here, but use the existing IN-list runtime filter pipeline;
		//   2) UpperLimit is set to avoid all filters being degraded to PASS due to 0.
		rfTag2 := builder.genNewMsgTag()

		// probe: primary key column from original table (scanNode left child)
		probeExpr2 := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: ivfCtx.pkPos,
				},
			},
		}
		probeSpec2 := MakeRuntimeFilter(rfTag2, false, 0, DeepCopyExpr(probeExpr2), false)
		scanNode.RuntimeFilterProbeList = append(scanNode.RuntimeFilterProbeList, probeSpec2)

		// build: placeholder column, HashBuild will generate IN-list based on build side join key's UniqueJoinKeys[0]
		buildExpr2 := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -1,
					ColPos: 0,
				},
			},
		}

		// Set inLimit to "unlimited" to ensure this runtime filter won't be disabled due to upper limit.
		// Use int32 max value directly here.
		const unlimitedInFilterCard = int32(1<<31 - 1)
		buildSpec2 := MakeRuntimeFilter(rfTag2, false, unlimitedInFilterCard, buildExpr2, false)

		outerJoinNode := builder.qry.Nodes[outerJoinNodeID]
		outerJoinNode.RuntimeFilterBuildList = append(outerJoinNode.RuntimeFilterBuildList, buildSpec2)

		// Outer join doesn't add extra project, let global column pruning optimizer handle it
		joinRootID = outerJoinNodeID
	} else {
		// JOIN( table, ivf )
		wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: ivfCtx.pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 0, // tf.pkid
					},
				},
			},
		})

		joinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{scanNode.NodeId, tableFuncNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{wherePkEqPk},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// In non-nested mode, outer join also doesn't add extra project, let optimizer handle column pruning
		joinRootID = joinNodeID
	}

	// Keep FilterList on scanNode so filters are applied during table scan
	// Clear Limit/Offset from scanNode since they should be applied after SORT
	scanNode.Limit = nil
	scanNode.Offset = nil

	// Create SortBy, still sort directly by table function's score, let remap map ColRef to corresponding output column
	orderByScore := []*OrderBySpec{
		{
			Expr: &plan.Expr{
				Typ: tableFuncNode.TableDef.Cols[1].Typ, // score column
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 1, // score column
					},
				},
			},
			Flag: vecCtx.sortDirection,
		},
	}

	sortByID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{joinRootID},
		OrderBy:  orderByScore,
		Limit:    limit,                         // Apply LIMIT after sorting
		Offset:   DeepCopyExpr(sortNode.Offset), // Apply OFFSET after sorting
	}, ctx)

	projNode.Children[0] = sortByID

	if childNode != nil {
		sortIdx := orderExpr.GetCol().ColPos
		projMap := make(map[[2]int32]*plan.Expr)
		for i, proj := range childNode.ProjectList {
			if i == int(sortIdx) {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = DeepCopyExpr(orderByScore[0].Expr)
			} else {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = proj
			}
		}

		replaceColumnsForNode(projNode, projMap)
	}

	return nodeID, nil
}

func (builder *QueryBuilder) buildPkExprFromNode(nodeID int32, pkType plan.Type, pkName string) *plan.Expr {
	if builder == nil || nodeID < 0 {
		return nil
	}
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.TableDef == nil || len(node.BindingTags) == 0 {
			return nil
		}
		colIdx, ok := node.TableDef.Name2ColIndex[pkName]
		if !ok {
			colIdx = node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
		}
		return &plan.Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: node.BindingTags[0],
					ColPos: colIdx,
					Name:   pkName,
				},
			},
		}
	case plan.Node_PROJECT:
		for _, expr := range node.ProjectList {
			if col := expr.GetCol(); col != nil {
				if builder.getColName(col) == pkName {
					return DeepCopyExpr(expr)
				}
			}
		}
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	case plan.Node_JOIN:
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	default:
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	}
	return nil
}

func (builder *QueryBuilder) getColName(col *plan.ColRef) string {
	if col == nil {
		return ""
	}
	if builder == nil || builder.nameByColRef == nil {
		return col.Name
	}
	if name := builder.nameByColRef[[2]int32{col.RelPos, col.ColPos}]; name != "" {
		return name
	}
	return col.Name
}

func (builder *QueryBuilder) rebindScanNode(scanNode *plan.Node) {
	if scanNode == nil || len(scanNode.BindingTags) == 0 {
		return
	}
	oldTag := scanNode.BindingTags[0]
	newTag := builder.genNewBindTag()
	scanNode.BindingTags[0] = newTag
	builder.addNameByColRef(newTag, scanNode.TableDef)
	for _, expr := range scanNode.FilterList {
		replaceColRefTag(expr, oldTag, newTag)
	}
	// Also update BlockFilterList, which was copied from the original scanNode
	// and still contains references to the old binding tag
	for _, expr := range scanNode.BlockFilterList {
		replaceColRefTag(expr, oldTag, newTag)
	}
}

func replaceColRefTag(expr *plan.Expr, oldTag, newTag int32) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos == oldTag {
			impl.Col.RelPos = newTag
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			replaceColRefTag(arg, oldTag, newTag)
		}
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			replaceColRefTag(sub, oldTag, newTag)
		}
	}
}

func (builder *QueryBuilder) canApplyRegularIndex(node *plan.Node) bool {
	if node == nil || node.TableDef == nil {
		return false
	}
	colCnt := len(node.TableDef.Cols)
	if colCnt == 0 {
		return false
	}
	for _, expr := range node.FilterList {
		if !colRefsWithin(expr, colCnt) {
			return false
		}
	}
	return len(node.FilterList) > 0
}

func colRefsWithin(expr *plan.Expr, colCnt int) bool {
	if expr == nil {
		return true
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return int(impl.Col.ColPos) < colCnt
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if !colRefsWithin(arg, colCnt) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if !colRefsWithin(sub, colCnt) {
				return false
			}
		}
		return true
	default:
		return true
	}
}
