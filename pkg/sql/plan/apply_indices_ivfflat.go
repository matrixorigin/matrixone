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

func (builder *QueryBuilder) applyIndicesForSortUsingIvfflat(nodeID int32, projNode, sortNode, scanNode *plan.Node, multiTableIndex *MultiTableIndex) (int32, error) {

	if len(sortNode.OrderBy) != 1 {
		return nodeID, nil
	}

	var childNode *plan.Node
	sortDirection := sortNode.OrderBy[0].Flag // For the most part, it is ASC
	orderExpr := sortNode.OrderBy[0].Expr
	distFnExpr := orderExpr.GetF()
	if distFnExpr == nil {
		childNode = builder.qry.Nodes[sortNode.Children[0]]
		if childNode.NodeType == plan.Node_PROJECT {
			distFnExpr = childNode.ProjectList[orderExpr.GetCol().ColPos].GetF()
		}

		if distFnExpr == nil {
			return nodeID, nil
		}
	}

	var limit *plan.Expr
	var rankOption *plan.RankOption
	if sortNode.Limit != nil {
		limit = sortNode.Limit
		rankOption = sortNode.RankOption
	} else if scanNode.Limit != nil {
		limit = scanNode.Limit
		rankOption = scanNode.RankOption
	} else if projNode.Limit != nil {
		limit = projNode.Limit
		rankOption = projNode.RankOption
	}
	if limit == nil {
		return nodeID, nil
	}

	ctx := builder.ctxByNode[nodeID]
	metaDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids]
	entriesDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries]

	opTypeAst, err := sonic.Get([]byte(metaDef.IndexAlgoParams), catalog.IndexAlgoParamOpType)
	if err != nil {
		return nodeID, nil
	}
	opType, err := opTypeAst.StrictString()
	if err != nil {
		return nodeID, nil
	}

	origFuncName := distFnExpr.Func.ObjName
	if opType != metric.DistFuncOpTypes[origFuncName] {
		return nodeID, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := scanNode.TableDef.Name2ColIndex[keyPart]
	_, vecLitArg, found := builder.getArgsFromDistFn(distFnExpr, partPos)
	if !found {
		return nodeID, nil
	}

	nThread, err := builder.compCtx.ResolveVariable("ivf_threads_search", true, false)
	if err != nil {
		return nodeID, err
	}

	nProbe := int64(5)
	// Get nprobe from system variable
	nProbeIf, err := builder.compCtx.ResolveVariable("probe_limit", true, false)
	if err != nil {
		return nodeID, err
	}
	if nProbeIf != nil {
		var ok bool
		nProbe, ok = (nProbeIf.(int64))
		if !ok {
			return nodeID, moerr.NewInternalErrorNoCtx("ResolveVariable: probe_limit is not int64")
		}
	}

	pkPos := scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	pkType := scanNode.TableDef.Cols[pkPos].Typ
	partType := scanNode.TableDef.Cols[partPos].Typ
	params := idxDef.IndexAlgoParams

	tblCfgStr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d,
			"entries": "%s", "nprobe" : %d, "pktype" : %d, "pkey" : "%s", "part" : "%s", "parttype" : %d, "orig_func_name": "%s"}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		metaDef.IndexTableName,
		idxDef.IndexTableName,
		nThread.(int64),
		entriesDef.IndexTableName,
		uint(nProbe),
		pkType.Id,
		scanNode.TableDef.Pkey.PkeyColName,
		keyPart,
		partType.Id,
		origFuncName)

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
				Param: []byte(params),
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
			DeepCopyExpr(vecLitArg),
		},
	}
	tableFuncNodeID := builder.appendNode(tableFuncNode, ctx)

	err = builder.addBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_ivf_alias_0")}, ctx)
	if err != nil {
		return 0, err
	}

	// change doc_id type to the primary type here
	tableFuncNode.TableDef.Cols[0].Typ = pkType

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

	pushdownEnabled := false
	if rankOption != nil && rankOption.Mode == "pre" {
		pushdownEnabled = true
	}

	if pushdownEnabled {
		// secondScanNode: copy original scanNode for JOIN(ivf, table)
		secondScanNodeID := builder.copyNode(ctx, scanNode.NodeId)
		secondScanNode := builder.qry.Nodes[secondScanNodeID]

		// second scan is only used for runtime filter & inner join, should not inherit limit/offset from original table.
		// Otherwise BloomFilter will only see the truncated primary key set, causing data loss.
		secondScanNode.Limit = nil
		secondScanNode.Offset = nil

		// Add a PROJECT node above secondScanNode to output only the primary key column
		secondProjectTag := builder.genNewBindTag()
		secondPkExpr := &plan.Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: secondScanNode.BindingTags[0],
					ColPos: pkPos,
					Name:   scanNode.TableDef.Cols[pkPos].Name,
				},
			},
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
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 0, // tf.pkid
					},
				},
			},
			{
				Typ: pkType,
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
			Typ: pkType,
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
			Typ: pkType,
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
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: pkType,
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
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: pkPos,
				},
			},
		}
		probeSpec2 := MakeRuntimeFilter(rfTag2, false, 0, DeepCopyExpr(probeExpr2), false)
		scanNode.RuntimeFilterProbeList = append(scanNode.RuntimeFilterProbeList, probeSpec2)

		// build: placeholder column, HashBuild will generate IN-list based on build side join key's UniqueJoinKeys[0]
		buildExpr2 := &plan.Expr{
			Typ: pkType,
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
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: pkType,
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
			Flag: sortDirection,
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
