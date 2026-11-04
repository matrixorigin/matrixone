// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftplan "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// coldef shall copy index type
var (
	ftIndexColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "doc_id",
			Typ: plan.Type{
				Id:          int32(types.T_any),
				NotNullable: false,
			},
		},
		{
			Name: "score",
			Typ: plan.Type{
				Id:          int32(types.T_float32),
				NotNullable: false,
				Width:       4,
			},
		},
	}
)

// buildFullTextSearchScan builds the INDEX_SEARCH_SCAN of a MATCH resolved to the
// classic fulltext index idxdef of scanNode, whose index table is idxObjRef. pattern is
// the MATCH pattern expression, so a prepared-statement '?' parameter is bound and
// evaluated at execution. guard, when not nil, is the zero-relevance guard of a score
// threshold known only at execution. sql is the pre-compiled index-scan SQL of a
// literal pattern, shown by EXPLAIN (Verbose).
func (builder *QueryBuilder) buildFullTextSearchScan(
	ctx *BindContext,
	scanNode *plan.Node,
	idxdef *plan.IndexDef,
	idxObjRef *plan.ObjectRef,
	opts ftplan.ScanOptions,
	pattern, guard *plan.Expr,
	sql string,
) (int32, error) {
	algoOptions, err := ftplan.EncodeScanOptions(opts)
	if err != nil {
		return -1, err
	}
	spec := &plan.IndexSearchScan{
		SourceTable:    DeepCopyObjectRef(scanNode.ObjRef),
		SourceTableDef: DeepCopyTableDef(scanNode.TableDef, true),
		ScanSnapshot:   DeepCopySnapshot(scanNode.ScanSnapshot),
		Index:          DeepCopyIndexDef(idxdef),
		QueryPayload:   DeepCopyExpr(pattern),
		AlgoOptions:    algoOptions,
		HiddenTables: []*plan.IndexHiddenTableRef{
			{Role: catalog.FullTextIndex_TblType, Object: DeepCopyObjectRef(idxObjRef)},
		},
	}
	if guard != nil {
		spec.AlgoExprs = []*plan.Expr{guard}
		spec.AlgoExprNames = []string{fulltext.ZeroRelevanceGuardExpr}
	}
	node := &plan.Node{
		NodeType: plan.Node_INDEX_SEARCH_SCAN,
		Stats:    &plan.Stats{Sql: sql},
		ObjRef:   DeepCopyObjectRef(scanNode.ObjRef),
		TableDef: &plan.TableDef{
			Name:      scanNode.TableDef.Name,
			TableType: "fulltext_index_scan",
			Cols:      DeepCopyColDefList(ftIndexColdefs),
		},
		BindingTags: []int32{builder.genNewBindTag()},
		// Named-snapshot read TS; DeepCopySnapshot(nil) is nil (#27941).
		ScanSnapshot:    DeepCopySnapshot(scanNode.ScanSnapshot),
		IndexSearchScan: spec,
	}
	return builder.appendNode(node, ctx), nil
}

func (builder *QueryBuilder) getFullTextIndexScanSql(params string, idxtbl string, pattern string, mode int64) (string, error) {
	var param fulltext.FullTextParserParam
	if len(params) > 0 {
		err := json.Unmarshal([]byte(params), &param)
		if err != nil {
			return "", err
		}
	}

	ps, err := fulltext.ParsePattern(pattern, mode, param.Parser)
	if err != nil {
		return "", err
	}
	scoreAlgo, err := fulltext.GetScoreAlgo(builder.compCtx.GetProcess())
	if err != nil {
		return "", err
	}
	return fulltext.PatternToSql(ps, mode, idxtbl, param.Parser, scoreAlgo)
}
