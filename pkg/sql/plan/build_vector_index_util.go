// Copyright 2023 Matrix Origin
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
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	bigIntType = types.T_int64.ToType()
	doubleType = types.T_float64.ToType()
	//varCharType = types.T_varchar.ToType()
	//
	//opTypeToDistanceFunc = map[string]string{
	//	"vector_l2_ops":     "l2_distance",
	//	"vector_ip_ops":     "inner_product",
	//	"vector_cosine_ops": "cosine_distance",
	//}
)

func makeIvfFlatIndexTblScan(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTableId int32) (int32, []*Expr) {
	scanNodeProjections := make([]*Expr, len(indexTableDefs[idxTableId].Cols))
	for colIdx, column := range indexTableDefs[idxTableId].Cols {
		scanNodeProjections[colIdx] = &plan.Expr{
			Typ: column.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(colIdx),
					Name:   column.Name,
				},
			},
		}
	}
	centroidsScanId := builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		ObjRef:      idxRefs[idxTableId],
		TableDef:    indexTableDefs[idxTableId],
		ProjectList: scanNodeProjections,
	}, bindCtx)
	return centroidsScanId, scanNodeProjections
}

func makeMetaTblScanWhereKeyEqVersion(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef) (int32, error) {
	metaTableScanId, scanCols := makeIvfFlatIndexTblScan(builder, bindCtx, indexTableDefs, idxRefs, 0)

	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		DeepCopyExpr(scanCols[0]),
		MakePlan2StringConstExprWithType("version"),
	})
	if err != nil {
		return -1, err
	}
	builder.qry.Nodes[metaTableScanId].FilterList = []*Expr{whereKeyEqVersion}
	return metaTableScanId, nil
}

func makeCrossJoinCentroidsMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, metaTableScanId int32) (int32, error) {
	centroidsScanId, _ := makeIvfFlatIndexTblScan(builder, bindCtx, indexTableDefs, idxRefs, 1)

	metaProjection := getProjectionByLastNode(builder, metaTableScanId)
	metaProjectValueCol := DeepCopyExpr(metaProjection[1])
	metaProjectValueCol.Expr.(*plan.Expr_Col).Col.RelPos = 1
	prevMetaScanCastValAsBigInt, err := makePlan2CastExpr(builder.GetContext(), metaProjectValueCol, makePlan2Type(&bigIntType))
	if err != nil {
		return -1, err
	}
	// 0: centroids.version
	// 1: centroids.centroid_id
	// 2: centroids.centroid
	prevCentroidScanProjection := getProjectionByLastNode(builder, centroidsScanId)[:3]
	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		prevCentroidScanProjection[0],
		prevMetaScanCastValAsBigInt,
	})
	if err != nil {
		return -1, err
	}

	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_INNER,
		Children:    []int32{centroidsScanId, metaTableScanId},
		ProjectList: prevCentroidScanProjection,
		OnList:      []*Expr{whereCentroidVersionEqCurrVersion},
	}, bindCtx)

	return joinMetaAndCentroidsId, nil
}

func makeTblCrossJoinL2Centroids(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, lastNodeId int32, currVersionCentroids int32, typeOriginPk Type, posOriginPk int, typeOriginVecColumn Type, posOriginVecColumn int, includeSourceCols []ivfIncludeSourceCol, optype string) int32 {
	projectList := []*Expr{
		{ // centroids.version
			Typ: makePlan2TypeValue(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 1,
					ColPos: 0,
					Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
				},
			},
		},
		{ // centroids.centroid_id
			Typ: makePlan2TypeValue(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 1,
					ColPos: 1,
					Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
				},
			},
		},
		{ // tbl.pk
			Typ: *DeepCopyType(&typeOriginPk),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(posOriginPk),
					Name:   tableDef.Cols[posOriginPk].Name,
				},
			},
		},
		{ // tbl.embedding
			Typ: *DeepCopyType(&typeOriginVecColumn),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(posOriginVecColumn),
					Name:   tableDef.Cols[posOriginVecColumn].Name,
				},
			},
		},
	}
	for _, includeCol := range includeSourceCols {
		projectList = append(projectList, &plan.Expr{
			Typ: *DeepCopyType(&includeCol.typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(includeCol.pos),
					Name:   includeCol.name,
				},
			},
		})
	}

	joinTblAndCentroidsUsingCrossL2Join := builder.appendNode(&plan.Node{
		NodeType:     plan.Node_JOIN,
		JoinType:     plan.Node_L2,
		ExtraOptions: optype,
		Children:     []int32{lastNodeId, currVersionCentroids},
		ProjectList:  projectList,
		OnList: []*Expr{
			{ // centroids.centroid
				Typ: *DeepCopyType(&typeOriginVecColumn),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 2,
						Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
					},
				},
			},
			{ // tbl.embedding
				Typ: *DeepCopyType(&typeOriginVecColumn),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(posOriginVecColumn),
						Name:   tableDef.Cols[posOriginVecColumn].Name,
					},
				},
			},
		},
	}, bindCtx)
	return joinTblAndCentroidsUsingCrossL2Join
}

func makeFinalProject(builder *QueryBuilder, bindCtx *BindContext, joinTblAndCentroidsUsingCrossL2Join int32) (int32, error) {
	var finalProjections = getProjectionByLastNode(builder, joinTblAndCentroidsUsingCrossL2Join)

	cpKey, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{
		DeepCopyExpr(finalProjections[0]),
		DeepCopyExpr(finalProjections[1]),
		DeepCopyExpr(finalProjections[2]),
	})
	if err != nil {
		return -1, err
	}

	projectList := make([]*Expr, 0, len(finalProjections)+1)
	for _, projection := range finalProjections {
		projectList = append(projectList, DeepCopyExpr(projection))
	}
	projectList = append(projectList, cpKey)

	projectWithCpKey := builder.appendNode(
		&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{joinTblAndCentroidsUsingCrossL2Join},
			ProjectList: projectList,
		},
		bindCtx)
	return projectWithCpKey, nil
}

// ivfEntryProjPos is the position of the entry vector inside the projection built
// by makeTblCrossJoinL2Centroids: (centroids.version, centroids.centroid_id,
// tbl.pk, tbl.<vector>, <include cols...>). makeFinalProject depends on the same
// layout for its serial() key over positions 0..2.
const ivfEntryProjPos = 3

// ivfEntriesEntryColType returns the declared type of the entries table's entry
// column. The entries table always has one; a missing column means the catalog
// entry is not an ivfflat entries table.
func ivfEntriesEntryColType(entriesTableDef *TableDef) (Type, bool) {
	for _, col := range entriesTableDef.Cols {
		if col.Name == catalog.SystemSI_IVFFLAT_TblCol_Entries_entry {
			return col.Typ, true
		}
	}
	return Type{}, false
}

// makeIvfQuantizeBoundsAgg builds a node yielding exactly one row with the trained
// scalar-quantizer bounds (metadata keys quantize_min / quantize_max) as two DOUBLE
// columns.
//
// The pivot is MAX(CASE key WHEN <k> THEN CAST(val AS DOUBLE) END) over the whole
// metadata table rather than two key-filtered scans, because an aggregate with no
// GROUP BY produces one row even when neither key is present. A key-filtered scan
// would produce zero rows for an index built before the quantizer trained bounds,
// and the cross join in makeIvfEntriesQuantizeProject would then silently drop
// every entry row instead of falling back to identity.
func makeIvfQuantizeBoundsAgg(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef) (int32, error) {
	metaScanId, metaCols := makeIvfFlatIndexTblScan(builder, bindCtx, indexTableDefs, idxRefs, 0)

	c := &exprChain{ctx: builder.GetContext()}
	doubleTyp := makePlan2Type(&doubleType)
	aggList := make([]*Expr, 0, 2)
	for _, key := range []string{
		catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin,
		catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax,
	} {
		// MAX(CASE key WHEN <k> THEN CAST(val AS DOUBLE) END)
		keyEq := c.bind("=", DeepCopyExpr(metaCols[0]), MakePlan2StringConstExprWithType(key))
		picked := c.bind("case", keyEq, c.cast(DeepCopyExpr(metaCols[1]), doubleTyp),
			makePlan2NullConstExprWithType())
		aggList = append(aggList, c.bind("max", picked))
	}
	if c.err != nil {
		return -1, c.err
	}

	aggProjection := make([]*Expr, len(aggList))
	for i, agg := range aggList {
		aggProjection[i] = &plan.Expr{
			Typ: agg.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -2,
					ColPos: int32(i),
				},
			},
		}
	}

	return builder.appendNode(&plan.Node{
		NodeType:    plan.Node_AGG,
		Children:    []int32{metaScanId},
		AggList:     aggList,
		ProjectList: aggProjection,
		SpillMem:    builder.aggSpillMem,
	}, bindCtx), nil
}

// ivfEntryRewritePlan decides how a DML-projected entry must be transformed to match
// the entries column.
//
// rewrite is false only when the projected base value is already a valid entry, which
// is the case for the float formats (float16/bf16/float32) when the entry type equals
// the base type: quantizer.CastSQL narrows, and narrowing to the same type is a no-op.
//
// affine marks int8/uint8, where the entry carries the trained scalar quantizer
// q(x)=x*mul+add. The build path applies that map for EVERY base type -- including an
// already-int8 base, where the quantizer rescales the value instead of narrowing it --
// so equal types are NOT a no-op there. Treating them as one lets DML write raw values
// into an index whose build rows are scaled: mixed encodings, a silently wrong ranking
// and no error at all.
func ivfEntryRewritePlan(entryTyp, baseTyp Type) (rewrite, affine bool) {
	entryT := types.T(entryTyp.Id)
	affine = entryT == types.T_array_int8 || entryT == types.T_array_uint8
	return affine || entryTyp.Id != baseTyp.Id, affine
}

// makeIvfEntriesQuantizeProject rewrites the projected entry vector into the entries
// table's declared element type, applying the same scalar quantizer the build path
// uses.
//
// CREATE INDEX ... QUANTIZATION 'int8' declares the entry column as vecint8 while the
// base column stays wide. Both other writers of this table quantize on the way in --
// the build path via quantizer.Int8EntrySQL (ivfflat/plugin/compile) and the CDC path
// via quantizer.Int8EntrySQLFromBounds (iscp/index_sqlwriter). The synchronous DML
// plan did not, so an INSERT or UPDATE stored the base value verbatim: the 16 raw
// bytes of a vecf32(4) read back as a 16-element vecint8, and the next search failed
// with "vector dimension not matched" (#27732).
//
// Only the projected ENTRY is rewritten. The CENTROIDX join key deliberately stays in
// the base domain, because centroids are stored as float32 whenever quantization is
// set (see the ivfflat plan schema), so the assignment must be computed on the wide
// value.
//
// The arithmetic mirrors quantizer.Int8EntrySQLFromBounds exactly -- the same
// 255.0/(max-min) grouping and the same inner cast to vecf32 -- because a DML row
// that rounds differently from a build row lands in a different quantization bucket
// and silently changes which neighbours a search returns.
func makeIvfEntriesQuantizeProject(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, lastNodeId int32,
	typeOriginVecColumn Type) (int32, error) {
	entryTyp, ok := ivfEntriesEntryColType(indexTableDefs[2])
	if !ok {
		return -1, moerr.NewInternalErrorf(builder.GetContext(),
			"ivfflat entries table has no %q column", catalog.SystemSI_IVFFLAT_TblCol_Entries_entry)
	}
	rewrite, affine := ivfEntryRewritePlan(entryTyp, typeOriginVecColumn)
	if !rewrite {
		return lastNodeId, nil
	}
	if affine {
		var err error
		if lastNodeId, err = appendIvfQuantizeBoundsJoin(builder, bindCtx, indexTableDefs, idxRefs, lastNodeId); err != nil {
			return -1, err
		}
	}

	projection := getProjectionByLastNode(builder, lastNodeId)
	// The bounds join appended qmin/qmax past the entries columns. They feed the
	// entry expression and are trimmed again below: only the entries table's own
	// columns may reach makeFinalProject.
	entryCols := len(projection)
	var qmin, qmax *Expr
	if affine {
		entryCols -= 2
		qmin, qmax = projection[entryCols], projection[entryCols+1]
	}

	entryExpr, err := makeIvfQuantizedEntryExpr(builder, projection[ivfEntryProjPos], entryTyp,
		qmin, qmax, types.T(entryTyp.Id) == types.T_array_uint8)
	if err != nil {
		return -1, err
	}
	projection[ivfEntryProjPos] = entryExpr

	return builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeId},
		ProjectList: projection[:entryCols],
	}, bindCtx), nil
}

// appendIvfQuantizeBoundsJoin cross-joins the single row of trained quantizer bounds
// onto lastNodeId, appending qmin and qmax to its projection. The right side is an
// aggregate with no GROUP BY, so it contributes exactly one row and can never drop an
// entry row.
func appendIvfQuantizeBoundsJoin(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, lastNodeId int32) (int32, error) {
	boundsId, err := makeIvfQuantizeBoundsAgg(builder, bindCtx, indexTableDefs, idxRefs)
	if err != nil {
		return -1, err
	}
	projection := getProjectionByLastNode(builder, lastNodeId)
	bounds := getProjectionByLastNode(builder, boundsId)
	for _, e := range bounds {
		e.Expr.(*plan.Expr_Col).Col.RelPos = 1
	}
	return builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_INNER,
		Children:    []int32{lastNodeId, boundsId},
		ProjectList: append(projection, bounds...),
	}, bindCtx), nil
}

// makeIvfQuantizedEntryExpr builds the entry projection for a narrowed entries column.
//
// qmin/qmax are nil for the float formats (float16/bf16/float32), which narrow with a
// plain cast -- the same choice quantizer.CastSQL makes on the build path. For int8
// and uint8 they carry the trained bounds and the expression becomes
// cast(cast(base as vecf32(dim)) * mul + add as vec<t>(dim)), with
//
//	mul = COALESCE(255.0 / (max-min), 1.0)
//	add = COALESCE(0.0 - min * (255.0/(max-min)) - 128.0, 0.0)   // int8
//	add = COALESCE(0.0 - min * (255.0/(max-min)), 0.0)           // uint8, no -128 shift
//
// The COALESCE fallback to identity (1,0) covers a degenerate or absent range -- a
// zero range divides to NULL, and an index built before the quantizer trained bounds
// has no metadata rows at all. Search applies the same identity fallback when the
// bounds are missing, so the two stay consistent.
// exprChain binds a sequence of expressions, remembering the FIRST error so a formula
// can be written as a formula. Every call after a failure is a no-op returning nil, and
// the caller checks once at the end.
//
// The alternative is an `if err != nil { return }` after each of a dozen binds, which
// buries the arithmetic it is supposed to protect and leaves a dozen branches that
// cannot be reached with valid input.
type exprChain struct {
	ctx context.Context
	err error
}

func (c *exprChain) bind(op string, args ...*Expr) *Expr {
	if c.err != nil {
		return nil
	}
	e, err := BindFuncExprImplByPlanExpr(c.ctx, op, args)
	if err != nil {
		c.err = err
		return nil
	}
	return e
}

func (c *exprChain) cast(e *Expr, typ Type) *Expr {
	if c.err != nil {
		return nil
	}
	out, err := makePlan2CastExpr(c.ctx, e, typ)
	if err != nil {
		c.err = err
		return nil
	}
	return out
}

func makeIvfQuantizedEntryExpr(builder *QueryBuilder, baseExpr *Expr, entryTyp Type,
	qmin, qmax *Expr, isUint8 bool) (*Expr, error) {
	c := &exprChain{ctx: builder.GetContext()}
	dim := entryTyp.Width
	lit := makePlan2Float64ConstExprWithType

	// Pin the affine map to float32 for both a vecf32 and a vecf64 base, so a DML row
	// sees the same two float32 roundings as the build path and the query encoder.
	narrowed := c.cast(baseExpr, plan.Type{Id: int32(types.T_array_float32), Width: dim})
	if qmin == nil {
		return c.result(c.cast(narrowed, entryTyp))
	}

	//	mul = COALESCE(255.0 / (max-min), 1.0)
	//	add = COALESCE(0.0 - min*(255.0/(max-min)) [- 128.0], 0.0)
	//
	// `0.0 - min * (255.0/rng)` keeps the divide-then-multiply order of the build
	// side's add = -min*mul before the float32 coercion, and the inner scale is the
	// UN-coalesced division, exactly as quantizer.Int8EntrySQLFromBounds writes it.
	rng := c.bind("-", qmax, DeepCopyExpr(qmin))
	scale := c.bind("/", lit(255.0), rng)
	mul := c.bind("coalesce", DeepCopyExpr(scale), lit(1.0))

	addRaw := c.bind("-", lit(0.0), c.bind("*", DeepCopyExpr(qmin), DeepCopyExpr(scale)))
	if !isUint8 {
		addRaw = c.bind("-", addRaw, lit(128.0)) // int8 also shifts onto [-128,127]
	}
	add := c.bind("coalesce", addRaw, lit(0.0))

	return c.result(c.cast(c.bind("+", c.bind("*", narrowed, mul), add), entryTyp))
}

// result returns e unless the chain failed, so a builder can end in one statement.
func (c *exprChain) result(e *Expr) (*Expr, error) {
	if c.err != nil {
		return nil, c.err
	}
	return e, nil
}
