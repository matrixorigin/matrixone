// Copyright 2022 Matrix Origin
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

package readutil

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type FastFilterOp func(*objectio.ObjectStats) (bool, error)
type LoadOp = func(
	context.Context, *objectio.ObjectStats, objectio.ObjectMeta, objectio.BloomFilter,
) (objectio.ObjectMeta, objectio.BloomFilter, error)
type ObjectFilterOp func(objectio.ObjectMeta, objectio.BloomFilter) (bool, error)
type SeekFirstBlockOp func(objectio.ObjectDataMeta) int
type BlockFilterOp func(int, objectio.BlockObject, objectio.BloomFilter) (bool, bool, error)
type LoadOpFactory func(fileservice.FileService) LoadOp

var loadMetadataOnlyOpFactory LoadOpFactory
var loadMetadataAndBFOpFactory LoadOpFactory

func init() {
	loadMetadataAndBFOpFactory = func(fs fileservice.FileService) LoadOp {
		return func(
			ctx context.Context,
			obj *objectio.ObjectStats,
			inMeta objectio.ObjectMeta,
			inBF objectio.BloomFilter,
		) (outMeta objectio.ObjectMeta, outBF objectio.BloomFilter, err error) {
			location := obj.ObjectLocation()
			outMeta = inMeta
			if outMeta == nil {
				if outMeta, err = objectio.FastLoadObjectMeta(
					ctx, &location, false, fs,
				); err != nil {
					return nil, nil, err
				}
			}
			outBF = inBF
			if outBF == nil {
				meta := outMeta.MustDataMeta()
				if outBF, err = objectio.LoadBFWithMeta(
					ctx, meta, location, fs,
				); err != nil {
					return nil, nil, err
				}
			}
			return outMeta, outBF, nil
		}
	}
	loadMetadataOnlyOpFactory = func(fs fileservice.FileService) LoadOp {
		return func(
			ctx context.Context,
			obj *objectio.ObjectStats,
			inMeta objectio.ObjectMeta,
			inBF objectio.BloomFilter,
		) (outMeta objectio.ObjectMeta, outBF objectio.BloomFilter, err error) {
			outMeta = inMeta
			outBF = inBF
			if outMeta != nil {
				return
			}
			location := obj.ObjectLocation()
			if outMeta, err = objectio.FastLoadObjectMeta(
				ctx, &location, false, fs,
			); err != nil {
				return nil, nil, err
			}
			return outMeta, outBF, nil
		}
	}
}

func isSortedKey(colDef *plan.ColDef) (isPK, isSorted bool) {
	if colDef.Name == catalog.FakePrimaryKeyColName {
		return false, false
	}
	isPK, isCluster := colDef.Primary, colDef.ClusterBy
	isSorted = isPK || isCluster
	return
}

// makeDecimalZoneMapBound preserves the bound's own scale for pruning. Folded
// constants carry correctly encoded bytes and type metadata, but raw-byte ZM
// helpers assume that the bytes already use the persisted ZM's scale. That
// assumption is not valid for comparisons such as DECIMAL(20,4) < DECIMAL(38,0).
func makeDecimalZoneMapBound(colDef *plan.ColDef, value []byte, valueExpr *plan.Expr) (objectio.ZoneMap, bool) {
	columnType := types.T(colDef.Typ.Id)
	if !columnType.IsDecimal() {
		return nil, true
	}
	if valueExpr == nil || types.T(valueExpr.Typ.Id) != columnType ||
		columnType == types.T_decimal256 || len(value) != columnType.FixedLength() {
		return nil, false
	}
	bound := index.NewZM(columnType, valueExpr.Typ.Scale)
	index.UpdateZM(bound, value)
	return bound, true
}

func makeVectorValueZoneMapBound(
	columnType types.T, vec *vector.Vector, value []byte,
) (objectio.ZoneMap, bool) {
	if vec == nil || vec.GetType().Oid != columnType {
		return nil, false
	}
	if !columnType.IsDecimal() {
		return nil, true
	}
	if columnType == types.T_decimal256 || len(value) != columnType.FixedLength() {
		return nil, false
	}
	bound := index.NewZM(columnType, vec.GetType().Scale)
	index.UpdateZM(bound, value)
	return bound, true
}

// When requireKnown is set, -1 means that ordered skipping was not proved.
func seekFirstBlockByZoneMap(
	meta objectio.ObjectDataMeta,
	seqNum uint16,
	bound objectio.ZoneMap,
	columnType types.T,
	requireKnown bool,
	compare func(objectio.ZoneMap) index.ZoneMapMatch,
) int {
	blockCnt := int(meta.BlockCount())
	if blockCnt == 0 || !zoneMapMetadataComparable(meta.MustGetColumn(seqNum).ZoneMap(), bound, columnType) {
		if requireKnown {
			return -1
		}
		return 0
	}
	for j := range blockCnt {
		zm := meta.GetBlockMeta(uint32(j)).MustGetColumn(seqNum).ZoneMap()
		if !zoneMapMetadataComparable(zm, bound, columnType) || requireKnown && !compare(zm).Known {
			if requireKnown {
				return -1
			}
			return 0
		}
	}
	return sort.Search(blockCnt, func(j int) bool {
		return compare(meta.GetBlockMeta(uint32(j)).MustGetColumn(seqNum).ZoneMap()).Match
	})
}

func zoneMapMetadataComparable(
	zm objectio.ZoneMap, bound objectio.ZoneMap, columnType types.T,
) bool {
	if !index.RawZoneMapComparable(zm, columnType) {
		return false
	}
	return bound == nil || index.RawZoneMapComparable(bound, columnType)
}

// compileScalarComparisonFilter owns scalar comparison pruning for both raw
// and converted domains. Only raw ordered comparisons may seek, stop early or
// consult a bloom filter; a conversion's tri-state matcher proves pruning alone.
func compileScalarComparisonFilter(
	expr *plan.Expr, exprImpl *plan.Expr_F, tableDef *plan.TableDef,
	fs fileservice.FileService, zone *time.Location,
) (fastFilterOp FastFilterOp, loadOp LoadOp, objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp, seekOp SeekFirstBlockOp, canCompile bool,
	highSelectivityHint bool, handled bool) {
	name := exprImpl.F.Func.ObjName
	op, binary := types.ParseComparisonOp(name)
	if !binary && name != "between" {
		return
	}
	args := exprImpl.F.Args
	if binary && len(args) != 2 || !binary && len(args) != 3 {
		return nil, nil, nil, nil, nil, false, false, true
	}
	for _, arg := range args {
		if arg == nil {
			return nil, nil, nil, nil, nil, false, false, true
		}
	}
	column := args[0]
	values := args[1:]
	if column.GetCol() == nil {
		if !binary || args[1].GetCol() == nil {
			return nil, nil, nil, nil, nil, false, false, true
		}
		column, values, op = args[1], args[:1], op.Reverse()
	}
	ref := column.GetCol()
	colDef := getColDefByName(expr, ref.Name, ref.ColPos, tableDef)
	if colDef == nil {
		return nil, nil, nil, nil, nil, false, false, true
	}
	mixed := false
	for _, value := range values {
		mixed = mixed || !index.RawScalarZoneMapTypesCompatible(types.T(colDef.Typ.Id), types.T(value.Typ.Id))
	}
	// The native BETWEEN owner has its own byte truncation and candidate-seek
	// guarantees. Converted bounds instead use the SQL comparison conjunction.
	if !binary && !mixed {
		return
	}
	vals, ok := getConstBytesFromExpr(values)
	if !ok {
		return nil, nil, nil, nil, nil, false, false, true
	}
	columnType := types.New(types.T(colDef.Typ.Id), colDef.Typ.Width, colDef.Typ.Scale)
	columnType.Charset = uint8(colDef.Typ.Charset)
	var matchers [2]func(objectio.ZoneMap) index.ZoneMapMatch
	var bound objectio.ZoneMap
	for i, value := range values {
		comparison := op
		if !binary {
			comparison = types.ComparisonGreaterEqual
			if i == 1 {
				comparison = types.ComparisonLessEqual
			}
		}
		scale := value.Typ.Scale
		if columnType.Oid == types.T_timestamp {
			scale = columnType.Scale
		} else if !binary && types.T(values[0].Typ.Id) == types.T_timestamp && types.T(values[1].Typ.Id) == types.T_timestamp {
			scale = values[0].Typ.Scale
		}
		valueType := types.New(types.T(value.Typ.Id), value.Typ.Width, value.Typ.Scale)
		valueType.Charset = uint8(value.Typ.Charset)
		var scalarBound objectio.ZoneMap
		matchers[i], scalarBound, ok = index.MakeScalarZoneMapMatcher(columnType, vals[i], valueType, scale, zone, comparison)
		if !ok {
			return nil, nil, nil, nil, nil, false, false, true
		}
		if i == 0 {
			bound = scalarBound
		}
	}
	match := matchers[0]
	if !binary {
		lower, upper := matchers[0], matchers[1]
		match = func(zm objectio.ZoneMap) index.ZoneMapMatch { return lower(zm).And(upper(zm)) }
	}
	isPK, sorted := isSortedKey(colDef)
	if sorted {
		fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
			if obj.ZMIsEmpty() {
				return true, nil
			}
			return match(obj.SortKeyZoneMap()).MayMatch(), nil
		}
	}
	rawEqual := !mixed && binary && op == types.ComparisonEqual
	if rawEqual && isPK {
		loadOp = loadMetadataAndBFOpFactory(fs)
	} else {
		loadOp = loadMetadataOnlyOpFactory(fs)
	}
	highSelectivityHint = isPK && (mixed || rawEqual)
	seqNum := uint16(colDef.Seqnum)
	objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
		if sorted {
			return true, nil
		}
		return match(meta.MustDataMeta().MustGetColumn(seqNum).ZoneMap()).MayMatch(), nil
	}
	if mixed || op == types.ComparisonGreater || op == types.ComparisonGreaterEqual || !sorted && !(rawEqual && isPK) {
		blockFilterOp = func(_ int, blkMeta objectio.BlockObject, _ objectio.BloomFilter) (bool, bool, error) {
			return false, match(blkMeta.MustGetColumn(seqNum).ZoneMap()).MayMatch(), nil
		}
	} else {
		blockFilterOp = func(blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter) (bool, bool, error) {
			zm := blkMeta.MustGetColumn(seqNum).ZoneMap()
			result := match(zm)
			if sorted {
				if op == types.ComparisonLess || op == types.ComparisonLessEqual {
					return result.Excludes(), result.MayMatch(), nil
				}
				if rawEqual && index.AnyLEByBound(zm, vals[0], bound, columnType.Oid).Excludes() {
					return true, false, nil
				}
			}
			if !result.MayMatch() {
				return false, false, nil
			}
			if rawEqual && isPK && result.Known && (bound == nil || bound.GetType() == zm.GetType() && bound.GetScale() == zm.GetScale()) {
				var bloom index.BloomFilter
				if err := bloom.Unmarshal(bf.GetBloomFilter(uint32(blkIdx))); err != nil {
					return false, false, err
				}
				exists, err := bloom.MayContainsKey(vals[0])
				return false, exists, err
			}
			return false, true, nil
		}
	}
	if !mixed && sorted && (rawEqual || op == types.ComparisonGreater || op == types.ComparisonGreaterEqual) {
		seekMatch := match
		if rawEqual {
			seekMatch = func(zm objectio.ZoneMap) index.ZoneMapMatch {
				return index.AnyGEByBound(zm, vals[0], bound, columnType.Oid)
			}
		}
		seekOp = func(meta objectio.ObjectDataMeta) int {
			return seekFirstBlockByZoneMap(meta, seqNum, bound, columnType.Oid, false, seekMatch)
		}
	}
	return fastFilterOp, loadOp, objectFilterOp, blockFilterOp, seekOp, true, highSelectivityHint, true
}

func CompileFilterExprs(
	exprs []*plan.Expr,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	canCompile bool,
	highSelectivityHint bool,
) {
	return compileFilterExprs(exprs, tableDef, fs, nil)
}

func compileFilterExprs(
	exprs []*plan.Expr,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
	zone *time.Location,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	canCompile bool,
	highSelectivityHint bool,
) {
	canCompile = true
	if len(exprs) == 0 {
		return
	}
	if len(exprs) == 1 {
		return compileFilterExpr(exprs[0], tableDef, fs, zone)
	}
	ops1 := make([]FastFilterOp, 0, len(exprs))
	ops2 := make([]LoadOp, 0, len(exprs))
	ops3 := make([]ObjectFilterOp, 0, len(exprs))
	ops4 := make([]BlockFilterOp, 0, len(exprs))
	ops5 := make([]SeekFirstBlockOp, 0, len(exprs))
	compiled := 0

	for _, expr := range exprs {
		expr_op1, expr_op2, expr_op3, expr_op4, expr_op5, can, hsh := compileFilterExpr(expr, tableDef, fs, zone)
		if !can {
			continue
		}
		compiled++
		if expr_op1 != nil {
			ops1 = append(ops1, expr_op1)
		}
		if expr_op2 != nil {
			ops2 = append(ops2, expr_op2)
		}
		if expr_op3 != nil {
			ops3 = append(ops3, expr_op3)
		}
		if expr_op4 != nil {
			ops4 = append(ops4, expr_op4)
		}
		if expr_op5 != nil {
			ops5 = append(ops5, expr_op5)
		}
		highSelectivityHint = highSelectivityHint || hsh
	}
	if compiled == 0 {
		return nil, nil, nil, nil, nil, false, false
	}
	fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
		for _, op := range ops1 {
			ok, err := op(obj)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	loadOp = func(
		ctx context.Context,
		obj *objectio.ObjectStats,
		inMeta objectio.ObjectMeta,
		inBF objectio.BloomFilter,
	) (meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
		_, _ = inMeta, inBF
		for _, op := range ops2 {
			if meta != nil && bf != nil {
				continue
			}
			if meta, bf, err = op(ctx, obj, meta, bf); err != nil {
				return
			}
		}
		return
	}
	objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
		for _, op := range ops3 {
			ok, err := op(meta, bf)
			if !ok || err != nil {
				return ok, err
			}
		}
		return true, nil
	}
	blockFilterOp = func(
		blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
	) (bool, bool, error) {
		ok := true
		for _, op := range ops4 {
			thisCan, thisOK, err := op(blkIdx, blkMeta, bf)
			if err != nil {
				return false, false, err
			}
			if thisCan {
				return true, false, nil
			}
			ok = ok && thisOK
		}
		return false, ok, nil
	}

	seekOp = func(obj objectio.ObjectDataMeta) int {
		var pos int
		for _, op := range ops5 {
			pos2 := op(obj)
			if pos2 > pos {
				pos = pos2
			}
		}
		return pos
	}
	return
}

func CompileFilterExpr(
	expr *plan.Expr,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	canCompile bool,
	highSelectivityHint bool,
) {
	return compileFilterExpr(expr, tableDef, fs, nil)
}

func compileFilterExpr(
	expr *plan.Expr,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
	zone *time.Location,
) (FastFilterOp, LoadOp, ObjectFilterOp, BlockFilterOp, SeekFirstBlockOp, bool, bool) {
	return compileFilterExprWithProof(expr, tableDef, fs, zone, false)
}

func compileFilterExprWithProof(
	expr *plan.Expr,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
	zone *time.Location,
	candidateFilter bool,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	canCompile bool,
	highSelectivityHint bool,
) {
	canCompile = true
	if expr == nil {
		return
	}
	candidateFilter = candidateFilter || expr.CandidateFilter
	// Zone maps persist the original byte order. A native Unicode collation
	// compares UCA weights instead, so applying a raw byte zone-map bound can
	// discard a matching block before the residual SQL comparator runs. Keep
	// this optional pruning path disabled for expressions that read a Unicode
	// string column until the persisted zone-map domain is versioned as well.
	if exprUsesUnicodeCollationColumn(expr, tableDef) {
		canCompile = false
		return
	}
	switch exprImpl := expr.Expr.(type) {
	// case *plan.Expr_Col:
	case *plan.Expr_F:
		if function.GetZoneMapEvaluation(exprImpl.F) == function.ZoneMapUnsupported {
			canCompile = false
			return
		}
		name := exprImpl.F.Func.ObjName
		if name == "or" && len(exprImpl.F.Args) < 2 {
			return nil, nil, nil, nil, nil, false, false
		}
		if candidateFilter && name != "or" {
			if name == "isnull" || name == "isnotnull" {
				if len(exprImpl.F.Args) != 1 || candidateColumnDefinition(exprImpl.F.Args[0], tableDef) == nil {
					return nil, nil, nil, nil, nil, false, false
				}
			} else if !candidateScalarFilterSafe(expr, tableDef, false) {
				return nil, nil, nil, nil, nil, false, false
			}
		}
		if op1, op2, op3, op4, op5, can, hsh, handled := compileScalarComparisonFilter(
			expr, exprImpl, tableDef, fs, zone,
		); handled {
			return op1, op2, op3, op4, op5, can, hsh
		}

		switch exprImpl.F.Func.ObjName {
		case "or":
			highSelectivityHint = true
			fastOps := make([]FastFilterOp, 0, len(exprImpl.F.Args))
			loadOps := make([]LoadOp, 0, len(exprImpl.F.Args))
			objectOps := make([]ObjectFilterOp, 0, len(exprImpl.F.Args))
			blockOps := make([]BlockFilterOp, 0, len(exprImpl.F.Args))
			seekOps := make([]SeekFirstBlockOp, 0, len(exprImpl.F.Args))

			for idx := range exprImpl.F.Args {
				op1, op2, op3, op4, op5, can, hsh := compileFilterExprWithProof(exprImpl.F.Args[idx], tableDef, fs, zone, candidateFilter)
				if !can {
					return nil, nil, nil, nil, nil, false, false
				}

				fastOps = append(fastOps, op1)
				loadOps = append(loadOps, op2)
				objectOps = append(objectOps, op3)
				blockOps = append(blockOps, op4)
				seekOps = append(seekOps, op5)

				highSelectivityHint = highSelectivityHint && hsh
			}

			fastFilterOp = func(stats *objectio.ObjectStats) (bool, error) {
				unknown := false
				for idx := range fastOps {
					if fastOps[idx] == nil {
						unknown = true
						continue
					}
					if ok, err := fastOps[idx](stats); ok || err != nil {
						return ok, err
					}
				}
				return unknown || !candidateFilter, nil
			}

			loadOp = func(ctx context.Context, stats *objectio.ObjectStats, inMeta objectio.ObjectMeta, inBF objectio.BloomFilter) (
				meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
				for idx := range loadOps {
					if loadOps[idx] == nil {
						continue
					}
					if meta, bf, err = loadOps[idx](ctx, stats, inMeta, inBF); err != nil {
						return
					}
					inMeta = meta
					inBF = bf
				}
				return
			}

			objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
				unknown := false
				for idx := range objectOps {
					if objectOps[idx] == nil {
						unknown = true
						continue
					}

					if ok, err := objectOps[idx](meta, bf); ok || err != nil {
						return ok, err
					}
				}
				return unknown || !candidateFilter, nil
			}

			blockFilterOp = func(blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter) (bool, bool, error) {
				can := true
				ok := false
				for idx := range blockOps {
					if blockOps[idx] == nil {
						if candidateFilter {
							can = false
							ok = true
						}
						continue
					}

					if thisCan, thisOk, err := blockOps[idx](blkIdx, blkMeta, bf); err != nil {
						return false, false, err
					} else {
						ok = ok || thisOk
						can = can && thisCan
					}
				}
				return can, ok, nil
			}

			seekOp = func(meta objectio.ObjectDataMeta) int {
				pos := int(meta.BlockCount())
				for idx := range seekOps {
					if seekOps[idx] == nil {
						return 0
					}
					pp := seekOps[idx](meta)
					pos = min(pos, pp)
				}
				return pos
			}

		case "and":
			highSelectivityHint = true
			fastOps := make([]FastFilterOp, 0, len(exprImpl.F.Args))
			loadOps := make([]LoadOp, 0, len(exprImpl.F.Args))
			objectOps := make([]ObjectFilterOp, 0, len(exprImpl.F.Args))
			blockOps := make([]BlockFilterOp, 0, len(exprImpl.F.Args))
			seekOps := make([]SeekFirstBlockOp, 0, len(exprImpl.F.Args))
			compiled := 0

			for idx := range exprImpl.F.Args {
				op1, op2, op3, op4, op5, can, hsh := compileFilterExprWithProof(exprImpl.F.Args[idx], tableDef, fs, zone, candidateFilter)
				if !can {
					continue
				}
				compiled++

				fastOps = append(fastOps, op1)
				loadOps = append(loadOps, op2)
				objectOps = append(objectOps, op3)
				blockOps = append(blockOps, op4)
				seekOps = append(seekOps, op5)

				highSelectivityHint = highSelectivityHint || hsh
			}
			if compiled == 0 {
				return nil, nil, nil, nil, nil, false, false
			}

			fastFilterOp = func(stats *objectio.ObjectStats) (bool, error) {
				for idx := range fastOps {
					if fastOps[idx] == nil {
						continue
					}
					if ok, err := fastOps[idx](stats); !ok || err != nil {
						return ok, err
					}
				}
				return true, nil
			}

			loadOp = func(ctx context.Context, stats *objectio.ObjectStats, inMeta objectio.ObjectMeta, inBF objectio.BloomFilter) (
				meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
				for idx := range loadOps {
					if loadOps[idx] == nil {
						continue
					}
					if meta, bf, err = loadOps[idx](ctx, stats, inMeta, inBF); err != nil {
						return
					}
					inMeta = meta
					inBF = bf
				}
				return
			}

			objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
				for idx := range objectOps {
					if objectOps[idx] == nil {
						continue
					}

					if ok, err := objectOps[idx](meta, bf); !ok || err != nil {
						return ok, err
					}
				}
				return true, nil
			}

			blockFilterOp = func(blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter) (bool, bool, error) {
				ok := true
				for idx := range blockOps {
					if blockOps[idx] == nil {
						continue
					}

					if thisCan, thisOk, err := blockOps[idx](blkIdx, blkMeta, bf); err != nil {
						return false, false, err
					} else {
						if thisCan {
							return true, false, nil
						}
						ok = ok && thisOk
					}
				}
				return false, ok, nil
			}

			seekOp = func(meta objectio.ObjectDataMeta) int {
				var pos int
				for idx := range seekOps {
					if seekOps[idx] == nil {
						continue
					}
					pp := seekOps[idx](meta)
					pos = max(pos, pp)
				}
				return pos
			}

		case "prefix_eq":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			columnType := types.T(colDef.Typ.Id)
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return index.PrefixEqByValue(obj.SortKeyZoneMap(), vals[0], columnType).MayMatch(), nil
				}
			}
			highSelectivityHint = isPK

			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return index.PrefixEqByValue(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], columnType,
				).MayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && index.AnyPrefixLEByValue(zm, vals[0], columnType).Excludes() {
					return true, false, nil
				}
				return false, index.PrefixEqByValue(zm, vals[0], columnType).MayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), nil, columnType, false, func(zm objectio.ZoneMap) index.ZoneMapMatch {
						return index.AnyPrefixGEByValue(zm, vals[0], columnType)
					})
				}
			}
		case "prefix_between":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			columnType := types.T(colDef.Typ.Id)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return index.PrefixBetweenByValue(obj.SortKeyZoneMap(), vals[0], vals[1], columnType).MayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return index.PrefixBetweenByValue(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], vals[1], columnType,
				).MayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && index.AnyPrefixLEByValue(zm, vals[1], columnType).Excludes() {
					return true, false, nil
				}
				return false, index.PrefixBetweenByValue(zm, vals[0], vals[1], columnType).MayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					pos := seekFirstBlockByZoneMap(meta, uint16(seqNum), nil, columnType, candidateFilter, func(zm objectio.ZoneMap) index.ZoneMapMatch {
						return index.AnyPrefixGEByValue(zm, vals[0], columnType)
					})
					if candidateFilter && pos >= 0 && pos < int(meta.BlockCount()) && index.AnyPrefixLEByValue(
						meta.GetBlockMeta(uint32(pos)).MustGetColumn(uint16(seqNum)).ZoneMap(), vals[1], columnType,
					).Excludes() {
						return int(meta.BlockCount())
					}
					return max(pos, 0)
				}
			}
		case "prefix_in_range":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok || len(vals) < 3 || len(vals[2]) == 0 {
				canCompile = false
				return
			}
			hint := vals[2][0]
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			columnType := types.T(colDef.Typ.Id)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return index.PrefixInRangeByValue(
						obj.SortKeyZoneMap(), vals[0], vals[1], hint, columnType,
					).MayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return index.PrefixInRangeByValue(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], vals[1], hint, columnType,
				).MayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted {
					upperResult := index.AnyPrefixLEByValue(zm, vals[1], columnType)
					if hint == 2 || hint == 3 {
						upperResult = index.AnyPrefixLTByValue(zm, vals[1], columnType)
					}
					if upperResult.Excludes() {
						return true, false, nil
					}
				}
				return false, index.PrefixInRangeByValue(zm, vals[0], vals[1], hint, columnType).MayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), nil, columnType, false, func(zm objectio.ZoneMap) index.ZoneMapMatch {
						if hint == 1 || hint == 3 {
							return index.AnyPrefixGTByValue(zm, vals[0], columnType)
						}
						return index.AnyPrefixGEByValue(zm, vals[0], columnType)
					})
				}
			}
		case "between":
			colExpr, vals, valExprs, ok := mustColConstValueWithTypeFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			lowerBound, ok := makeDecimalZoneMapBound(colDef, vals[0], valExprs[0])
			if !ok {
				canCompile = false
				return
			}
			upperBound, ok := makeDecimalZoneMapBound(colDef, vals[1], valExprs[1])
			if !ok {
				canCompile = false
				return
			}
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return index.AnyBetweenBounds(
						obj.SortKeyZoneMap(), vals[0], vals[1], lowerBound, upperBound, types.T(colDef.Typ.Id),
					).MayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return index.AnyBetweenBounds(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], vals[1], lowerBound, upperBound, types.T(colDef.Typ.Id),
				).MayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				upperResult := index.AnyLEByBound(zm, vals[1], upperBound, types.T(colDef.Typ.Id))
				if isSorted && upperResult.Excludes() {
					return true, false, nil
				}
				return false, index.AnyBetweenBounds(
					zm, vals[0], vals[1], lowerBound, upperBound, types.T(colDef.Typ.Id),
				).MayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					pos := seekFirstBlockByZoneMap(meta, uint16(seqNum), lowerBound, types.T(colDef.Typ.Id), candidateFilter, func(zm objectio.ZoneMap) index.ZoneMapMatch {
						return index.AnyGEByBound(zm, vals[0], lowerBound, types.T(colDef.Typ.Id))
					})
					if candidateFilter && pos >= 0 && pos < int(meta.BlockCount()) && index.AnyLEByBound(
						meta.GetBlockMeta(uint32(pos)).MustGetColumn(uint16(seqNum)).ZoneMap(), vals[1], upperBound, types.T(colDef.Typ.Id),
					).Excludes() {
						return int(meta.BlockCount())
					}
					return max(pos, 0)
				}
			}
		case "in_range":
			colExpr, vals, valExprs, ok := mustColConstValueWithTypeFromBinaryFuncExpr(exprImpl)
			if !ok || len(vals) < 3 || len(vals[2]) == 0 {
				canCompile = false
				return
			}
			hint := vals[2][0]
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			lowerBound, ok := makeDecimalZoneMapBound(colDef, vals[0], valExprs[0])
			if !ok {
				canCompile = false
				return
			}
			upperBound, ok := makeDecimalZoneMapBound(colDef, vals[1], valExprs[1])
			if !ok {
				canCompile = false
				return
			}
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return index.InRangeBounds(
						obj.SortKeyZoneMap(), vals[0], vals[1], lowerBound, upperBound, hint, types.T(colDef.Typ.Id),
					).MayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return index.InRangeBounds(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], vals[1], lowerBound, upperBound, hint, types.T(colDef.Typ.Id),
				).MayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted {
					if hint == 2 || hint == 3 {
						// open UB: break when min >= ub
						if index.AnyLTByBound(zm, vals[1], upperBound, types.T(colDef.Typ.Id)).Excludes() {
							return true, false, nil
						}
					} else {
						// closed UB: break when min > ub
						if index.AnyLEByBound(zm, vals[1], upperBound, types.T(colDef.Typ.Id)).Excludes() {
							return true, false, nil
						}
					}
				}
				return false, index.InRangeBounds(
					zm, vals[0], vals[1], lowerBound, upperBound, hint, types.T(colDef.Typ.Id),
				).MayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), lowerBound, types.T(colDef.Typ.Id), false, func(zm objectio.ZoneMap) index.ZoneMapMatch {
						if hint == 1 || hint == 3 {
							return index.AnyGTByBound(zm, vals[0], lowerBound, types.T(colDef.Typ.Id))
						}
						return index.AnyGEByBound(zm, vals[0], lowerBound, types.T(colDef.Typ.Id))
					})
				}
			}
		case "prefix_in":
			colExpr, val, ok := mustColVecValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			vec := vector.NewVec(types.T_any.ToType())
			if err := vec.UnmarshalBinary(val); err != nil {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			columnType := types.T(colDef.Typ.Id)
			if columnType != types.T_varchar || vec.GetType().Oid != types.T_varchar {
				canCompile = false
				return
			}
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return index.PrefixInVector(obj.SortKeyZoneMap(), vec, columnType).MayMatch(), nil
				}
			}
			highSelectivityHint = isPK && vec.Length() <= 10
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return index.PrefixInVector(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vec, columnType,
				).MayMatch(), nil
			}
			var minPrefix, maxPrefix []byte
			if vec.Length() > 0 && !vec.IsConstNull() && !vec.GetNulls().Any() {
				col, area := vector.MustVarlenaRawData(vec)
				minPrefix = col[0].GetByteSlice(area)
				maxPrefix = col[len(col)-1].GetByteSlice(area)
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				if blkMeta.IsEmpty() {
					return false, true, nil
				}
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && maxPrefix != nil && index.AnyPrefixLEByValue(zm, maxPrefix, columnType).Excludes() {
					return true, false, nil
				}
				if index.PrefixInVector(zm, vec, columnType).Excludes() {
					return false, false, nil
				}
				return false, true, nil
			}
			if isSorted && minPrefix != nil {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), nil, columnType, false, func(zm objectio.ZoneMap) index.ZoneMapMatch {
						return index.AnyPrefixGEByValue(zm, minPrefix, columnType)
					})
				}
			}
			// ok
		case "isnull", "is_null", "isnotnull", "is_not_null":
			if len(exprImpl.F.Args) != 1 || exprImpl.F.Args[0].GetCol() == nil {
				canCompile = false
				return
			}
			colExpr := exprImpl.F.Args[0].GetCol()
			colDef := getColDefByName(expr, colExpr.Name, colExpr.ColPos, tableDef)
			fid, _ := function.DecodeOverloadID(exprImpl.F.Func.GetObj())
			matches := func(nulls, rows uint32) bool {
				if fid == function.ISNULL {
					return nulls != 0
				}
				return nulls < rows
			}
			fastFilterOp = nil
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				dataMeta := meta.MustDataMeta()
				return matches(dataMeta.MustGetColumn(uint16(seqNum)).NullCnt(), dataMeta.BlockHeader().Rows()), nil
			}
			blockFilterOp = func(_ int, blkMeta objectio.BlockObject, _ objectio.BloomFilter) (bool, bool, error) {
				return false, matches(blkMeta.MustGetColumn(uint16(seqNum)).NullCnt(), blkMeta.GetRows()), nil
			}

		case "in":
			colExpr, val, ok := mustColVecValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			vec := vector.NewVec(types.T_any.ToType())
			if err := vec.UnmarshalBinary(val); err != nil {
				canCompile = false
				return
			}
			if vec.GetType().Oid == types.T_char {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			columnType := types.T(colDef.Typ.Id)
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return index.AnyInVector(obj.SortKeyZoneMap(), vec, columnType).MayMatch(), nil
				}
			}
			if isPK {
				loadOp = loadMetadataAndBFOpFactory(fs)
			} else {
				loadOp = loadMetadataOnlyOpFactory(fs)
			}

			highSelectivityHint = isPK && vec.Length() <= 10

			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return index.AnyInVector(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vec, columnType,
				).MayMatch(), nil
			}
			vecHasNull := vec.IsConstNull() || vec.GetNulls().Any()
			var minVal, maxVal []byte
			if vec.Length() > 0 && !vecHasNull {
				minVal = vec.GetRawBytesAt(0)
				maxVal = vec.GetRawBytesAt(vec.Length() - 1)
			}
			var minBound, maxBound objectio.ZoneMap
			if minVal != nil {
				minBound, ok = makeVectorValueZoneMapBound(columnType, vec, minVal)
				if !ok {
					canCompile = false
					return
				}
				maxBound, ok = makeVectorValueZoneMapBound(columnType, vec, maxVal)
				if !ok {
					canCompile = false
					return
				}
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && maxVal != nil && index.AnyLEByBound(zm, maxVal, maxBound, columnType).Excludes() {
					return true, false, nil
				}
				membership := index.AnyInVector(zm, vec, columnType)
				if membership.Excludes() {
					return false, false, nil
				}
				if isPK && membership.Known {
					blkBf := bf.GetBloomFilter(uint32(blkIdx))
					blkBfIdx := index.NewEmptyBloomFilter()
					if err := index.DecodeBloomFilter(blkBfIdx, blkBf); err != nil {
						return false, false, err
					}
					lowerBound, upperBound := zm.SubVecIn(vec)
					if exist := blkBfIdx.MayContainsAny(vec, lowerBound, upperBound); !exist {
						return false, false, nil
					}
				}
				return false, true, nil
			}
			if isSorted && minVal != nil {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), minBound, columnType, false, func(zm objectio.ZoneMap) index.ZoneMapMatch {
						return index.AnyGEByBound(zm, minVal, minBound, columnType)
					})
				}
			}
		default:
			canCompile = false
		}
	default:
		canCompile = false
	}
	return
}

func exprUsesUnicodeCollationColumn(expr *plan.Expr, tableDef *plan.TableDef) bool {
	if expr == nil || tableDef == nil {
		return false
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col == nil {
			return false
		}
		name := impl.Col.Name
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		pos, ok := tableDef.Name2ColIndex[name]
		if !ok && impl.Col.ColPos >= 0 && int(impl.Col.ColPos) < len(tableDef.Cols) {
			pos, ok = impl.Col.ColPos, true
		}
		if !ok || pos < 0 || int(pos) >= len(tableDef.Cols) || tableDef.Cols[pos] == nil {
			return false
		}
		return types.IsUnicodeCollation(uint8(tableDef.Cols[pos].Typ.Charset))
	case *plan.Expr_F:
		if impl.F == nil {
			return false
		}
		for _, arg := range impl.F.Args {
			if exprUsesUnicodeCollationColumn(arg, tableDef) {
				return true
			}
		}
	case *plan.Expr_List:
		if impl.List == nil {
			return false
		}
		for _, item := range impl.List.List {
			if exprUsesUnicodeCollationColumn(item, tableDef) {
				return true
			}
		}
	}
	return false
}
