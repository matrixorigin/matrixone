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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

type temporalZoneMapMatch struct {
	matches    bool
	comparable bool
}

func (m temporalZoneMapMatch) mayMatch() bool {
	return !m.comparable || m.matches
}

func (m temporalZoneMapMatch) and(other temporalZoneMapMatch) temporalZoneMapMatch {
	if m.comparable && !m.matches || other.comparable && !other.matches {
		return temporalZoneMapMatch{comparable: true}
	}
	if m.comparable && other.comparable {
		return temporalZoneMapMatch{matches: true, comparable: true}
	}
	return temporalZoneMapMatch{}
}

type temporalFilterRange struct {
	min types.Timestamp
	max types.Timestamp
}

func temporalFilterRangeFromZoneMap(
	zm objectio.ZoneMap,
	timestampScale int32,
	zone *time.Location,
) (temporalFilterRange, bool) {
	switch zm.GetType() {
	case types.T_timestamp:
		minValue, minOK := zm.GetMin().(types.Timestamp)
		maxValue, maxOK := zm.GetMax().(types.Timestamp)
		return temporalFilterRange{min: minValue, max: maxValue}, minOK && maxOK
	case types.T_datetime:
		minValue, minOK := zm.GetMin().(types.Datetime)
		maxValue, maxOK := zm.GetMax().(types.Datetime)
		if !minOK || !maxOK {
			return temporalFilterRange{}, false
		}
		if minValue == maxValue {
			value := minValue.ToTimestamp(zone).TruncateToScale(timestampScale)
			return temporalFilterRange{min: value, max: value}, true
		}
		minTimestamp, maxTimestamp, ok := types.DatetimeRangeToTimestampRange(minValue, maxValue, zone)
		if !ok {
			return temporalFilterRange{}, false
		}
		return temporalFilterRange{
			min: minTimestamp.TruncateToScale(timestampScale),
			max: maxTimestamp.TruncateToScale(timestampScale),
		}, true
	default:
		return temporalFilterRange{}, false
	}
}

func temporalFilterRangeFromValue(
	value []byte,
	valueType types.T,
	timestampScale int32,
	zone *time.Location,
) (temporalFilterRange, bool) {
	if len(value) != 8 {
		return temporalFilterRange{}, false
	}
	var timestamp types.Timestamp
	switch valueType {
	case types.T_timestamp:
		timestamp = types.DecodeTimestamp(value)
	case types.T_datetime:
		timestamp = types.DecodeDatetime(value).ToTimestamp(zone).TruncateToScale(timestampScale)
	default:
		return temporalFilterRange{}, false
	}
	return temporalFilterRange{min: timestamp, max: timestamp}, true
}

func temporalFilterMatch(op string, value, bound temporalFilterRange) temporalZoneMapMatch {
	var matches bool
	switch op {
	case "=":
		matches = value.max >= bound.min && value.min <= bound.max
	case "<":
		matches = value.min < bound.max
	case "<=":
		matches = value.min <= bound.max
	case ">":
		matches = value.max > bound.min
	case ">=":
		matches = value.max >= bound.min
	default:
		return temporalZoneMapMatch{}
	}
	return temporalZoneMapMatch{matches: matches, comparable: true}
}

func makeTemporalFilterMatcher(
	columnType types.T,
	value []byte,
	valueType types.T,
	timestampScale int32,
	zone *time.Location,
	op string,
) (func(objectio.ZoneMap) temporalZoneMapMatch, bool) {
	if columnType == valueType {
		return func(zm objectio.ZoneMap) temporalZoneMapMatch {
			if !zm.IsInited() || zm.GetType() != columnType {
				return temporalZoneMapMatch{}
			}
			var matches bool
			switch op {
			case "=":
				matches = zm.ContainsKey(value)
			case "<":
				matches = zm.AnyLTByValue(value)
			case "<=":
				matches = zm.AnyLEByValue(value)
			case ">":
				matches = zm.AnyGTByValue(value)
			case ">=":
				matches = zm.AnyGEByValue(value)
			default:
				return temporalZoneMapMatch{}
			}
			return temporalZoneMapMatch{matches: matches, comparable: true}
		}, true
	}
	if !isMixedTemporalFilterTypes(columnType, valueType) {
		return nil, false
	}

	bound, ok := temporalFilterRangeFromValue(value, valueType, timestampScale, zone)
	if !ok {
		return nil, false
	}
	return func(zm objectio.ZoneMap) temporalZoneMapMatch {
		valueRange, ok := temporalFilterRangeFromZoneMap(zm, timestampScale, zone)
		if !ok {
			return temporalZoneMapMatch{}
		}
		return temporalFilterMatch(op, valueRange, bound)
	}, true
}

func reverseTemporalFilterOperator(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op
	}
}

func isMixedTemporalFilterTypes(typesToCheck ...types.T) bool {
	hasDatetime, hasTimestamp := false, false
	for _, typ := range typesToCheck {
		switch typ {
		case types.T_datetime:
			hasDatetime = true
		case types.T_timestamp:
			hasTimestamp = true
		default:
			return false
		}
	}
	return hasDatetime && hasTimestamp
}

func compileTemporalFilterExpr(
	expr *plan.Expr,
	exprImpl *plan.Expr_F,
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
	handled bool,
) {
	op := exprImpl.F.Func.ObjName
	if op != "=" && op != "<" && op != "<=" && op != ">" && op != ">=" && op != "between" {
		return
	}

	args := exprImpl.F.Args
	var colExpr *plan.Expr_Col
	var columnPlanExpr *plan.Expr
	var valueExprs []*plan.Expr
	columnOnLeft := true
	if op == "between" {
		if len(args) != 3 {
			return
		}
		var ok bool
		if colExpr, ok = args[0].Expr.(*plan.Expr_Col); !ok {
			return
		}
		columnPlanExpr = args[0]
		valueExprs = args[1:]
	} else {
		if len(args) != 2 {
			return
		}
		if col, ok := args[0].Expr.(*plan.Expr_Col); ok {
			colExpr = col
			columnPlanExpr = args[0]
			valueExprs = args[1:]
		} else if col, ok := args[1].Expr.(*plan.Expr_Col); ok {
			colExpr = col
			columnPlanExpr = args[1]
			valueExprs = args[:1]
			columnOnLeft = false
		} else {
			return
		}
	}

	temporalTypes := make([]types.T, 0, len(valueExprs)+1)
	temporalTypes = append(temporalTypes, types.T(columnPlanExpr.Typ.Id))
	for _, valueExpr := range valueExprs {
		temporalTypes = append(temporalTypes, types.T(valueExpr.Typ.Id))
	}
	if !isMixedTemporalFilterTypes(temporalTypes...) {
		return
	}
	if zone == nil {
		return nil, nil, nil, nil, nil, false, false, true
	}

	values, ok := getConstBytesFromExpr(valueExprs)
	if !ok {
		return nil, nil, nil, nil, nil, false, false, true
	}
	colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
	if !columnOnLeft {
		op = reverseTemporalFilterOperator(op)
	}

	matchers := make([]func(objectio.ZoneMap) temporalZoneMapMatch, len(values))
	columnType := types.T(colDef.Typ.Id)
	commonBetweenScale := int32(-1)
	if op == "between" && columnType == types.T_datetime &&
		types.T(valueExprs[0].Typ.Id) == types.T_timestamp &&
		types.T(valueExprs[1].Typ.Id) == types.T_timestamp {
		commonBetweenScale = valueExprs[0].Typ.Scale
	}
	for i := range values {
		comparisonOp := op
		if op == "between" {
			if i == 0 {
				comparisonOp = ">="
			} else {
				comparisonOp = "<="
			}
		}
		timestampScale := valueExprs[i].Typ.Scale
		if columnType == types.T_timestamp {
			timestampScale = colDef.Typ.Scale
		} else if commonBetweenScale >= 0 {
			timestampScale = commonBetweenScale
		}
		matchers[i], ok = makeTemporalFilterMatcher(
			columnType,
			values[i], types.T(valueExprs[i].Typ.Id), timestampScale,
			zone, comparisonOp,
		)
		if !ok {
			return nil, nil, nil, nil, nil, false, false, true
		}
	}
	match := func(zm objectio.ZoneMap) temporalZoneMapMatch {
		if op == "between" {
			return matchers[0](zm).and(matchers[1](zm))
		}
		return matchers[0](zm)
	}

	isPK, isSorted := isSortedKey(colDef)
	if isSorted {
		fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
			if obj.ZMIsEmpty() {
				return true, nil
			}
			return match(obj.SortKeyZoneMap()).mayMatch(), nil
		}
	}
	loadOp = loadMetadataOnlyOpFactory(fs)
	seqNum := colDef.Seqnum
	objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
		if isSorted {
			return true, nil
		}
		return match(meta.MustDataMeta().MustGetColumn(uint16(seqNum)).ZoneMap()).mayMatch(), nil
	}
	blockFilterOp = func(
		_ int, blkMeta objectio.BlockObject, _ objectio.BloomFilter,
	) (bool, bool, error) {
		return false, match(blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()).mayMatch(), nil
	}
	return fastFilterOp, loadOp, objectFilterOp, blockFilterOp, nil, true, isPK, true
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
	switch exprImpl := expr.Expr.(type) {
	// case *plan.Expr_Lit:
	// case *plan.Expr_Col:
	case *plan.Expr_F:
		if op1, op2, op3, op4, op5, can, hsh, handled := compileTemporalFilterExpr(
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
				op1, op2, op3, op4, op5, can, hsh := compileFilterExpr(exprImpl.F.Args[idx], tableDef, fs, zone)
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
				for idx := range fastOps {
					if fastOps[idx] == nil {
						continue
					}
					if ok, err := fastOps[idx](stats); ok || err != nil {
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

					if ok, err := objectOps[idx](meta, bf); ok || err != nil {
						return ok, err
					}
				}
				return true, nil
			}

			blockFilterOp = func(blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter) (bool, bool, error) {
				can := true
				ok := false
				for idx := range blockOps {
					if blockOps[idx] == nil {
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
				op1, op2, op3, op4, op5, can, hsh := compileFilterExpr(exprImpl.F.Args[idx], tableDef, fs, zone)
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

		case "<=":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyLEByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLEByValue(vals[0]), nil
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				ok := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLEByValue(vals[0])
				if isSorted {
					return !ok, ok, nil
				}
				return false, ok, nil
			}
		case ">=":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyGEByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0]), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					blkIdx := sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0])
					})
					return blkIdx
				}
			}
		case ">":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyGTByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0]), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					blkIdx := sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0])
					})
					return blkIdx
				}
			}
		case "<":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyLTByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLTByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				ok := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLTByValue(vals[0])
				if isSorted {
					return !ok, ok, nil
				}
				return false, ok, nil
			}
		case "prefix_eq":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixEq(vals[0]), nil
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
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && types.PrefixCompare(zm.GetMinBuf(), vals[0]) > 0 {
					return true, false, nil
				}
				return false, zm.PrefixEq(vals[0]), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					return sort.Search(blockCnt, func(j int) bool {
						return types.PrefixCompare(
							meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().GetMaxBuf(),
							vals[0],
						) >= 0
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
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixBetween(vals[0], vals[1]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixBetween(vals[0], vals[1]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && types.PrefixCompare(zm.GetMinBuf(), vals[1]) > 0 {
					return true, false, nil
				}
				return false, zm.PrefixBetween(vals[0], vals[1]), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					return sort.Search(blockCnt, func(j int) bool {
						return types.PrefixCompare(
							meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().GetMaxBuf(),
							vals[0],
						) >= 0
					})
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
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixInRange(vals[0], vals[1], hint), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixInRange(vals[0], vals[1], hint), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted {
					cmp := types.PrefixCompare(zm.GetMinBuf(), vals[1])
					if cmp > 0 || (cmp == 0 && (hint == 2 || hint == 3)) {
						return true, false, nil
					}
				}
				return false, zm.PrefixInRange(vals[0], vals[1], hint), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					return sort.Search(blockCnt, func(j int) bool {
						zm := meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap()
						if hint == 1 || hint == 3 {
							return types.PrefixCompare(zm.GetMaxBuf(), vals[0]) > 0
						}
						return types.PrefixCompare(zm.GetMaxBuf(), vals[0]) >= 0
					})
				}
			}
		case "between":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().Between(vals[0], vals[1]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().Between(vals[0], vals[1]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && !zm.AnyLEByValue(vals[1]) {
					return true, false, nil
				}
				return false, zm.Between(vals[0], vals[1]), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					return sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0])
					})
				}
			}
		case "in_range":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok || len(vals) < 3 || len(vals[2]) == 0 {
				canCompile = false
				return
			}
			hint := vals[2][0]
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().InRange(vals[0], vals[1], hint), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().InRange(vals[0], vals[1], hint), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted {
					if hint == 2 || hint == 3 {
						// open UB: break when min >= ub
						if !zm.AnyLTByValue(vals[1]) {
							return true, false, nil
						}
					} else {
						// closed UB: break when min > ub
						if !zm.AnyLEByValue(vals[1]) {
							return true, false, nil
						}
					}
				}
				return false, zm.InRange(vals[0], vals[1], hint), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					return sort.Search(blockCnt, func(j int) bool {
						zm := meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap()
						if hint == 1 || hint == 3 {
							return zm.AnyGTByValue(vals[0])
						}
						return zm.AnyGEByValue(vals[0])
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
			_ = vec.UnmarshalBinary(val)
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixIn(vec), nil
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
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixIn(vec), nil
			}
			var minPrefix, maxPrefix []byte
			if vec.Length() > 0 {
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
				if isSorted && maxPrefix != nil && types.PrefixCompare(zm.GetMinBuf(), maxPrefix) > 0 {
					return true, false, nil
				}
				if !zm.PrefixIn(vec) {
					return false, false, nil
				}
				return false, true, nil
			}
			if isSorted && minPrefix != nil {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					return sort.Search(blockCnt, func(j int) bool {
						return types.PrefixCompare(
							meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().GetMaxBuf(),
							minPrefix,
						) >= 0
					})
				}
			}
			// ok
		case "isnull", "is_null":
			colExpr, _, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			fastFilterOp = nil
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).NullCnt() != 0, nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).NullCnt() != 0, nil
			}

			// ok
		case "isnotnull", "is_not_null":
			colExpr, _, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			fastFilterOp = nil
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).NullCnt() < dataMeta.BlockHeader().Rows(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).NullCnt() < blkMeta.GetRows(), nil
			}

		case "in":
			colExpr, val, ok := mustColVecValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			vec := vector.NewVec(types.T_any.ToType())
			_ = vec.UnmarshalBinary(val)
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyIn(vec), nil
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
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyIn(vec), nil
			}
			vecHasNull := vec.IsConstNull() || vec.GetNulls().Any()
			var maxVal []byte
			if vec.Length() > 0 && !vecHasNull {
				maxVal = vec.GetRawBytesAt(vec.Length() - 1)
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && maxVal != nil && !zm.AnyLEByValue(maxVal) {
					return true, false, nil
				}
				if !zm.AnyIn(vec) {
					return false, false, nil
				}
				if isPK {
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
			if isSorted && vec.Length() > 0 && !vecHasNull {
				minVal := vec.GetRawBytesAt(0)
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					return sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(minVal)
					})
				}
			}
		case "=":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().ContainsKey(vals[0]), nil
				}
			}
			if isPK {
				loadOp = loadMetadataAndBFOpFactory(fs)
			} else {
				loadOp = loadMetadataOnlyOpFactory(fs)
			}

			highSelectivityHint = isPK

			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().ContainsKey(vals[0]), nil
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				var (
					can, ok bool
				)
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted {
					can = !zm.AnyLEByValue(vals[0])
					if can {
						ok = false
					} else {
						ok = zm.ContainsKey(vals[0])
					}
				} else {
					can = false
					ok = zm.ContainsKey(vals[0])
				}
				if !ok {
					return can, ok, nil
				}
				if isPK {
					var blkBF index.BloomFilter
					buf := bf.GetBloomFilter(uint32(blkIdx))
					if err := blkBF.Unmarshal(buf); err != nil {
						return false, false, err
					}
					exist, err := blkBF.MayContainsKey(vals[0])
					if err != nil || !exist {
						return false, false, err
					}
				}
				return false, true, nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					blkIdx := sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0])
					})
					return blkIdx
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
