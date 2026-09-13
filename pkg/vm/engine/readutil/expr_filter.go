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

type zoneMapMatch struct {
	matches    bool
	comparable bool
}

func (m zoneMapMatch) mayMatch() bool {
	return !m.comparable || m.matches
}

func (m zoneMapMatch) excludes() bool {
	return m.comparable && !m.matches
}

func (m zoneMapMatch) and(other zoneMapMatch) zoneMapMatch {
	if m.excludes() || other.excludes() {
		return zoneMapMatch{comparable: true}
	}
	if m.comparable && other.comparable {
		return zoneMapMatch{matches: true, comparable: true}
	}
	return zoneMapMatch{}
}

func rawZoneMapComparable(zm objectio.ZoneMap, columnType types.T) bool {
	return columnType != types.T_json && zm.IsInited() && zm.GetType() == columnType
}

func anyLTByBound(
	zm objectio.ZoneMap, value []byte, bound objectio.ZoneMap, columnType types.T,
) zoneMapMatch {
	if bound == nil {
		if !rawZoneMapComparable(zm, columnType) {
			return zoneMapMatch{}
		}
		return zoneMapMatch{matches: zm.AnyLTByValue(value), comparable: true}
	}
	result, ok := zm.AnyLT(bound)
	return zoneMapMatch{matches: result, comparable: ok}
}

func anyLEByBound(
	zm objectio.ZoneMap, value []byte, bound objectio.ZoneMap, columnType types.T,
) zoneMapMatch {
	if bound == nil {
		if !rawZoneMapComparable(zm, columnType) {
			return zoneMapMatch{}
		}
		return zoneMapMatch{matches: zm.AnyLEByValue(value), comparable: true}
	}
	result, ok := zm.AnyLE(bound)
	return zoneMapMatch{matches: result, comparable: ok}
}

func anyGTByBound(
	zm objectio.ZoneMap, value []byte, bound objectio.ZoneMap, columnType types.T,
) zoneMapMatch {
	if bound == nil {
		if !rawZoneMapComparable(zm, columnType) {
			return zoneMapMatch{}
		}
		return zoneMapMatch{matches: zm.AnyGTByValue(value), comparable: true}
	}
	result, ok := zm.AnyGT(bound)
	return zoneMapMatch{matches: result, comparable: ok}
}

func anyGEByBound(
	zm objectio.ZoneMap, value []byte, bound objectio.ZoneMap, columnType types.T,
) zoneMapMatch {
	if bound == nil {
		if !rawZoneMapComparable(zm, columnType) {
			return zoneMapMatch{}
		}
		return zoneMapMatch{matches: zm.AnyGEByValue(value), comparable: true}
	}
	result, ok := zm.AnyGE(bound)
	return zoneMapMatch{matches: result, comparable: ok}
}

func intersectsBound(
	zm objectio.ZoneMap, value []byte, bound objectio.ZoneMap, columnType types.T,
) zoneMapMatch {
	if bound == nil {
		if !rawZoneMapComparable(zm, columnType) {
			return zoneMapMatch{}
		}
		return zoneMapMatch{matches: zm.ContainsKey(value), comparable: true}
	}
	result, ok := zm.Intersect(bound)
	return zoneMapMatch{matches: result, comparable: ok}
}

func anyBetweenBounds(
	zm objectio.ZoneMap,
	lowerValue, upperValue []byte,
	lowerBound, upperBound objectio.ZoneMap,
	columnType types.T,
) zoneMapMatch {
	if lowerBound == nil {
		if !rawZoneMapComparable(zm, columnType) {
			return zoneMapMatch{}
		}
		return zoneMapMatch{matches: zm.Between(lowerValue, upperValue), comparable: true}
	}
	result, ok := zm.AnyBetween(lowerBound, upperBound)
	return zoneMapMatch{matches: result, comparable: ok}
}

func inRangeBounds(
	zm objectio.ZoneMap,
	lowerValue, upperValue []byte,
	lowerBound, upperBound objectio.ZoneMap,
	hint uint8,
	columnType types.T,
) zoneMapMatch {
	switch hint {
	case 1: // (lb, ub]
		return anyGTByBound(zm, lowerValue, lowerBound, columnType).
			and(anyLEByBound(zm, upperValue, upperBound, columnType))
	case 2: // [lb, ub)
		return anyGEByBound(zm, lowerValue, lowerBound, columnType).
			and(anyLTByBound(zm, upperValue, upperBound, columnType))
	case 3: // (lb, ub)
		return anyGTByBound(zm, lowerValue, lowerBound, columnType).
			and(anyLTByBound(zm, upperValue, upperBound, columnType))
	default: // [lb, ub]
		return anyGEByBound(zm, lowerValue, lowerBound, columnType).
			and(anyLEByBound(zm, upperValue, upperBound, columnType))
	}
}

func zoneMapVectorComparable(
	zm objectio.ZoneMap, vec *vector.Vector, columnType types.T,
) bool {
	if !rawZoneMapComparable(zm, columnType) || vec == nil || vec.GetType().Oid != columnType {
		return false
	}
	return !columnType.IsDecimal() || zm.GetScale() == vec.GetType().Scale
}

func anyInVector(
	zm objectio.ZoneMap, vec *vector.Vector, columnType types.T,
) zoneMapMatch {
	if !zoneMapVectorComparable(zm, vec, columnType) {
		return zoneMapMatch{}
	}
	return zoneMapMatch{matches: zm.AnyIn(vec), comparable: true}
}

func prefixEqByValue(zm objectio.ZoneMap, value []byte, columnType types.T) zoneMapMatch {
	if !rawZoneMapComparable(zm, columnType) {
		return zoneMapMatch{}
	}
	return zoneMapMatch{matches: zm.PrefixEq(value), comparable: true}
}

func prefixBetweenByValue(
	zm objectio.ZoneMap, lower, upper []byte, columnType types.T,
) zoneMapMatch {
	if !rawZoneMapComparable(zm, columnType) {
		return zoneMapMatch{}
	}
	return zoneMapMatch{matches: zm.PrefixBetween(lower, upper), comparable: true}
}

func prefixInRangeByValue(
	zm objectio.ZoneMap, lower, upper []byte, hint uint8, columnType types.T,
) zoneMapMatch {
	if !rawZoneMapComparable(zm, columnType) {
		return zoneMapMatch{}
	}
	return zoneMapMatch{matches: zm.PrefixInRange(lower, upper, hint), comparable: true}
}

func prefixInVector(
	zm objectio.ZoneMap, vec *vector.Vector, columnType types.T,
) zoneMapMatch {
	if !zoneMapVectorComparable(zm, vec, columnType) {
		return zoneMapMatch{}
	}
	if vec.IsConstNull() || vec.GetNulls().Any() {
		return zoneMapMatch{}
	}
	return zoneMapMatch{matches: zm.PrefixIn(vec), comparable: true}
}

func anyPrefixLTByValue(zm objectio.ZoneMap, value []byte, columnType types.T) zoneMapMatch {
	if !rawZoneMapComparable(zm, columnType) {
		return zoneMapMatch{}
	}
	return zoneMapMatch{
		matches:    types.PrefixCompare(zm.GetMinBuf(), value) < 0,
		comparable: true,
	}
}

func anyPrefixLEByValue(zm objectio.ZoneMap, value []byte, columnType types.T) zoneMapMatch {
	if !rawZoneMapComparable(zm, columnType) {
		return zoneMapMatch{}
	}
	return zoneMapMatch{
		matches:    types.PrefixCompare(zm.GetMinBuf(), value) <= 0,
		comparable: true,
	}
}

func anyPrefixGTByValue(zm objectio.ZoneMap, value []byte, columnType types.T) zoneMapMatch {
	if !rawZoneMapComparable(zm, columnType) {
		return zoneMapMatch{}
	}
	return zoneMapMatch{
		matches:    types.PrefixCompare(zm.GetMaxBuf(), value) > 0,
		comparable: true,
	}
}

func anyPrefixGEByValue(zm objectio.ZoneMap, value []byte, columnType types.T) zoneMapMatch {
	if !rawZoneMapComparable(zm, columnType) {
		return zoneMapMatch{}
	}
	return zoneMapMatch{
		matches:    types.PrefixCompare(zm.GetMaxBuf(), value) >= 0,
		comparable: true,
	}
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

func seekFirstBlockByZoneMap(
	meta objectio.ObjectDataMeta,
	seqNum uint16,
	bound objectio.ZoneMap,
	columnType types.T,
	compare func(objectio.ZoneMap) zoneMapMatch,
) int {
	blockCnt := int(meta.BlockCount())
	if blockCnt == 0 || !zoneMapMetadataComparable(meta.MustGetColumn(seqNum).ZoneMap(), bound, columnType) {
		return 0
	}
	for j := range blockCnt {
		if !zoneMapMetadataComparable(
			meta.GetBlockMeta(uint32(j)).MustGetColumn(seqNum).ZoneMap(), bound, columnType,
		) {
			return 0
		}
	}
	return sort.Search(blockCnt, func(j int) bool {
		result := compare(meta.GetBlockMeta(uint32(j)).MustGetColumn(seqNum).ZoneMap())
		return result.matches
	})
}

func zoneMapMetadataComparable(
	zm objectio.ZoneMap, bound objectio.ZoneMap, columnType types.T,
) bool {
	if !rawZoneMapComparable(zm, columnType) {
		return false
	}
	return bound == nil || rawZoneMapComparable(bound, columnType)
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

func temporalFilterMatch(op string, value, bound temporalFilterRange) zoneMapMatch {
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
		return zoneMapMatch{}
	}
	return zoneMapMatch{matches: matches, comparable: true}
}

func makeTemporalFilterMatcher(
	columnType types.T,
	value []byte,
	valueType types.T,
	timestampScale int32,
	zone *time.Location,
	op string,
) (func(objectio.ZoneMap) zoneMapMatch, bool) {
	if columnType == valueType {
		return func(zm objectio.ZoneMap) zoneMapMatch {
			switch op {
			case "=":
				return intersectsBound(zm, value, nil, columnType)
			case "<":
				return anyLTByBound(zm, value, nil, columnType)
			case "<=":
				return anyLEByBound(zm, value, nil, columnType)
			case ">":
				return anyGTByBound(zm, value, nil, columnType)
			case ">=":
				return anyGEByBound(zm, value, nil, columnType)
			default:
				return zoneMapMatch{}
			}
		}, true
	}
	if !isMixedTemporalFilterTypes(columnType, valueType) {
		return nil, false
	}

	bound, ok := temporalFilterRangeFromValue(value, valueType, timestampScale, zone)
	if !ok {
		return nil, false
	}
	return func(zm objectio.ZoneMap) zoneMapMatch {
		valueRange, ok := temporalFilterRangeFromZoneMap(zm, timestampScale, zone)
		if !ok {
			return zoneMapMatch{}
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

	matchers := make([]func(objectio.ZoneMap) zoneMapMatch, len(values))
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
	match := func(zm objectio.ZoneMap) zoneMapMatch {
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
			colExpr, vals, valExprs, ok := mustColConstValueWithTypeFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			bound, ok := makeDecimalZoneMapBound(colDef, vals[0], valExprs[0])
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
					return anyLEByBound(obj.SortKeyZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return anyLEByBound(dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				result := anyLEByBound(blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id))
				if isSorted {
					return result.excludes(), result.mayMatch(), nil
				}
				return false, result.mayMatch(), nil
			}
		case ">=":
			colExpr, vals, valExprs, ok := mustColConstValueWithTypeFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			bound, ok := makeDecimalZoneMapBound(colDef, vals[0], valExprs[0])
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
					return anyGEByBound(obj.SortKeyZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return anyGEByBound(dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, anyGEByBound(blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), bound, types.T(colDef.Typ.Id), func(zm objectio.ZoneMap) zoneMapMatch {
						return anyGEByBound(zm, vals[0], bound, types.T(colDef.Typ.Id))
					})
				}
			}
		case ">":
			colExpr, vals, valExprs, ok := mustColConstValueWithTypeFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			bound, ok := makeDecimalZoneMapBound(colDef, vals[0], valExprs[0])
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
					return anyGTByBound(obj.SortKeyZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return anyGTByBound(dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, anyGTByBound(blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), bound, types.T(colDef.Typ.Id), func(zm objectio.ZoneMap) zoneMapMatch {
						return anyGTByBound(zm, vals[0], bound, types.T(colDef.Typ.Id))
					})
				}
			}
		case "<":
			colExpr, vals, valExprs, ok := mustColConstValueWithTypeFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			bound, ok := makeDecimalZoneMapBound(colDef, vals[0], valExprs[0])
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
					return anyLTByBound(obj.SortKeyZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return anyLTByBound(dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				result := anyLTByBound(blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id))
				if isSorted {
					return result.excludes(), result.mayMatch(), nil
				}
				return false, result.mayMatch(), nil
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
					return prefixEqByValue(obj.SortKeyZoneMap(), vals[0], columnType).mayMatch(), nil
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
				return prefixEqByValue(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], columnType,
				).mayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && anyPrefixLEByValue(zm, vals[0], columnType).excludes() {
					return true, false, nil
				}
				return false, prefixEqByValue(zm, vals[0], columnType).mayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), nil, columnType, func(zm objectio.ZoneMap) zoneMapMatch {
						return anyPrefixGEByValue(zm, vals[0], columnType)
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
					return prefixBetweenByValue(obj.SortKeyZoneMap(), vals[0], vals[1], columnType).mayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return prefixBetweenByValue(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], vals[1], columnType,
				).mayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted && anyPrefixLEByValue(zm, vals[1], columnType).excludes() {
					return true, false, nil
				}
				return false, prefixBetweenByValue(zm, vals[0], vals[1], columnType).mayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), nil, columnType, func(zm objectio.ZoneMap) zoneMapMatch {
						return anyPrefixGEByValue(zm, vals[0], columnType)
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
			columnType := types.T(colDef.Typ.Id)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return prefixInRangeByValue(
						obj.SortKeyZoneMap(), vals[0], vals[1], hint, columnType,
					).mayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return prefixInRangeByValue(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], vals[1], hint, columnType,
				).mayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted {
					upperResult := anyPrefixLEByValue(zm, vals[1], columnType)
					if hint == 2 || hint == 3 {
						upperResult = anyPrefixLTByValue(zm, vals[1], columnType)
					}
					if upperResult.excludes() {
						return true, false, nil
					}
				}
				return false, prefixInRangeByValue(zm, vals[0], vals[1], hint, columnType).mayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), nil, columnType, func(zm objectio.ZoneMap) zoneMapMatch {
						if hint == 1 || hint == 3 {
							return anyPrefixGTByValue(zm, vals[0], columnType)
						}
						return anyPrefixGEByValue(zm, vals[0], columnType)
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
					return anyBetweenBounds(
						obj.SortKeyZoneMap(), vals[0], vals[1], lowerBound, upperBound, types.T(colDef.Typ.Id),
					).mayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return anyBetweenBounds(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], vals[1], lowerBound, upperBound, types.T(colDef.Typ.Id),
				).mayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				upperResult := anyLEByBound(zm, vals[1], upperBound, types.T(colDef.Typ.Id))
				if isSorted && upperResult.excludes() {
					return true, false, nil
				}
				return false, anyBetweenBounds(
					zm, vals[0], vals[1], lowerBound, upperBound, types.T(colDef.Typ.Id),
				).mayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), lowerBound, types.T(colDef.Typ.Id), func(zm objectio.ZoneMap) zoneMapMatch {
						return anyGEByBound(zm, vals[0], lowerBound, types.T(colDef.Typ.Id))
					})
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
					return inRangeBounds(
						obj.SortKeyZoneMap(), vals[0], vals[1], lowerBound, upperBound, hint, types.T(colDef.Typ.Id),
					).mayMatch(), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return inRangeBounds(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], vals[1], lowerBound, upperBound, hint, types.T(colDef.Typ.Id),
				).mayMatch(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted {
					if hint == 2 || hint == 3 {
						// open UB: break when min >= ub
						if anyLTByBound(zm, vals[1], upperBound, types.T(colDef.Typ.Id)).excludes() {
							return true, false, nil
						}
					} else {
						// closed UB: break when min > ub
						if anyLEByBound(zm, vals[1], upperBound, types.T(colDef.Typ.Id)).excludes() {
							return true, false, nil
						}
					}
				}
				return false, inRangeBounds(
					zm, vals[0], vals[1], lowerBound, upperBound, hint, types.T(colDef.Typ.Id),
				).mayMatch(), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), lowerBound, types.T(colDef.Typ.Id), func(zm objectio.ZoneMap) zoneMapMatch {
						if hint == 1 || hint == 3 {
							return anyGTByBound(zm, vals[0], lowerBound, types.T(colDef.Typ.Id))
						}
						return anyGEByBound(zm, vals[0], lowerBound, types.T(colDef.Typ.Id))
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
					return prefixInVector(obj.SortKeyZoneMap(), vec, columnType).mayMatch(), nil
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
				return prefixInVector(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vec, columnType,
				).mayMatch(), nil
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
				if isSorted && maxPrefix != nil && anyPrefixLEByValue(zm, maxPrefix, columnType).excludes() {
					return true, false, nil
				}
				if prefixInVector(zm, vec, columnType).excludes() {
					return false, false, nil
				}
				return false, true, nil
			}
			if isSorted && minPrefix != nil {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), nil, columnType, func(zm objectio.ZoneMap) zoneMapMatch {
						return anyPrefixGEByValue(zm, minPrefix, columnType)
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
			if err := vec.UnmarshalBinary(val); err != nil {
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
					return anyInVector(obj.SortKeyZoneMap(), vec, columnType).mayMatch(), nil
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
				return anyInVector(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vec, columnType,
				).mayMatch(), nil
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
				if isSorted && maxVal != nil && anyLEByBound(zm, maxVal, maxBound, columnType).excludes() {
					return true, false, nil
				}
				membership := anyInVector(zm, vec, columnType)
				if membership.excludes() {
					return false, false, nil
				}
				if isPK && membership.comparable {
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
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), minBound, columnType, func(zm objectio.ZoneMap) zoneMapMatch {
						return anyGEByBound(zm, minVal, minBound, columnType)
					})
				}
			}
		case "=":
			colExpr, vals, valExprs, ok := mustColConstValueWithTypeFromBinaryFuncExpr(exprImpl)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(expr, colExpr.Col.Name, colExpr.Col.ColPos, tableDef)
			bound, ok := makeDecimalZoneMapBound(colDef, vals[0], valExprs[0])
			if !ok {
				canCompile = false
				return
			}
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj *objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return intersectsBound(obj.SortKeyZoneMap(), vals[0], bound, types.T(colDef.Typ.Id)).mayMatch(), nil
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
				return intersectsBound(
					dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap(), vals[0], bound, types.T(colDef.Typ.Id),
				).mayMatch(), nil
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				var (
					can, ok bool
				)
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				intersection := intersectsBound(zm, vals[0], bound, types.T(colDef.Typ.Id))
				if isSorted {
					can = anyLEByBound(zm, vals[0], bound, types.T(colDef.Typ.Id)).excludes()
					if can {
						ok = false
					} else {
						ok = intersection.mayMatch()
					}
				} else {
					can = false
					ok = intersection.mayMatch()
				}
				if !ok {
					return can, ok, nil
				}
				// Bloom keys are raw encoded values and carry no scale. A decimal
				// bound with a different persisted scale cannot be queried safely.
				if isPK && intersection.comparable && (bound == nil ||
					(bound.GetType() == zm.GetType() && bound.GetScale() == zm.GetScale())) {
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
					return seekFirstBlockByZoneMap(meta, uint16(seqNum), bound, types.T(colDef.Typ.Id), func(zm objectio.ZoneMap) zoneMapMatch {
						return anyGEByBound(zm, vals[0], bound, types.T(colDef.Typ.Id))
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
