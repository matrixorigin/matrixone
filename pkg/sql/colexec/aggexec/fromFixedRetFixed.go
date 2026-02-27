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

package aggexec

import (
	"bytes"
	"fmt"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var _ AggFuncExec = &aggregatorFromFixedToFixed[int64, int64]{}

func RegisterAggFromFixedRetFixed[from, to types.FixedSizeTExceptStrType](
	basicInformation SingleColumnAggInformation,
	initCommonContext AggCommonContextInit,
	initGroupContext AggGroupContextInit,
	initResult InitFixedResultOfAgg[to],
	fill fixedFixedFill[from, to],
	fills fixedFixedFills[from, to],
	merge fixedFixedMerge[from, to],
	flush fixedFixedFlush[from, to]) {

	key := generateKeyOfSingleColumnAgg(
		basicInformation.id, basicInformation.arg)
	if _, ok := registeredAggFunctions[key]; ok {
		panic(fmt.Sprintf("agg function with id %d and arg %s has been registered", basicInformation.id, basicInformation.arg))
	}

	impl := aggImplementation{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          true,
			acceptNull:           false,
			setNullForEmptyGroup: basicInformation.setNullForEmptyGroup,
		},

		ret: basicInformation.ret,

		ctx: aggContextImplementation{
			hasCommonContext:      initCommonContext != nil,
			hasGroupContext:       initGroupContext != nil,
			generateCommonContext: initCommonContext,
			generateGroupContext:  initGroupContext,
		},

		logic: aggLogicImplementation{
			init:  initResult,
			fill:  fill,
			fills: fills,
			merge: merge,
			flush: flush,
		},
	}
	if initResult == nil {
		impl.logic.init = nil
	}
	if flush == nil {
		impl.logic.flush = nil
	}

	registeredAggFunctions[key] = impl
	singleAgg[basicInformation.id] = true
}

// newSingleAggFuncExec1NewVersion creates a aggregatorFromFixedToFixed from agg information.
func newSingleAggFuncExec1NewVersion(
	mg *mpool.MPool, info singleAggInfo, impl aggImplementation) AggFuncExec {
	switch info.retType.Oid {
	case types.T_bool:
		return newAggregatorFromFixedToFixed[bool](mg, info, impl)
	case types.T_int8:
		return newAggregatorFromFixedToFixed[int8](mg, info, impl)
	case types.T_int16:
		return newAggregatorFromFixedToFixed[int16](mg, info, impl)
	case types.T_int32:
		return newAggregatorFromFixedToFixed[int32](mg, info, impl)
	case types.T_int64:
		return newAggregatorFromFixedToFixed[int64](mg, info, impl)
	case types.T_uint8:
		return newAggregatorFromFixedToFixed[uint8](mg, info, impl)
	case types.T_uint16:
		return newAggregatorFromFixedToFixed[uint16](mg, info, impl)
	case types.T_uint32:
		return newAggregatorFromFixedToFixed[uint32](mg, info, impl)
	case types.T_uint64:
		return newAggregatorFromFixedToFixed[uint64](mg, info, impl)
	case types.T_float32:
		return newAggregatorFromFixedToFixed[float32](mg, info, impl)
	case types.T_float64:
		return newAggregatorFromFixedToFixed[float64](mg, info, impl)
	case types.T_decimal64:
		return newAggregatorFromFixedToFixed[types.Decimal64](mg, info, impl)
	case types.T_decimal128:
		return newAggregatorFromFixedToFixed[types.Decimal128](mg, info, impl)
	case types.T_date:
		return newAggregatorFromFixedToFixed[types.Date](mg, info, impl)
	case types.T_datetime:
		return newAggregatorFromFixedToFixed[types.Datetime](mg, info, impl)
	case types.T_time:
		return newAggregatorFromFixedToFixed[types.Time](mg, info, impl)
	case types.T_timestamp:
		return newAggregatorFromFixedToFixed[types.Timestamp](mg, info, impl)
	case types.T_year:
		return newAggregatorFromFixedToFixed[types.MoYear](mg, info, impl)
	case types.T_bit:
		return newAggregatorFromFixedToFixed[uint64](mg, info, impl)
	case types.T_TS:
		return newAggregatorFromFixedToFixed[types.TS](mg, info, impl)
	case types.T_Rowid:
		return newAggregatorFromFixedToFixed[types.Rowid](mg, info, impl)
	case types.T_Blockid:
		return newAggregatorFromFixedToFixed[types.Blockid](mg, info, impl)
	case types.T_uuid:
		return newAggregatorFromFixedToFixed[types.Uuid](mg, info, impl)
	case types.T_enum:
		return newAggregatorFromFixedToFixed[types.Enum](mg, info, impl)
	}
	panic(fmt.Sprintf("unsupported result type %s for single column agg executor1", info.retType))
}

func newAggregatorFromFixedToFixed[to types.FixedSizeTExceptStrType](
	mg *mpool.MPool, info singleAggInfo, impl aggImplementation) AggFuncExec {
	switch info.argType.Oid {
	case types.T_bool:
		e := &aggregatorFromFixedToFixed[bool, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_bit:
		e := &aggregatorFromFixedToFixed[uint64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_int8:
		e := &aggregatorFromFixedToFixed[int8, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_int16:
		e := &aggregatorFromFixedToFixed[int16, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_int32:
		e := &aggregatorFromFixedToFixed[int32, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_int64:
		e := &aggregatorFromFixedToFixed[int64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint8:
		e := &aggregatorFromFixedToFixed[uint8, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint16:
		e := &aggregatorFromFixedToFixed[uint16, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint32:
		e := &aggregatorFromFixedToFixed[uint32, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint64:
		e := &aggregatorFromFixedToFixed[uint64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_float32:
		e := &aggregatorFromFixedToFixed[float32, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_float64:
		e := &aggregatorFromFixedToFixed[float64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_decimal64:
		e := &aggregatorFromFixedToFixed[types.Decimal64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_decimal128:
		e := &aggregatorFromFixedToFixed[types.Decimal128, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_date:
		e := &aggregatorFromFixedToFixed[types.Date, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_datetime:
		e := &aggregatorFromFixedToFixed[types.Datetime, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_time:
		e := &aggregatorFromFixedToFixed[types.Time, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_timestamp:
		e := &aggregatorFromFixedToFixed[types.Timestamp, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_year:
		e := &aggregatorFromFixedToFixed[types.MoYear, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_TS:
		e := &aggregatorFromFixedToFixed[types.TS, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_Rowid:
		e := &aggregatorFromFixedToFixed[types.Rowid, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_Blockid:
		e := &aggregatorFromFixedToFixed[types.Rowid, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uuid:
		e := &aggregatorFromFixedToFixed[types.Uuid, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_enum:
		e := &aggregatorFromFixedToFixed[types.Enum, to]{}
		e.init(mg, info, impl)
		return e
	}
	panic(fmt.Sprintf("unexpected parameter to Init a singleAggFuncExec1NewVersion, aggInfo: %s", info))
}

// aggregatorFromFixedToFixed[from, to] is the aggregator which accept from-type column and return a to-type result.
type aggregatorFromFixedToFixed[from, to types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggExecExtraInformation
	distinctHash

	arg sFixedArg[from]
	ret aggResultWithFixedType[to]

	execContext *AggContext

	fill  fixedFixedFill[from, to]
	fills fixedFixedFills[from, to]
	merge fixedFixedMerge[from, to]
	flush fixedFixedFlush[from, to]
}

func (exec *aggregatorFromFixedToFixed[from, to]) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *aggregatorFromFixedToFixed[from, to]) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, dist, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}
	if dist != nil {
		return nil, moerr.NewInternalErrorNoCtx("dist should have been nil")
	}
	encoded := &EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  exec.execContext.getGroupContextEncodings(),
	}
	return encoded.Marshal()
}

func (exec *aggregatorFromFixedToFixed[from, to]) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	return marshalRetAndGroupsToBuffer[dummyBinaryMarshaler](
		cnt, flags, buf,
		&exec.ret.optSplitResult, nil, exec.execContext.getGroupContextEncodingsForFlags(cnt, flags))
}

func (exec *aggregatorFromFixedToFixed[from, to]) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	start := exec.ret.optSplitResult.optInformation.chunkSize * chunk
	chunkNGroup := exec.ret.optSplitResult.getNthChunkSize(chunk)
	return marshalChunkToBuffer[dummyBinaryMarshaler](
		chunk, buf,
		&exec.ret.optSplitResult, nil, exec.execContext.getGroupContextEncodingsForChunk(start, chunkNGroup))
}

func (exec *aggregatorFromFixedToFixed[from, to]) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	_, groups, err := unmarshalFromReader[dummyBinaryUnmarshaler](reader, &exec.ret.optSplitResult)
	if err != nil {
		return err
	}
	exec.ret.setupT()
	exec.execContext.decodeGroupContexts(groups, exec.singleAggInfo.retType, exec.singleAggInfo.argType)
	return nil
}

func (exec *aggregatorFromFixedToFixed[from, to]) unmarshal(_ *mpool.MPool, result, empties, groups [][]byte) error {
	exec.execContext.decodeGroupContexts(groups, exec.singleAggInfo.retType, exec.singleAggInfo.argType)
	return exec.ret.unmarshalFromBytes(result, empties, nil)
}

func (exec *aggregatorFromFixedToFixed[from, to]) init(
	mg *mpool.MPool,
	info singleAggInfo,
	impl aggImplementation) {

	if info.IsDistinct() {
		exec.distinctHash = newDistinctHash(mg)
	}

	var v to
	if resultInitMethod := impl.logic.init; resultInitMethod != nil {
		v = resultInitMethod.(InitFixedResultOfAgg[to])(info.retType, info.argType)
	}

	// XXX
	// This is fucked.   See marshal/unmarshal for special handling of distinct.
	exec.ret = initAggResultWithFixedTypeResult[to](mg, info.retType, info.emptyNull, v, info.IsDistinct())

	exec.singleAggInfo = info
	exec.singleAggExecExtraInformation = emptyExtraInfo
	exec.execContext = newAggContextFromImpl(impl.ctx, info.retType, info.argType)

	if flushMethod := impl.logic.flush; flushMethod != nil {
		exec.flush = flushMethod.(fixedFixedFlush[from, to])
	}

	exec.fill = impl.logic.fill.(fixedFixedFill[from, to])
	exec.fills = impl.logic.fills.(fixedFixedFills[from, to])
	exec.merge = impl.logic.merge.(fixedFixedMerge[from, to])
}

func (exec *aggregatorFromFixedToFixed[from, to]) GroupGrow(more int) error {
	if err := exec.ret.grows(more); err != nil {
		return err
	}
	// deal with distinct hash.
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	// deal with execContext.
	exec.execContext.growsGroupContext(more, exec.singleAggInfo.retType, exec.singleAggInfo.argType)
	return nil
}

func (exec *aggregatorFromFixedToFixed[from, to]) PreAllocateGroups(more int) error {
	exec.execContext.preAllocate(more)
	return exec.ret.preExtend(more)
}

func (exec *aggregatorFromFixedToFixed[from, to]) Fill(
	group int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}

	if vectors[0].IsConst() {
		row = 0
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(group, vectors, row); !need || err != nil {
			return err
		}
	}

	x, y := exec.ret.updateNextAccessIdx(group)
	err := exec.fill(
		exec.execContext.getGroupContext(group),
		exec.execContext.getCommonContext(),
		vector.MustFixedColWithTypeCheck[from](vectors[0])[row],
		exec.ret.isGroupEmpty(x, y),
		exec.ret.get, exec.ret.set)
	exec.ret.setGroupNotEmpty(x, y)
	return err
}

func (exec *aggregatorFromFixedToFixed[from, to]) BulkFill(
	group int, vectors []*vector.Vector) error {
	length := vectors[0].Length()
	if length == 0 || vectors[0].IsConstNull() {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBulkFill(group, vectors, length)
	}

	x, y := exec.ret.updateNextAccessIdx(group)
	getter := exec.ret.get
	setter := exec.ret.set
	groupContext := exec.execContext.getGroupContext(group)
	commonContext := exec.execContext.getCommonContext()

	if vectors[0].IsConst() {
		err := exec.fills(
			groupContext,
			commonContext,
			vector.MustFixedColWithTypeCheck[from](vectors[0])[0],
			length, exec.ret.isGroupEmpty(x, y),
			getter, setter)
		exec.ret.setGroupNotEmpty(x, y)
		return err
	}

	exec.arg.prepare(vectors[0])
	if exec.arg.w.WithAnyNullValue() {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			v, null := exec.arg.w.GetValue(i)
			if !null {
				if err := exec.fill(groupContext, commonContext, v, exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
					return err
				}
				exec.ret.setGroupNotEmpty(x, y)
			}
		}
		return nil
	}

	vs := exec.arg.w.UnSafeGetAllValue()
	for _, v := range vs {
		if err := exec.fill(groupContext, commonContext, v, exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
			return err
		}
		exec.ret.setGroupNotEmpty(x, y)
	}
	return nil
}

func (exec *aggregatorFromFixedToFixed[from, to]) distinctBulkFill(
	group int, vectors []*vector.Vector, length int) error {
	x, y := exec.ret.updateNextAccessIdx(group)
	getter := exec.ret.get
	setter := exec.ret.set
	groupContext := exec.execContext.getGroupContext(group)
	commonContext := exec.execContext.getCommonContext()

	if vectors[0].IsConst() {
		if need, err := exec.distinctHash.fill(group, vectors, 0); !need || err != nil {
			return err
		}
		err := exec.fill(groupContext, commonContext, vector.MustFixedColWithTypeCheck[from](vectors[0])[0], exec.ret.isGroupEmpty(x, y), getter, setter)
		exec.ret.setGroupNotEmpty(x, y)
		return err
	}

	exec.arg.prepare(vectors[0])
	needs, err := exec.distinctHash.bulkFill(group, vectors)
	if err != nil {
		return err
	}

	if exec.arg.w.WithAnyNullValue() {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					if err = exec.fill(groupContext, commonContext, v, exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
						return err
					}
					exec.ret.setGroupNotEmpty(x, y)
				}
			}
		}
		return nil
	}

	vs := exec.arg.w.UnSafeGetAllValue()
	for i, v := range vs {
		if needs[i] {
			if err = exec.fill(groupContext, commonContext, v, exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
				return err
			}
			exec.ret.setGroupNotEmpty(x, y)
		}
	}
	return nil
}

func (exec *aggregatorFromFixedToFixed[from, to]) BatchFill(
	offset int, groups []uint64, vectors []*vector.Vector) error {
	if len(groups) == 0 || vectors[0].IsConstNull() {
		return nil
	}

	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

	getter := exec.ret.get
	setter := exec.ret.set
	commonContext := exec.execContext.getCommonContext()

	if vectors[0].IsConst() {
		value := vector.MustFixedColWithTypeCheck[from](vectors[0])[0]
		for _, group := range groups {
			if group != GroupNotMatched {
				idx := int(group - 1)
				x, y := exec.ret.updateNextAccessIdx(idx)
				if err := exec.fill(
					exec.execContext.getGroupContext(idx), commonContext, value, exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
					return err
				}
				exec.ret.setGroupNotEmpty(x, y)
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	if exec.arg.w.WithAnyNullValue() {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					x, y := exec.ret.updateNextAccessIdx(groupIdx)
					if err := exec.fill(
						exec.execContext.getGroupContext(groupIdx), commonContext, v, exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
						return err
					}
					exec.ret.setGroupNotEmpty(x, y)
				}
			}
			idx++
		}
		return nil
	}

	vs := exec.arg.w.UnSafeGetAllValue()
	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			groupIdx := int(groups[idx] - 1)
			x, y := exec.ret.updateNextAccessIdx(groupIdx)
			if err := exec.fill(
				exec.execContext.getGroupContext(groupIdx), commonContext, vs[i], exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
				return err
			}
			exec.ret.setGroupNotEmpty(x, y)
		}
		idx++
	}
	return nil
}

func (exec *aggregatorFromFixedToFixed[from, to]) distinctBatchFill(
	offset int, groups []uint64, vectors []*vector.Vector) error {
	getter := exec.ret.get
	setter := exec.ret.set
	commonContext := exec.execContext.getCommonContext()

	needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
	if err != nil {
		return err
	}

	if vectors[0].IsConst() {
		value := vector.MustFixedColWithTypeCheck[from](vectors[0])[0]
		for i, group := range groups {
			if needs[i] && group != GroupNotMatched {
				idx := int(group - 1)
				x, y := exec.ret.updateNextAccessIdx(idx)
				if err = exec.fill(
					exec.execContext.getGroupContext(idx), commonContext, value, exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
					return err
				}
				exec.ret.setGroupNotEmpty(x, y)
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	if exec.arg.w.WithAnyNullValue() {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					x, y := exec.ret.updateNextAccessIdx(groupIdx)
					if err = exec.fill(
						exec.execContext.getGroupContext(groupIdx), commonContext, v, exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
						return err
					}
					exec.ret.setGroupNotEmpty(x, y)
				}
			}
			idx++
		}
		return nil
	}

	vs := exec.arg.w.UnSafeGetAllValue()
	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if needs[idx] && groups[idx] != GroupNotMatched {
			groupIdx := int(groups[idx] - 1)
			x, y := exec.ret.updateNextAccessIdx(groupIdx)
			if err = exec.fill(
				exec.execContext.getGroupContext(groupIdx), commonContext, vs[i], exec.ret.bsFromEmptyList[x][y], getter, setter); err != nil {
				return err
			}
			exec.ret.setGroupNotEmpty(x, y)
		}
		idx++
	}
	return nil
}

func (exec *aggregatorFromFixedToFixed[from, to]) Flush() ([]*vector.Vector, error) {
	getter := exec.ret.get
	setter := exec.ret.set
	commonContext := exec.execContext.getCommonContext()

	if exec.partialResult != nil {
		if value, ok := exec.partialResult.(from); ok {
			x, y := exec.ret.updateNextAccessIdx(exec.partialGroup)
			if err := exec.fill(
				exec.execContext.getGroupContext(exec.partialGroup), commonContext, value, exec.ret.isGroupEmpty(x, y), getter, setter); err != nil {
				return nil, err
			}
			exec.ret.setGroupNotEmpty(x, y)
		}
	}

	if exec.flush != nil {
		lim := exec.ret.getEachBlockLimitation()
		groups := exec.ret.totalGroupCount()

		if exec.ret.optInformation.shouldSetNullToEmptyGroup {
			for i, x := 0, 0; i < groups; i += lim {
				n := groups - i
				if n > lim {
					n = lim
				}

				for j, k := 0, i; j < n; j++ {
					if exec.ret.isGroupEmpty(x, j) {
						k++
						continue
					}
					exec.ret.setNextAccessDirectly(x, j)

					if err := exec.flush(exec.execContext.getGroupContext(k), commonContext, getter, setter); err != nil {
						return nil, err
					}
					k++
				}
				x++
			}

		} else {
			for i, x := 0, 0; i < groups; i += lim {
				n := groups - i
				if n > lim {
					n = lim
				}

				for j, k := 0, i; j < n; j++ {
					exec.ret.setNextAccessDirectly(x, j)

					if err := exec.flush(exec.execContext.getGroupContext(k), commonContext, getter, setter); err != nil {
						return nil, err
					}
					k++
				}
				x++
			}
		}
	}

	return exec.ret.flushAll(), nil
}

func (exec *aggregatorFromFixedToFixed[from, to]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*aggregatorFromFixedToFixed[from, to])

	x1, y1 := exec.ret.updateNextAccessIdx(groupIdx1)
	x2, y2 := other.ret.updateNextAccessIdx(groupIdx2)
	isNextEmpty := other.ret.isGroupEmpty(x2, y2)

	if err := exec.merge(
		exec.execContext.getGroupContext(groupIdx1),
		other.execContext.getGroupContext(groupIdx2),
		exec.execContext.getCommonContext(),
		exec.ret.isGroupEmpty(x1, y1),
		isNextEmpty,
		exec.ret.get, other.ret.get, exec.ret.set); err != nil {
		return err
	}
	exec.ret.MergeAnotherEmpty(x1, x2, isNextEmpty)

	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *aggregatorFromFixedToFixed[from, to]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*aggregatorFromFixedToFixed[from, to])
	getter1 := exec.ret.get
	getter2 := other.ret.get
	setter := exec.ret.set
	commonContext := exec.execContext.getCommonContext()

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		groupIdx1, groupIdx2 := int(groups[i]-1), i+offset

		x1, y1 := exec.ret.updateNextAccessIdx(groupIdx1)
		x2, y2 := other.ret.updateNextAccessIdx(groupIdx2)
		isNextEmpty := other.ret.isGroupEmpty(x2, y2)

		if err := exec.merge(
			exec.execContext.getGroupContext(groupIdx1),
			other.execContext.getGroupContext(groupIdx2),
			commonContext,
			exec.ret.isGroupEmpty(x1, y1),
			isNextEmpty,
			getter1, getter2,
			setter); err != nil {
			return err
		}
		exec.ret.MergeAnotherEmpty(x1, y1, isNextEmpty)
	}

	return exec.distinctHash.merge(&other.distinctHash)
}

func (exec *aggregatorFromFixedToFixed[from, to]) Free() {
	exec.ret.free()
	exec.distinctHash.free()
}

func (exec *aggregatorFromFixedToFixed[from, to]) Size() int64 {
	return exec.ret.Size() + exec.distinctHash.Size() + exec.execContext.Size()
}
