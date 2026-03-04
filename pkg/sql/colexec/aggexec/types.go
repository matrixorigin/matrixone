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
	"encoding"
	"fmt"
	io "io"

	proto "github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	// GroupNotMatched is a constant for the BatchFill method.
	// if the group is GroupNotMatched, the BatchFill method will ignore the row.
	GroupNotMatched = 0
)

// AggFuncExecExpression is the exporting structure for the aggregation information.
// it is used to indicate the information of the aggregation function for the operators like 'group' or 'merge group'.
type AggFuncExecExpression struct {
	aggID          int64
	isDistinct     bool
	argExpressions []*plan.Expr
	extraConfig    []byte
}

func MakeAggFunctionExpression(id int64, isDistinct bool, args []*plan.Expr, config []byte) AggFuncExecExpression {
	return AggFuncExecExpression{
		aggID:          id,
		isDistinct:     isDistinct,
		argExpressions: args,
		extraConfig:    config,
	}
}

func (ag *AggFuncExecExpression) GetAggID() int64 {
	return ag.aggID
}

func (ag *AggFuncExecExpression) IsDistinct() bool {
	return ag.isDistinct
}

func (ag *AggFuncExecExpression) GetArgExpressions() []*plan.Expr {
	return ag.argExpressions
}

func (ag *AggFuncExecExpression) GetExtraConfig() []byte {
	return ag.extraConfig
}

// AggFuncExec is an interface to do execution for aggregation.
type AggFuncExec interface {
	marshal() ([]byte, error)
	unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error
	GetOptResult() SplitResult

	AggID() int64
	IsDistinct() bool

	// TypesInfo return the argument types and return type of the function.
	TypesInfo() ([]types.Type, types.Type)

	// GroupGrow increases the number of groups in the aggregation.
	GroupGrow(more int) error

	// PreAllocateGroups pre-allocates more additional groups to reduce garbage collection overhead.
	PreAllocateGroups(more int) error

	// XXX: WTF.
	Fill(groupIndex int, row int, vectors []*vector.Vector) error
	BulkFill(groupIndex int, vectors []*vector.Vector) error

	// BatchFill : add values to the aggregation for multiple groups at once.
	BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error

	// XXX: WTF.
	Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error

	// BatchMerge combines the aggregation result of multiple couples.
	// next: offset + i
	// self: groups[i] - 1
	BatchMerge(next AggFuncExec, offset int, groups []uint64) error

	// SetExtraInformation sets additional information for the aggregation executor,
	// such as partial results.
	//
	// but for the 'group_concat', it was a bad hack to use the method to set the separator.
	// and for the 'cluster_centers', it was used to set the fields of this agg.
	// todo: the old implementation is not good, we should use the vector.Vector to replace the any.
	//  and the hacks should be removed.
	//  but for first version, I will keep it.
	SetExtraInformation(partialResult any, groupIndex int) (err error)

	// Flush return the aggregation result.
	Flush() ([]*vector.Vector, error)

	// Serialize intermediate result to bytes.
	SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error
	SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error
	UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error

	Size() int64

	// Free clean the resource and reuse the aggregation if possible.
	Free()
}

// indicate who implements the AggFuncExec interface.
var (
	_ AggFuncExec = (*aggregatorFromFixedToFixed[int32, int64])(nil)
	_ AggFuncExec = (*aggregatorFromFixedToBytes[int32])(nil)
	_ AggFuncExec = (*aggregatorFromBytesToFixed[int64])(nil)
	_ AggFuncExec = (*aggregatorFromBytesToBytes)(nil)
	_ AggFuncExec = &groupConcatExec{}
)

var (
	emptyExtraInfo = singleAggExecExtraInformation{
		partialGroup:  0,
		partialResult: nil,
	}
)

// MakeAgg is the only exporting method to create an aggregation function executor.
// all the aggID should be registered before calling this function.
func MakeAgg(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	param ...types.Type,
) (AggFuncExec, error) {
	exec, ok, err := makeSpecialAggExec(mg, aggID, isDistinct, param...)
	if err != nil {
		return nil, err
	}
	if ok {
		return exec, nil
	}
	if _, ok = singleAgg[aggID]; ok && len(param) == 1 {
		return makeSingleAgg(mg, aggID, isDistinct, param[0]), nil
	}
	errmsg := fmt.Sprintf("unexpected aggID %d and param types %v.", aggID, param)
	return nil, moerr.NewInternalErrorNoCtx(errmsg)
}

// makeSingleAgg supports to create an aggregation function executor for single column.
func makeSingleAgg(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	param types.Type) AggFuncExec {
	agg, err := getSingleAggImplByInfo(aggID, param)
	if err != nil {
		panic(err)
	}

	result := agg.ret([]types.Type{param})
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   result,
		emptyNull: agg.setNullForEmptyGroup,
	}

	pIsVarLen, rIsVarLen := param.IsVarlen(), result.IsVarlen()
	if pIsVarLen && rIsVarLen {
		return newAggregatorFromBytesToBytes(mg, info, agg)
	}

	if !pIsVarLen && rIsVarLen {
		return newAggregatorFromFixedToBytes(mg, info, agg)
	}

	if pIsVarLen {
		return newAggregatorFromBytesToFixed(mg, info, agg)
	}
	return newSingleAggFuncExec1NewVersion(mg, info, agg)
}

func makeSpecialAggExec(
	mp *mpool.MPool,
	id int64, isDistinct bool, params ...types.Type,
) (AggFuncExec, bool, error) {
	if _, ok := specialAgg[id]; ok {
		switch id {
		case AggIdOfBitmapConstruct:
			return makeBmpConstructExec(mp, id, params[0]), true, nil
		case AggIdOfBitmapOr:
			return makeBmpOrExec(mp, id, params[0]), true, nil
		case AggIdOfBitXor:
			return makeBitXorExec(mp, id, isDistinct, params[0]), true, nil
		case AggIdOfBitAnd:
			return makeBitAndExec(mp, id, isDistinct, params[0]), true, nil
		case AggIdOfBitOr:
			return makeBitOrExec(mp, id, isDistinct, params[0]), true, nil
		case AggIdOfVarPop:
			return makeVarPopExec(mp, id, isDistinct, params[0]), true, nil
		case AggIdOfStdDevPop:
			return makeStdDevPopExec(mp, id, isDistinct, params[0]), true, nil
		case AggIdOfVarSample:
			return makeVarSampleExec(mp, id, isDistinct, params[0]), true, nil
		case AggIdOfStdDevSample:
			return makeStdDevSampleExec(mp, id, isDistinct, params[0]), true, nil
		case AggIdOfAny:
			return makeAnyValueExec(mp, id, params[0]), true, nil
		case AggIdOfMin:
			return makeMinMaxExec(mp, id, true, params[0]), true, nil
		case AggIdOfMax:
			return makeMinMaxExec(mp, id, false, params[0]), true, nil
		case AggIdOfSum:
			return makeSumAvgExec(mp, true, id, isDistinct, params[0]), true, nil
		case AggIdOfAvg:
			return makeSumAvgExec(mp, false, id, isDistinct, params[0]), true, nil
		case AggIdOfCountColumn:
			return makeCount(mp, false, id, isDistinct, params[0]), true, nil
		case AggIdOfCountStar:
			return makeCount(mp, true, id, isDistinct, params[0]), true, nil
		case AggIdOfMedian:
			exec, err := makeMedian(mp, id, isDistinct, params[0])
			return exec, true, err
		case AggIdOfGroupConcat:
			return makeGroupConcat(mp, id, isDistinct, params, getCroupConcatRet(params...), groupConcatSep), true, nil
		case AggIdOfApproxCount:
			return makeApproxCount(mp, id, params[0]), true, nil
		case AggIdOfJsonArrayAgg:
			exec, err := makeJsonArrayAgg(mp, id, isDistinct, params)
			return exec, true, err
		case AggIdOfJsonObjectAgg:
			exec, err := makeJsonObjectAgg(mp, id, isDistinct, params)
			return exec, true, err
		case WinIdOfRowNumber, WinIdOfRank, WinIdOfDenseRank:
			exec, err := makeWindowExec(mp, id, isDistinct)
			return exec, true, err
		case WinIdOfPercentRank:
			exec, err := makePercentRankExec(mp, id, isDistinct)
			return exec, true, err
		case WinIdOfNtile:
			exec, err := makeNtileExec(mp, id, isDistinct, params)
			return exec, true, err
		case WinIdOfCumeDist:
			exec, err := makeWindowExec(mp, id, isDistinct)
			return exec, true, err
		case WinIdOfLag, WinIdOfLead, WinIdOfFirstValue, WinIdOfLastValue, WinIdOfNthValue:
			exec, err := makeValueWindowExec(mp, id, isDistinct, params)
			return exec, true, err
		}
	}
	return nil, false, nil
}

// makeGroupConcat is one special case of makeMultiAgg.
// it supports creating an aggregation function executor for special aggregation `group_concat()`.
func makeGroupConcat(
	mp *mpool.MPool,
	aggID int64, isDistinct bool,
	param []types.Type, result types.Type,
	separator string) AggFuncExec {
	info := multiAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argTypes:  param,
		retType:   result,
		emptyNull: true,
	}
	return newGroupConcatExec(mp, info, separator)
}

func makeJsonArrayAgg(
	mp *mpool.MPool,
	aggID int64, isDistinct bool,
	param []types.Type) (AggFuncExec, error) {
	if len(param) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("json_arrayagg needs exactly one argument")
	}
	info := multiAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argTypes:  param,
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	return newJsonArrayAggExec(mp, info), nil
}

func makeJsonObjectAgg(
	mp *mpool.MPool,
	aggID int64, isDistinct bool,
	param []types.Type) (AggFuncExec, error) {
	if len(param) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("json_objectagg needs exactly two arguments")
	}
	info := multiAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argTypes:  param,
		retType:   types.T_json.ToType(),
		emptyNull: true,
	}
	return newJsonObjectAggExec(mp, info), nil
}

func makeMedian(
	mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) (AggFuncExec, error) {
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   MedianReturnType([]types.Type{param}),
		emptyNull: true,
	}
	return newMedianExecutor(mp, info)
}

func makeWindowExec(
	mp *mpool.MPool, aggID int64, isDistinct bool) (AggFuncExec, error) {
	if isDistinct {
		return nil, moerr.NewInternalErrorNoCtx("window function does not support `distinct`")
	}

	if aggID == WinIdOfCumeDist {
		info := singleAggInfo{
			aggID:     aggID,
			distinct:  false,
			argType:   types.T_int64.ToType(),
			retType:   types.T_float64.ToType(),
			emptyNull: false,
		}
		return makeCumeDist(mp, info), nil
	}

	info := singleAggInfo{
		aggID:     aggID,
		distinct:  false,
		argType:   types.T_int64.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: false,
	}
	return makeRankDenseRankRowNumber(mp, info), nil
}

func makeValueWindowExec(
	mp *mpool.MPool, aggID int64, isDistinct bool, params []types.Type) (AggFuncExec, error) {
	if isDistinct {
		return nil, moerr.NewInternalErrorNoCtx("window function does not support `distinct`")
	}

	// Determine the return type based on the first parameter
	var retType types.Type
	if len(params) > 0 {
		retType = params[0]
	} else {
		retType = types.T_any.ToType()
	}

	info := singleAggInfo{
		aggID:     aggID,
		distinct:  false,
		argType:   retType,
		retType:   retType,
		emptyNull: true,
	}
	return makeValueWindowExecInternal(mp, info), nil
}

func makeValueWindowExecInternal(mp *mpool.MPool, info singleAggInfo) AggFuncExec {
	return &valueWindowExec{
		singleAggInfo:      info,
		mp:                 mp,
		frameValues:        make([][]*valueEntry, 0),
		currentRowPosition: make([]int, 0),
	}
}

func makeNtileExec(
	mp *mpool.MPool, aggID int64, isDistinct bool, params []types.Type) (AggFuncExec, error) {
	if isDistinct {
		return nil, moerr.NewInternalErrorNoCtx("window function does not support `distinct`")
	}
	if len(params) != 1 {
		return nil, moerr.NewInternalErrorNoCtx("ntile requires exactly one argument")
	}

	info := singleAggInfo{
		aggID:     aggID,
		distinct:  false,
		argType:   params[0],
		retType:   types.T_int64.ToType(),
		emptyNull: false,
	}
	return makeNtileWindowExec(mp, info), nil
}

type dummyBinaryMarshaler struct {
	encoding.BinaryMarshaler
}
type dummyBinaryUnmarshaler struct {
	encoding.BinaryUnmarshaler
}

func (d dummyBinaryMarshaler) MarshalBinary() ([]byte, error) {
	return nil, nil
}
func (d dummyBinaryUnmarshaler) UnmarshalBinary(data []byte) error {
	return nil
}

func marshalRetAndGroupsToBuffer[T encoding.BinaryMarshaler](
	cnt int64, flags [][]uint8, buf *bytes.Buffer,
	ret *optSplitResult, groups []T, extra [][]byte) error {
	types.WriteInt64(buf, cnt)
	if cnt == 0 {
		return nil
	}
	if err := ret.marshalToBuffers(flags, buf); err != nil {
		return err
	}

	if len(groups) == 0 {
		types.WriteInt64(buf, 0)
	} else {
		types.WriteInt64(buf, cnt)
		groupIdx := 0
		for i := range flags {
			for j := range flags[i] {
				if flags[i][j] == 1 {
					bs, err := groups[groupIdx].MarshalBinary()
					if err != nil {
						return err
					}
					if err = types.WriteSizeBytes(bs, buf); err != nil {
						return err
					}
				}
				groupIdx += 1
			}
		}
	}

	cnt = int64(len(extra))
	types.WriteInt64(buf, cnt)
	for i := range extra {
		if err := types.WriteSizeBytes(extra[i], buf); err != nil {
			return err
		}
	}
	return nil
}

func marshalChunkToBuffer[T encoding.BinaryMarshaler](chunk int, buf *bytes.Buffer,
	ret *optSplitResult, groups []T, extra [][]byte) error {
	chunkSz := ret.optInformation.chunkSize
	start := chunkSz * chunk
	chunkNGroup := ret.getNthChunkSize(chunk)
	if chunkSz < 0 {
		return moerr.NewInternalErrorNoCtx("invalid chunk number.")
	}

	cnt := int64(chunkNGroup)
	buf.Write(types.EncodeInt64(&cnt))

	if err := ret.marshalChunkToBuffer(chunk, buf); err != nil {
		return err
	}

	if len(groups) == 0 {
		types.WriteInt64(buf, 0)
	} else {
		types.WriteInt64(buf, cnt)
		for i := 0; i < chunkNGroup; i++ {
			bs, err := groups[start+i].MarshalBinary()
			if err != nil {
				return err
			}
			if err = types.WriteSizeBytes(bs, buf); err != nil {
				return err
			}
		}
	}

	cnt = int64(len(extra))
	types.WriteInt64(buf, cnt)
	for i := range extra {
		if err := types.WriteSizeBytes(extra[i], buf); err != nil {
			return err
		}
	}

	return nil
}

func unmarshalFromReaderNoGroup(reader io.Reader, ret *optSplitResult) error {
	var err error

	cnt, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	ret.optInformation.chunkSize = int(cnt)
	if err := ret.unmarshalFromReader(reader); err != nil {
		return err
	}
	return nil
}

func unmarshalFromReader[T encoding.BinaryUnmarshaler](reader io.Reader, ret *optSplitResult) ([]T, [][]byte, error) {
	err := unmarshalFromReaderNoGroup(reader, ret)
	if err != nil {
		return nil, nil, err
	}

	var res []T
	var extra [][]byte
	// read groups
	cnt, err := types.ReadInt64(reader)
	if err != nil {
		return nil, nil, err
	}
	if cnt != 0 {
		res = make([]T, cnt)
		for i := range res {
			_, bs, err := types.ReadSizeBytes(reader)
			if err != nil {
				return nil, nil, err
			}
			if err = res[i].UnmarshalBinary(bs); err != nil {
				return nil, nil, err
			}
		}
	}

	cnt, err = types.ReadInt64(reader)
	if err != nil {
		return nil, nil, err
	}
	if cnt > 0 {
		extra = make([][]byte, cnt)
		for i := range extra {
			_, bs, err := types.ReadSizeBytes(reader)
			if err != nil {
				return nil, nil, err
			}
			extra[i] = bs
		}
	}

	return res, extra, nil
}

func (ag *AggFuncExecExpression) MarshalToBuffer(buf *bytes.Buffer) error {
	buf.Write(types.EncodeInt64(&ag.aggID))
	buf.Write(types.EncodeBool(&ag.isDistinct))
	argLen := int32(len(ag.argExpressions))
	buf.Write(types.EncodeInt32(&argLen))
	for _, expr := range ag.argExpressions {
		bs, err := proto.Marshal(expr)
		if err != nil {
			return err
		}
		bsLen := int32(len(bs))
		buf.Write(types.EncodeInt32(&bsLen))
		buf.Write(bs)
	}
	exLen := int32(len(ag.extraConfig))
	buf.Write(types.EncodeInt32(&exLen))
	buf.Write(ag.extraConfig)
	return nil
}

func (ag *AggFuncExecExpression) UnmarshalFromReader(r io.Reader) error {
	var err error
	if ag.aggID, err = types.ReadInt64(r); err != nil {
		return err
	}
	if ag.isDistinct, err = types.ReadBool(r); err != nil {
		return err
	}
	argLen, err := types.ReadInt32(r)
	if err != nil {
		return err
	}
	for i := int32(0); i < argLen; i++ {
		_, bs, err := types.ReadSizeBytes(r)
		if err != nil {
			return err
		}
		expr := &plan.Expr{}
		if err := proto.Unmarshal(bs, expr); err != nil {
			return err
		}
		ag.argExpressions = append(ag.argExpressions, expr)
	}
	exLen, err := types.ReadInt32(r)
	if err != nil {
		return err
	}

	// if exLen is 0, the extra config is nil, we SHOULD NOT create a
	// zero length slice, which will cause failure later when people
	// check extraConfig != nil
	if exLen > 0 {
		ag.extraConfig = make([]byte, exLen)
		if _, err := io.ReadFull(r, ag.extraConfig); err != nil {
			return err
		}
	}
	return nil
}
