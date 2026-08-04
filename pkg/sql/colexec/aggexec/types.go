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
	configType     plan.AggregateConfigType
}

func MakeAggFunctionExpression(
	id int64,
	isDistinct bool,
	args []*plan.Expr,
	config []byte,
	configType ...plan.AggregateConfigType,
) AggFuncExecExpression {
	var typ plan.AggregateConfigType
	if len(configType) > 0 {
		typ = configType[0]
	}
	return AggFuncExecExpression{
		aggID:          id,
		isDistinct:     isDistinct,
		argExpressions: args,
		extraConfig:    config,
		configType:     typ,
	}
}

func (ag *AggFuncExecExpression) GetAggID() int64 {
	return ag.aggID
}

// PreservesFirstArgPrepareParamKind reports aggregates whose result is one of
// the first argument's original values without a semantic type conversion.
// Source conversion provenance may cross these materialization boundaries.
func (ag *AggFuncExecExpression) PreservesFirstArgPrepareParamKind() bool {
	switch ag.aggID {
	case AggIdOfMin, AggIdOfMax, AggIdOfAny, AggIdOfMaxBy, AggIdOfMaxByNonNull,
		WinIdOfFirstValue, WinIdOfLastValue, WinIdOfNthValue:
		return true
	case WinIdOfLag, WinIdOfLead:
		// With no explicit default, LAG/LEAD return the first argument or NULL.
		// A default expression can introduce a different source category.
		return len(ag.argExpressions) < 3
	default:
		return false
	}
}

func (ag *AggFuncExecExpression) IsDistinct() bool {
	return ag.isDistinct
}

func (ag *AggFuncExecExpression) GetArgExpressions() []*plan.Expr {
	return ag.argExpressions
}

func (ag *AggFuncExecExpression) RewriteArgExpressions(rewrite func(*plan.Expr) (*plan.Expr, bool, error)) (bool, error) {
	folded := false
	args := make([]*plan.Expr, len(ag.argExpressions))
	copy(args, ag.argExpressions)
	for i := range args {
		expr, exprFolded, err := rewrite(args[i])
		if err != nil {
			return false, err
		}
		if exprFolded {
			args[i] = expr
			folded = true
		}
	}
	if folded {
		ag.argExpressions = args
	}
	return folded, nil
}

func (ag *AggFuncExecExpression) GetExtraConfig() []byte {
	return ag.extraConfig
}

func (ag *AggFuncExecExpression) SetExtraConfig(config []byte) {
	ag.extraConfig = config
}

type AggregateConfig struct {
	Type plan.AggregateConfigType
	Data []byte
}

func (ag *AggFuncExecExpression) GetExtraInformation() any {
	if ag.extraConfig == nil {
		return nil
	}
	if ag.configType == plan.AggregateConfigType_AGG_CONFIG_NONE {
		return ag.extraConfig
	}
	return AggregateConfig{Type: ag.configType, Data: ag.extraConfig}
}

func (ag *AggFuncExecExpression) GetConfigType() plan.AggregateConfigType {
	return ag.configType
}

// AggFuncExec is an interface to do execution for aggregation.
type AggFuncExec interface {
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

// PrepareParamKindStateAccessor is an optional capability implemented by the
// aggregate state backed executors.  It exposes the provenance of the value
// vector without widening AggFuncExec (value-window and other non-serializable
// executors do not have a chunk state to expose).  Group spill/partial codecs
// use this capability to carry the winner category alongside the packed state
// rows.
type PrepareParamKindStateAccessor interface {
	PrepareParamKindsForChunk(chunk int) []vector.PrepareParamKind
	PrepareParamKindsForSelection(flags [][]uint8) []vector.PrepareParamKind
	// Row counts let transient provenance decoders validate an exact record
	// before allocating its row payload. They are intentionally separate from
	// the optional payload accessors because uniform states do not allocate.
	PrepareParamKindRowCountForChunk(chunk int) int
	PrepareParamKindRowCountFlat() int
	PrepareParamKindSummaryForChunk(chunk int) (vector.PrepareParamKind, bool)
	PrepareParamKindSummaryForSelection(flags [][]uint8) (vector.PrepareParamKind, bool)
	RestorePrepareParamKindsForChunk(chunk int, kinds []vector.PrepareParamKind, mp *mpool.MPool) error
	RestorePrepareParamKindsFlat(kinds []vector.PrepareParamKind, mp *mpool.MPool) error
}

// indicate who implements the AggFuncExec interface.
var (
	_ AggFuncExec = &groupConcatExec{}
)

// MakeAgg is the only exporting method to create an aggregation function executor.
func MakeAgg(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	param ...types.Type,
) (AggFuncExec, error) {
	return makeAgg(mg, aggID, isDistinct, false, param...)
}

// MakeAggWithLegacyTextMinMax is used only while decoding a remote pipeline
// during the MORPC v9 -> v10 rollout. It preserves the old bytewise text
// MIN/MAX comparator without changing the argument or result type metadata.
func MakeAggWithLegacyTextMinMax(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	param ...types.Type,
) (AggFuncExec, error) {
	return makeAgg(mg, aggID, isDistinct, true, param...)
}

func makeAgg(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	legacyTextMinMax bool,
	param ...types.Type,
) (AggFuncExec, error) {
	exec, ok, err := makeSpecialAggExec(mg, aggID, isDistinct, legacyTextMinMax, param...)
	if err != nil {
		return nil, err
	}
	if ok {
		return exec, nil
	}
	errmsg := fmt.Sprintf("unexpected aggID %d and param types %v.", aggID, param)
	return nil, moerr.NewInternalErrorNoCtx(errmsg)
}

func makeSpecialAggExec(
	mp *mpool.MPool,
	id int64, isDistinct bool, legacyTextMinMax bool, params ...types.Type,
) (AggFuncExec, bool, error) {
	if id == AggIdOfMaxBy && len(params) != 3 {
		return nil, true, moerr.NewInternalErrorNoCtx("max_by requires value, order, and tie arguments")
	}
	if id == AggIdOfMaxByNonNull && len(params) != 3 {
		return nil, true, moerr.NewInternalErrorNoCtx("max_by_non_null requires value, order, and tie arguments")
	}
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
		return makeMinMaxExecWithLegacyText(mp, id, true, params[0], legacyTextMinMax), true, nil
	case AggIdOfMax:
		return makeMinMaxExecWithLegacyText(mp, id, false, params[0], legacyTextMinMax), true, nil
	case AggIdOfMaxBy:
		return makeMaxByExec(mp, id, false, params), true, nil
	case AggIdOfMaxByNonNull:
		return makeMaxByExec(mp, id, true, params), true, nil
	case AggIdOfSum:
		return makeSumAvgExec(mp, true, id, isDistinct, params[0]), true, nil
	case AggIdOfAvg:
		return makeSumAvgExec(mp, false, id, isDistinct, params[0]), true, nil
	case AggIdOfCountColumn:
		return makeCount(mp, false, id, isDistinct, params), true, nil
	case AggIdOfCountStar:
		return makeCount(mp, true, id, isDistinct, params), true, nil
	case AggIdOfMedian:
		exec, err := makeMedian(mp, id, isDistinct, params[0])
		return exec, true, err
	case AggIdOfGroupConcat:
		return makeGroupConcat(mp, id, isDistinct, params, GroupConcatReturnType(params), ","), true, nil
	case AggIdOfApproxCount, AggIdOfApproxCountDistinct:
		return makeApproxCount(mp, id, params[0]), true, nil
	case AggIdOfHllAdd:
		return makeHllAdd(mp, id, params[0]), true, nil
	case AggIdOfHllMerge:
		return makeHllMerge(mp, id, params[0]), true, nil
	case AggIdOfApproxPercentile:
		exec, err := makeApproxPercentile(mp, id, isDistinct, params[0])
		return exec, true, err
	case AggIdOfJsonArrayAgg:
		exec, err := makeJsonArrayAgg(mp, id, isDistinct, params)
		return exec, true, err
	case AggIdOfJsonObjectAgg:
		exec, err := makeJsonObjectAgg(mp, id, isDistinct, params)
		return exec, true, err
	case AggIdOfAvgTwCache:
		exec, err := makeAvgTwCacheExec(mp, id, params[0])
		return exec, true, err
	case AggIdOfAvgTwResult:
		exec, err := makeAvgTwResultExec(mp, id, params[0])
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
	return newMedianExec(mp, aggID, isDistinct, param)
}

func makeApproxPercentile(
	mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) (AggFuncExec, error) {
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		emptyNull: true,
	}
	switch param.Oid {
	case types.T_decimal64, types.T_decimal128:
		info.retType = ApproxPercentileReturnType([]types.Type{param})
	default:
		info.retType = types.T_float64.ToType()
	}
	return newApproxPercentileExec(mp, info)
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
