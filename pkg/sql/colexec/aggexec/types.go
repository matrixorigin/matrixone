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
	SaveIntermediateResult(cnt int64, flags [][]uint8, writer io.Writer) error
	SaveIntermediateResultOfChunk(chunk int, writer io.Writer) error
	UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error

	Size() int64

	// Free clean the resource and reuse the aggregation if possible.
	Free()
}

// sourcePreservingMerger is implemented only by aggregate executors whose
// Merge method leaves the source group unchanged. Merge normally permits
// ownership transfer because distributed aggregation consumes partial states;
// window execution needs the stronger contract when it snapshots a running
// aggregate into multiple result groups.
type sourcePreservingMerger interface {
	sourcePreservingMerge()
}

// MergePreservesSource reports whether repeated merges can safely snapshot
// exec without consuming or otherwise mutating its source group.
func MergePreservesSource(exec AggFuncExec) bool {
	_, ok := exec.(sourcePreservingMerger)
	return ok
}

// windowSlidingAggregator is implemented by aggregates that can remove the
// oldest row from a one-group running state. It is intentionally narrower
// than AggFuncExec: only aggregates with an exact inverse for the supported
// argument type may opt in.
type windowSlidingAggregator interface {
	windowSlidingSupported() bool
	addWindowRow(row int, vectors []*vector.Vector) error
	removeWindowRow(row int, vectors []*vector.Vector) error
}

// SupportsWindowSliding reports whether exec can maintain a bounded window by
// adding the entering row and removing the leaving row. Callers must use a
// one-group executor and advance the frame monotonically.
func SupportsWindowSliding(exec AggFuncExec) bool {
	sliding, ok := exec.(windowSlidingAggregator)
	return ok && sliding.windowSlidingSupported()
}

// AddWindowRow adds one row to a one-group sliding aggregate.
func AddWindowRow(exec AggFuncExec, row int, vectors []*vector.Vector) error {
	sliding, ok := exec.(windowSlidingAggregator)
	if !ok || !sliding.windowSlidingSupported() {
		return moerr.NewInternalErrorNoCtx("aggregate does not support sliding windows")
	}
	return sliding.addWindowRow(row, vectors)
}

// RemoveWindowRow removes one row from a one-group sliding aggregate.
func RemoveWindowRow(exec AggFuncExec, row int, vectors []*vector.Vector) error {
	sliding, ok := exec.(windowSlidingAggregator)
	if !ok || !sliding.windowSlidingSupported() {
		return moerr.NewInternalErrorNoCtx("aggregate does not support sliding windows")
	}
	return sliding.removeWindowRow(row, vectors)
}

// GroupAggFuncExec is the complete contract required by Group. Keeping this
// separate from AggFuncExec lets window executors retain the common aggregate
// API without making Group rediscover its stronger allocation, preflight,
// spill, and prepared-parameter contracts at every call site.
//
// Once SetAllocationAccount succeeds, all fallible physical growth must be
// covered by PreflightBatchFill/PreflightBatchMerge and the spill codec must be
// usable without an unaccounted data-sized staging allocation.
type GroupAggFuncExec interface {
	AggFuncExec
	AllocationAccountOwner
	BatchCapacityPreflight
	SpillStateCodec
	PrepareParamKindStateAccessor

	AdditionalMemorySize() int64
	GetNumGroups() int
	SetPrepareParamKind(vector.PrepareParamKind)
}

// AllocationAccountOwner is implemented by aggregate executors whose complete
// retained state can participate in an operator's physical allocation
// account.  It is deliberately separate from AggFuncExec: callers that do not
// install a statement account keep the existing aggregate API, while an
// accountable operator must reject an executor that does not implement this
// closed lifecycle contract.
type AllocationAccountOwner interface {
	SetAllocationAccount(*AllocationAccount) error
	ClearAllocationAccount(*AllocationAccount) error
}

// BatchCapacityPreflight reserves every fallible physical allocation a bounded
// Group hash work unit can require after its non-mutating group preview is
// known, without changing aggregate values. Group can therefore spill its
// resident prefix and retry the same unpublished input unit on capacity
// pressure.
type BatchCapacityPreflight interface {
	PreflightBatchFill(offset int, groups []uint64, vectors []*vector.Vector) error
	PreflightBatchMerge(next AggFuncExec, offset int, groups []uint64) error
}

// SpillStateCodec is the bounded-memory, execution-local aggregate codec.
// It is separate from the stable intermediate-result codec because spill must
// stream selected physical rows without first allocating a selection Vector.
// MakeGroupAgg admits only executors that implement this closed spill contract.
type SpillStateCodec interface {
	SaveSpillIntermediateRows(chunk int, rows []int32, writer io.Writer) error
	UnmarshalSpillFromReader(reader io.Reader, mp *mpool.MPool) error
}

// ExactCountDistinctSpillState exposes the narrow ownership transfer required
// by Group's bounded exact COUNT(DISTINCT ...) spill path. BeginArgumentDrain
// validates and freezes the drain view without allocating a replacement for
// every aggregate chunk. Commit installs bounded empty replacements one chunk
// at a time after the caller has written the private spill wave; Abort keeps
// the resident state authoritative.
//
// Argument payloads use the aggregate's existing canonical key grammar without
// the chunk-local group prefix. InsertDistinctArgument accepts only payloads
// produced by a compatible drain or spill decoder.
type ExactCountDistinctSpillState interface {
	GroupAggFuncExec
	SupportsExactCountDistinctSpill() bool
	HasDistinctArguments() (bool, error)
	DistinctArgumentStats() (keys uint64, retainedBytes uint64, err error)
	BeginArgumentDrain(replacement *AllocationAccount) (DistinctArgumentDrain, error)
	RehomeDistinctArgumentState(allocation *AllocationAccount) error
	InsertDistinctArgument(group int, payload []byte) error
	AddDistinctCountContribution(
		group int,
		count uint64,
		allocation *AllocationAccount,
	) error
}

// DistinctArgumentDrain is a single-use prepared ownership transfer. Payload
// slices passed to ForEach point into the resident arena and are valid only for
// the duration of the callback.
type DistinctArgumentDrain interface {
	ForEach(func(group int, payload []byte) error) error
	KeyCount() uint64
	RetainedBytes() uint64
	Commit() error
	Abort()
}

// PrepareParamKindStateAccessor exposes the result vectors whose winner
// provenance is carried by Group's spill/partial wire extension. Group owns
// the codec and streams directly between these vectors and the wire; the
// aggregate does not materialize a second row-sized metadata slice.
type PrepareParamKindStateAccessor interface {
	PrepareParamKindChunkCount() int
	PrepareParamKindVectorForChunk(chunk int) *vector.Vector
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
	return makeAgg(mg, aggID, isDistinct, false, false, param...)
}

// MakeGroupAgg constructs an aggregate that satisfies Group's complete static
// execution contract. Window-function executors are deliberately rejected.
func MakeGroupAgg(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	allocation *AllocationAccount,
	extraInformation any,
	param ...types.Type,
) (GroupAggFuncExec, error) {
	return makeGroupAgg(
		mg, aggID, isDistinct, false, false, false, allocation, extraInformation, param...)
}

// MakeSingleGroupAgg constructs an aggregate for an execution path whose
// cardinality is statically bounded to one group. Aggregates may use this
// stronger contract to select representations that would not be safe for an
// unbounded GROUP BY.
func MakeSingleGroupAgg(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	allocation *AllocationAccount,
	extraInformation any,
	param ...types.Type,
) (GroupAggFuncExec, error) {
	return makeGroupAgg(
		mg, aggID, isDistinct, false, false, true, allocation, extraInformation, param...)
}

// MakeAggWithLegacyTextMinMax is used only while decoding a remote pipeline
// during the MORPC v10 -> v11 rollout. It preserves the old bytewise text
// MIN/MAX comparator without changing the argument or result type metadata.
func MakeAggWithLegacyTextMinMax(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	param ...types.Type,
) (AggFuncExec, error) {
	return makeAgg(mg, aggID, isDistinct, true, false, param...)
}

// MakeGroupAggWithLegacyTextMinMax is the Group-specific counterpart of
// MakeAggWithLegacyTextMinMax for mixed-version remote pipelines.
func MakeGroupAggWithLegacyTextMinMax(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	allocation *AllocationAccount,
	extraInformation any,
	param ...types.Type,
) (GroupAggFuncExec, error) {
	return makeGroupAgg(
		mg, aggID, isDistinct, true, false, false, allocation, extraInformation, param...)
}

// MakeSingleGroupAggWithLegacyTextMinMax combines the static single-group
// contract with mixed-version text MIN/MAX compatibility.
func MakeSingleGroupAggWithLegacyTextMinMax(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	allocation *AllocationAccount,
	extraInformation any,
	param ...types.Type,
) (GroupAggFuncExec, error) {
	return makeGroupAgg(
		mg, aggID, isDistinct, true, false, true, allocation, extraInformation, param...)
}

// MakeGroupAggWithLegacyRemoteState selects aggregate implementations whose
// partial-state layout is understood by pre-upgrade CNs. It is used only for
// a remotely decoded pipeline while the deployment protocol gate is below the
// version that introduced a new aggregate state layout.
func MakeGroupAggWithLegacyRemoteState(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	legacyTextMinMax bool, legacyVarianceState bool,
	allocation *AllocationAccount,
	extraInformation any,
	param ...types.Type,
) (GroupAggFuncExec, error) {
	return makeGroupAgg(
		mg, aggID, isDistinct, legacyTextMinMax, legacyVarianceState, false,
		allocation, extraInformation, param...)
}

func MakeSingleGroupAggWithLegacyRemoteState(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	legacyTextMinMax bool, legacyVarianceState bool,
	allocation *AllocationAccount,
	extraInformation any,
	param ...types.Type,
) (GroupAggFuncExec, error) {
	return makeGroupAgg(
		mg, aggID, isDistinct, legacyTextMinMax, legacyVarianceState, true,
		allocation, extraInformation, param...)
}

type singleGroupAggregate interface {
	setSingleGroupExecution() error
}

func makeGroupAgg(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	legacyTextMinMax bool,
	legacyVarianceState bool,
	singleGroup bool,
	allocation *AllocationAccount,
	extraInformation any,
	param ...types.Type,
) (GroupAggFuncExec, error) {
	exec, err := makeAgg(mg, aggID, isDistinct, legacyTextMinMax, legacyVarianceState, param...)
	if err != nil {
		return nil, err
	}
	groupExec, ok := exec.(GroupAggFuncExec)
	if !ok {
		exec.Free()
		return nil, moerr.NewNotSupportedNoCtxf(
			"aggregate %d does not support group execution", aggID)
	}
	if singleGroup {
		if configurable, ok := groupExec.(singleGroupAggregate); ok {
			if err := configurable.setSingleGroupExecution(); err != nil {
				groupExec.Free()
				return nil, err
			}
		}
	}
	if extraInformation != nil {
		if err := groupExec.SetExtraInformation(extraInformation, 0); err != nil {
			groupExec.Free()
			return nil, err
		}
	}
	if allocation != nil {
		if err := groupExec.SetAllocationAccount(allocation); err != nil {
			groupExec.Free()
			return nil, err
		}
	}
	return groupExec, nil
}

func makeAgg(
	mg *mpool.MPool,
	aggID int64, isDistinct bool,
	legacyTextMinMax bool,
	legacyVarianceState bool,
	param ...types.Type,
) (AggFuncExec, error) {
	exec, ok, err := makeSpecialAggExec(mg, aggID, isDistinct, legacyTextMinMax, legacyVarianceState, param...)
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
	id int64, isDistinct bool, legacyTextMinMax bool, legacyVariance bool, params ...types.Type,
) (AggFuncExec, bool, error) {
	if isDistinct &&
		(id == AggIdOfBitAnd || id == AggIdOfBitOr || id == AggIdOfBitXor) {
		return nil, true, moerr.NewNotSupportedNoCtx(
			"distinct bit operations are not supported")
	}
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
		return makeVarPopExec(mp, id, isDistinct, params[0], legacyVariance), true, nil
	case AggIdOfStdDevPop:
		return makeStdDevPopExec(mp, id, isDistinct, params[0], legacyVariance), true, nil
	case AggIdOfVarSample:
		return makeVarSampleExec(mp, id, isDistinct, params[0], legacyVariance), true, nil
	case AggIdOfStdDevSample:
		return makeStdDevSampleExec(mp, id, isDistinct, params[0], legacyVariance), true, nil
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
	case AggIdOfPercentileCont:
		if len(params) != 1 {
			return nil, true, moerr.NewInternalErrorNoCtx("percentile_cont requires one value argument")
		}
		exec, err := makeOrderedPercentileExec(mp, id, isDistinct, params[0], orderedPercentileContinuous)
		return exec, true, err
	case AggIdOfPercentileDisc:
		if len(params) != 1 {
			return nil, true, moerr.NewInternalErrorNoCtx("percentile_disc requires one value argument")
		}
		exec, err := makeOrderedPercentileExec(mp, id, isDistinct, params[0], orderedPercentileDiscrete)
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
		retType:   types.T_uint64.ToType(),
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
		if err := expr.ValidateStringLiteralForms(); err != nil {
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
