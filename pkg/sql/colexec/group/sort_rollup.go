// Copyright 2026 Matrix Origin
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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// sortRollupState is a bounded streaming state machine. There is one
// single-group aggregate executor for every ROLLUP prefix, plus at most one
// input batch and one output batch. The state therefore depends on the number
// of rollup levels, not on the number of input rows or groups.
//
// A prefix is kept open until the sorted input changes one of its keys. At a
// boundary, longer prefixes are flushed before the next row is accepted. This
// is exactly the order required by the grouping-set expansion:
//
//	(a,b) detail, (a) subtotal, then the next (a,c) detail.
type sortRollupState struct {
	keyIndices     []int
	logicalForExpr []int
	equalers       []sortRollupValueEqualer
	lastExprKeys   []*vector.Vector
	rollupSources  []*vector.Vector

	// levelAggs[p] owns the current aggregate for the grouping prefix of
	// length p. Prefix zero is the grand total and is always present.
	levelAggs [][]aggexec.GroupAggFuncExec
	groupIDs  []uint64

	// Input batches are retained only while an output batch became full in the
	// middle of processing them. The child owns the batch; the state only holds
	// the pointer until the next call.
	input                     *batch.Batch
	inputRow                  int
	inputStartBoundaryHandled bool

	// pendingClose drains prefixes in descending order. A separate boolean is
	// needed because prefix zero is a valid pending value.
	pendingClose     bool
	pendingCloseNext int
	pendingCloseMin  int

	output     *batch.Batch
	outputRows int
	// lastOutput is the ownership bridge between Call and its consumer. A
	// returned batch is borrowed by the caller until the next Call, after which
	// the streaming operator can reclaim it. Keeping only this one reference
	// preserves the bounded-memory contract without asking every parent
	// operator to know about sort-rollup internals.
	lastOutput *batch.Batch

	finalizing bool
	finalized  bool
	haveLast   bool
}

const sortRollupGroupIDBytes = int64(8)

func (s *sortRollupState) init(group *Group) error {
	if group == nil || len(group.GroupBy) == 0 {
		return moerr.NewInternalErrorNoCtx("sort rollup requires at least one grouping key")
	}

	logicalCount := len(group.GroupBy)
	if len(group.GroupByHashKey) > 0 {
		logicalCount = len(group.GroupByHashKey)
	}
	if logicalCount == 0 || logicalCount > len(group.GroupBy) {
		return moerr.NewInternalErrorNoCtx("invalid sort rollup grouping key count")
	}

	s.keyIndices = make([]int, logicalCount)
	s.equalers = make([]sortRollupValueEqualer, logicalCount)
	for i := 0; i < logicalCount; i++ {
		idx := i
		if len(group.GroupByHashKey) > 0 {
			idx = int(group.GroupByHashKey[i])
		}
		if idx < 0 || idx >= len(group.ctr.groupByEvaluate.Typ) {
			return moerr.NewInternalErrorNoCtx("sort rollup grouping key index out of range")
		}
		s.keyIndices[i] = idx
		typ := group.ctr.groupByEvaluate.Typ[idx]
		if !sortRollupTypeEligible(typ) {
			return moerr.NewNotSupportedNoCtxf(
				"sort rollup does not support grouping key type %s", typ.String())
		}
		equaler := newSortRollupValueEqualer(typ)
		if equaler == nil {
			return moerr.NewInternalErrorNoCtxf(
				"sort rollup has no equality fast path for type %s", typ.String())
		}
		s.equalers[i] = equaler
		rollup, err := vector.NewRollupConstWithAllocation(
			typ,
			1,
			group.ctr.mp,
			group.ctr.expressionAllocation,
		)
		if err != nil {
			s.free(group.ctr.mp)
			return err
		}
		s.rollupSources = append(s.rollupSources, rollup)
	}

	// groupByHashKey maps each visible key to a physical equality key. Hidden
	// keys must roll up together with their visible counterpart, otherwise a
	// pad-space key would expose an extra subtotal column in the physical batch.
	s.logicalForExpr = make([]int, len(group.GroupBy))
	for i := range s.logicalForExpr {
		s.logicalForExpr[i] = -1
	}
	for logical, idx := range s.keyIndices {
		s.logicalForExpr[logical] = logical
		if idx >= 0 && idx < len(s.logicalForExpr) {
			s.logicalForExpr[idx] = logical
		}
	}
	for expr, logical := range s.logicalForExpr {
		if logical < 0 {
			return moerr.NewInternalErrorNoCtxf(
				"sort rollup grouping expression %d has no equality key", expr)
		}
	}

	group.ctr.groupByTypes = append(group.ctr.groupByTypes[:0], group.ctr.groupByEvaluate.Typ...)
	s.lastExprKeys = make([]*vector.Vector, len(group.GroupBy))
	s.groupIDs = make([]uint64, hashmap.UnitLimit)
	for i := range s.groupIDs {
		s.groupIDs[i] = 1
	}

	// prepareGroupAndAggArg builds the normal multi-group list before it knows
	// that this operator is the streaming path. Release that unused list before
	// allocating the bounded per-prefix single-group lists.
	group.ctr.freeAggList()
	s.levelAggs = make([][]aggexec.GroupAggFuncExec, logicalCount+1)
	if len(group.Aggs) > 0 {
		for level := range s.levelAggs {
			list, err := group.ctr.makeSingleGroupAggList(group.Aggs)
			if err != nil {
				s.free(group.ctr.mp)
				return err
			}
			s.levelAggs[level] = list
		}
	}
	return nil
}

func (s *sortRollupState) free(mp *mpool.MPool) {
	for i := range s.lastExprKeys {
		if s.lastExprKeys[i] != nil {
			s.lastExprKeys[i].Free(mp)
			s.lastExprKeys[i] = nil
		}
	}
	for i := range s.rollupSources {
		if s.rollupSources[i] != nil {
			s.rollupSources[i].Free(mp)
			s.rollupSources[i] = nil
		}
	}
	for level := range s.levelAggs {
		for i := range s.levelAggs[level] {
			if s.levelAggs[level][i] != nil {
				s.levelAggs[level][i].Free()
				s.levelAggs[level][i] = nil
			}
		}
		s.levelAggs[level] = nil
	}
	if s.output != nil {
		s.output.Clean(mp)
		s.output = nil
	}
	if s.lastOutput != nil {
		s.lastOutput.Clean(mp)
		s.lastOutput = nil
	}
	s.lastExprKeys = nil
	s.rollupSources = nil
	s.levelAggs = nil
	s.groupIDs = nil
	s.keyIndices = nil
	s.logicalForExpr = nil
	s.equalers = nil
	s.input = nil
	s.inputRow = 0
	s.inputStartBoundaryHandled = false
	s.pendingClose = false
	s.outputRows = 0
	s.finalizing = false
	s.finalized = false
	s.haveLast = false
}

// sortRollupTypeEligible is intentionally narrower than the set of types
// accepted by hash grouping. The sort child uses SQL ORDER semantics, while
// hash grouping uses the equality key codec; FLOAT and JSON are known to have
// representation/equality cases where those relations are not identical.
func sortRollupTypeEligible(typ types.Type) bool {
	switch typ.Oid {
	case types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year,
		types.T_uuid, types.T_enum:
		return true
	default:
		return false
	}
}

func (s *sortRollupState) prepareWithinBatch(keyVecs []*vector.Vector) {
	for i, keyIdx := range s.keyIndices {
		s.equalers[i].Set(keyVecs[keyIdx], keyVecs[keyIdx])
	}
}

func (s *sortRollupState) commonPrefixWithinBatch(row, previousRow int) int {
	common := 0
	for i := range s.keyIndices {
		if !s.equalers[i].Equal(previousRow, row) {
			break
		}
		common++
	}
	return common
}

func (s *sortRollupState) commonPrefixFromLast(
	keyVecs []*vector.Vector,
	row int,
) int {
	common := 0
	for i, keyIdx := range s.keyIndices {
		last := s.lastExprKeys[keyIdx]
		if last == nil {
			break
		}
		s.equalers[i].Set(keyVecs[keyIdx], last)
		if !s.equalers[i].Equal(row, 0) {
			break
		}
		common++
	}
	return common
}

type sortRollupValueEqualer interface {
	Set(left, right *vector.Vector)
	Equal(left, right int) bool
}

// sortRollupFixedEqualer caches the typed backing slices once per input batch.
// Calling MustFixedColNoTypeCheck for every adjacent row looks simple but
// rebuilds those slices in the boundary hot loop; keeping them here makes the
// equality check a typed load plus an optional bitmap lookup.
type sortRollupFixedEqualer[T comparable] struct {
	left, right []T

	leftNulls, rightNulls         *nulls.Nulls
	leftGrouping, rightGrouping   *nulls.Nulls
	leftConst, rightConst         bool
	leftConstNull, rightConstNull bool
	leftHasNull, rightHasNull     bool
}

func (e *sortRollupFixedEqualer[T]) Set(left, right *vector.Vector) {
	e.left = vector.MustFixedColNoTypeCheck[T](left)
	e.right = vector.MustFixedColNoTypeCheck[T](right)
	e.leftConst, e.rightConst = left.IsConst(), right.IsConst()
	e.leftConstNull, e.rightConstNull = left.IsConstNull(), right.IsConstNull()
	e.leftNulls, e.rightNulls = left.GetNulls(), right.GetNulls()
	e.leftGrouping, e.rightGrouping = left.GetGrouping(), right.GetGrouping()
	e.leftHasNull = e.leftConstNull || !e.leftNulls.IsEmpty() || !e.leftGrouping.IsEmpty()
	e.rightHasNull = e.rightConstNull || !e.rightNulls.IsEmpty() || !e.rightGrouping.IsEmpty()
}

func (e *sortRollupFixedEqualer[T]) Equal(left, right int) bool {
	if e.leftConst {
		left = 0
	}
	if e.rightConst {
		right = 0
	}
	leftNull := e.leftConstNull
	if !leftNull && e.leftHasNull {
		leftNull = e.leftNulls.Contains(uint64(left)) || e.leftGrouping.Contains(uint64(left))
	}
	rightNull := e.rightConstNull
	if !rightNull && e.rightHasNull {
		rightNull = e.rightNulls.Contains(uint64(right)) || e.rightGrouping.Contains(uint64(right))
	}
	if leftNull || rightNull {
		return leftNull && rightNull
	}
	return e.left[left] == e.right[right]
}

func newSortRollupValueEqualer(typ types.Type) sortRollupValueEqualer {
	switch typ.Oid {
	case types.T_bool:
		return &sortRollupFixedEqualer[bool]{}
	case types.T_bit:
		return &sortRollupFixedEqualer[uint64]{}
	case types.T_int8:
		return &sortRollupFixedEqualer[int8]{}
	case types.T_int16:
		return &sortRollupFixedEqualer[int16]{}
	case types.T_int32:
		return &sortRollupFixedEqualer[int32]{}
	case types.T_int64:
		return &sortRollupFixedEqualer[int64]{}
	case types.T_uint8:
		return &sortRollupFixedEqualer[uint8]{}
	case types.T_uint16:
		return &sortRollupFixedEqualer[uint16]{}
	case types.T_uint32:
		return &sortRollupFixedEqualer[uint32]{}
	case types.T_uint64:
		return &sortRollupFixedEqualer[uint64]{}
	case types.T_decimal64:
		return &sortRollupFixedEqualer[types.Decimal64]{}
	case types.T_decimal128:
		return &sortRollupFixedEqualer[types.Decimal128]{}
	case types.T_decimal256:
		return &sortRollupFixedEqualer[types.Decimal256]{}
	case types.T_date:
		return &sortRollupFixedEqualer[types.Date]{}
	case types.T_time:
		return &sortRollupFixedEqualer[types.Time]{}
	case types.T_datetime:
		return &sortRollupFixedEqualer[types.Datetime]{}
	case types.T_timestamp:
		return &sortRollupFixedEqualer[types.Timestamp]{}
	case types.T_year:
		return &sortRollupFixedEqualer[types.MoYear]{}
	case types.T_uuid:
		return &sortRollupFixedEqualer[types.Uuid]{}
	case types.T_enum:
		return &sortRollupFixedEqualer[types.Enum]{}
	default:
		return nil
	}
}

func (s *sortRollupState) updateLastKeys(
	group *Group,
	keyVecs []*vector.Vector,
	row int,
) error {
	for i := range group.GroupBy {
		if s.lastExprKeys[i] == nil {
			vec, err := vector.NewOffHeapVecWithTypeAndAllocation(
				group.ctr.groupByEvaluate.Typ[i],
				group.ctr.expressionAllocation,
			)
			if err != nil {
				return err
			}
			s.lastExprKeys[i] = vec
		} else {
			s.lastExprKeys[i].CleanOnlyData()
		}
		if err := s.lastExprKeys[i].UnionOne(keyVecs[i], int64(row), group.ctr.mp); err != nil {
			return err
		}
	}
	s.haveLast = true
	return nil
}

func (s *sortRollupState) ensureOutput(group *Group) error {
	if s.output != nil {
		return nil
	}
	output, err := group.ctr.createNewGroupByBatch(nil, aggBatchSize)
	if err != nil {
		return err
	}
	s.output = output
	s.outputRows = 0
	return nil
}

func freeSortRollupVectors(vecs []*vector.Vector, mp *mpool.MPool) {
	for _, vec := range vecs {
		if vec != nil {
			vec.Free(mp)
		}
	}
}

func freeSortRollupArgWindows(
	windows [][]*vector.Vector,
	owned [][]bool,
	mp *mpool.MPool,
) {
	for i := range windows {
		for j, vec := range windows[i] {
			if vec != nil && i < len(owned) && j < len(owned[i]) && owned[i][j] {
				vec.Free(mp)
			}
		}
	}
}

// appendOutputRow materializes exactly one finished ROLLUP row. Aggregate
// Flush transfers ownership of the result vector, so the first row can attach
// those vectors directly and later rows append into them with UnionOne.
func (s *sortRollupState) appendOutputRow(
	group *Group,
	proc *process.Process,
	prefixLen int,
) error {
	if s.outputRows >= aggBatchSize {
		return nil
	}
	if err := s.ensureOutput(group); err != nil {
		return err
	}

	keyCount := len(group.GroupBy)
	for col := 0; col < keyCount; col++ {
		logical := s.logicalForExpr[col]
		var source *vector.Vector
		if logical >= 0 && logical < prefixLen {
			source = s.lastExprKeys[col]
		} else {
			source = s.rollupSources[logical]
		}
		if source == nil {
			return moerr.NewInternalErrorNoCtx("sort rollup output key is not initialized")
		}
		if err := s.output.Vecs[col].UnionOne(source, 0, group.ctr.mp); err != nil {
			return err
		}
	}

	var resultVecs []*vector.Vector
	if len(group.Aggs) > 0 {
		if prefixLen < 0 || prefixLen >= len(s.levelAggs) {
			return moerr.NewInternalErrorNoCtx("sort rollup aggregate prefix out of range")
		}
		for i, agg := range s.levelAggs[prefixLen] {
			vecs, err := aggexec.FlushWithContext(proc.Ctx, agg)
			if err != nil {
				freeSortRollupVectors(resultVecs, group.ctr.mp)
				return err
			}
			if len(vecs) != 1 || vecs[0] == nil || vecs[0].Length() != 1 {
				freeSortRollupVectors(vecs, group.ctr.mp)
				freeSortRollupVectors(resultVecs, group.ctr.mp)
				return moerr.NewInternalErrorNoCtxf(
					"sort rollup aggregate %d did not produce one result row", i)
			}
			vec := vecs[0]
			if !vec.HasPrepareParamKind() {
				vec.SetPrepareParamKind(group.ctr.prepareParamKind.Get(i))
			}
			resultVecs = append(resultVecs, vec)
		}
	}

	if len(resultVecs) > 0 {
		if s.outputRows == 0 {
			s.output.Vecs = append(s.output.Vecs, resultVecs...)
		} else {
			for i, vec := range resultVecs {
				if err := s.output.Vecs[keyCount+i].UnionOne(vec, 0, group.ctr.mp); err != nil {
					freeSortRollupVectors(resultVecs[i:], group.ctr.mp)
					return err
				}
				vec.Free(group.ctr.mp)
			}
		}
	}

	for _, agg := range s.levelAggs[prefixLen] {
		aggexec.ReportGroupConcatWarnings(agg, proc.GetWarningSink())
	}
	s.output.AddRowCount(1)
	s.outputRows++
	return nil
}

func (s *sortRollupState) resetLevel(
	group *Group,
	prefixLen int,
) error {
	for i := range s.levelAggs[prefixLen] {
		if s.levelAggs[prefixLen][i] != nil {
			s.levelAggs[prefixLen][i].Free()
			s.levelAggs[prefixLen][i] = nil
		}
	}
	if len(group.Aggs) == 0 {
		return nil
	}
	list, err := group.ctr.makeSingleGroupAggList(group.Aggs)
	if err != nil {
		return err
	}
	s.levelAggs[prefixLen] = list
	return nil
}

func (s *sortRollupState) startClose(commonPrefix int) {
	levels := len(s.levelAggs) - 1
	minPrefix := commonPrefix + 1
	if minPrefix > levels {
		return
	}
	s.pendingClose = true
	s.pendingCloseNext = levels
	s.pendingCloseMin = minPrefix
}

func (group *Group) drainSortRollupClose(proc *process.Process) error {
	s := group.ctr.sortRollup
	for s.pendingClose && s.pendingCloseNext >= s.pendingCloseMin {
		if s.outputRows >= aggBatchSize {
			return nil
		}
		prefix := s.pendingCloseNext
		if err := s.appendOutputRow(group, proc, prefix); err != nil {
			return err
		}
		if err := s.resetLevel(group, prefix); err != nil {
			return err
		}
		s.pendingCloseNext--
	}
	if s.pendingClose && s.pendingCloseNext < s.pendingCloseMin {
		s.pendingClose = false
		s.pendingCloseNext = 0
		s.pendingCloseMin = 0
	}
	return nil
}

func (group *Group) fillSortRollupLevels(
	proc *process.Process,
	offset int,
	rows int,
) error {
	if len(group.Aggs) == 0 || rows == 0 {
		return nil
	}
	s := group.ctr.sortRollup
	if cap(s.groupIDs) < hashmap.UnitLimit {
		s.groupIDs = make([]uint64, hashmap.UnitLimit)
		for i := range s.groupIDs {
			s.groupIDs[i] = 1
		}
	}
	end := offset + rows
	// Every live streaming prefix owns exactly one aggregate group. The
	// general BatchFill path still has to dispatch every row through its group
	// id hash table, even though all ids are 1. Preflight the same chunks for
	// allocation-account correctness, then use the aggregate's single-group
	// BulkFill once for the complete run. Windows are borrowed views, so the
	// input is not copied when a run starts after a previous key boundary.
	for start := offset; start < end; {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		n := min(hashmap.UnitLimit, end-start)
		groups := s.groupIDs[:n]
		for prefix := range s.levelAggs {
			for i, agg := range s.levelAggs[prefix] {
				if err := agg.PreflightBatchFill(
					start,
					groups,
					group.ctr.aggArgEvaluate[i].Vec,
				); err != nil {
					return err
				}
			}
		}
		start += n
	}

	argWindows := make([][]*vector.Vector, len(group.ctr.aggArgEvaluate))
	argWindowOwned := make([][]bool, len(group.ctr.aggArgEvaluate))
	for i, evaluated := range group.ctr.aggArgEvaluate {
		argWindows[i] = make([]*vector.Vector, len(evaluated.Vec))
		argWindowOwned[i] = make([]bool, len(evaluated.Vec))
		for j, vec := range evaluated.Vec {
			if offset == 0 && vec.Length() == rows {
				argWindows[i][j] = vec
				continue
			}
			var window *vector.Vector
			var err error
			if group.ctr.expressionAllocation != nil {
				window, err = vec.WindowByLogicalRowsWithAllocation(
					offset, end, group.ctr.mp, group.ctr.expressionAllocation)
			} else {
				window, err = vec.WindowByLogicalRows(offset, end)
			}
			if err != nil {
				freeSortRollupArgWindows(argWindows, argWindowOwned, group.ctr.mp)
				return err
			}
			argWindows[i][j] = window
			argWindowOwned[i][j] = true
		}
	}
	for prefix := range s.levelAggs {
		for i, agg := range s.levelAggs[prefix] {
			if err := agg.BulkFill(0, argWindows[i]); err != nil {
				freeSortRollupArgWindows(argWindows, argWindowOwned, group.ctr.mp)
				return err
			}
		}
	}
	freeSortRollupArgWindows(argWindows, argWindowOwned, group.ctr.mp)
	return nil
}

func (group *Group) processSortRollupInput(proc *process.Process) error {
	s := group.ctr.sortRollup
	if s.input == nil {
		return nil
	}
	rows := s.input.RowCount()
	if rows == 0 {
		s.input = nil
		s.inputRow = 0
		return nil
	}
	keyVecs := group.ctr.groupByEvaluate.Vec
	s.prepareWithinBatch(keyVecs)
	logicalCount := len(s.levelAggs) - 1

	for s.inputRow < rows {
		if s.pendingClose {
			if err := group.drainSortRollupClose(proc); err != nil {
				return err
			}
			if s.outputRows >= aggBatchSize {
				return nil
			}
		}

		start := s.inputRow
		if start == 0 && s.haveLast && !s.inputStartBoundaryHandled {
			// Mark the boundary before draining it. Draining may yield because
			// the output batch became full; on resume the same input row must not
			// close the already-reset prefix a second time.
			s.inputStartBoundaryHandled = true
			common := s.commonPrefixFromLast(keyVecs, start)
			if common < logicalCount {
				s.startClose(common)
				if err := group.drainSortRollupClose(proc); err != nil {
					return err
				}
				if s.outputRows >= aggBatchSize {
					return nil
				}
			}
			s.prepareWithinBatch(keyVecs)
		}

		// Find one complete-key run. BatchFill processes this run in chunks,
		// preserving vectorized aggregate execution instead of calling Fill for
		// every row and every live prefix.
		end := start + 1
		for end < rows && s.commonPrefixWithinBatch(end, end-1) == logicalCount {
			end++
		}
		if err := group.fillSortRollupLevels(proc, start, end-start); err != nil {
			return err
		}
		if err := s.updateLastKeys(group, keyVecs, end-1); err != nil {
			return err
		}
		s.inputRow = end

		if end < rows {
			common := s.commonPrefixWithinBatch(end, end-1)
			if common < logicalCount {
				s.startClose(common)
				if err := group.drainSortRollupClose(proc); err != nil {
					return err
				}
				if s.outputRows >= aggBatchSize {
					return nil
				}
			}
		}
	}

	s.input = nil
	s.inputRow = 0
	s.inputStartBoundaryHandled = false
	group.OpAnalyzer.SetMemUsed(group.sortRollupMemoryUsed())
	return group.checkSortRollupCapacity(proc)
}

func (group *Group) sortRollupMemoryUsed() int64 {
	if group == nil || group.ctr.sortRollup == nil || group.ctr.mp == nil {
		return 0
	}
	used := group.ctr.mp.CurrNB()
	used += int64(len(group.ctr.sortRollup.groupIDs)) * sortRollupGroupIDBytes
	for _, level := range group.ctr.sortRollup.levelAggs {
		for _, agg := range level {
			used += agg.AdditionalMemorySize()
		}
	}
	return used
}

func (group *Group) checkSortRollupCapacity(proc *process.Process) error {
	if group == nil || group.ctr.sortRollup == nil || group.ctr.spillMem <= 0 {
		return nil
	}
	used := group.sortRollupMemoryUsed()
	if group.ctr.spillMem < 10000 {
		// The streaming path has no hash-group count to compare against. Treat a
		// small explicit value as a strict resident-state threshold so tiny test
		// limits still fail deterministically.
		if used >= group.ctr.spillMem {
			return moerr.NewOOM(proc.Ctx)
		}
		return nil
	}
	if used > group.ctr.spillMem {
		return moerr.NewOOM(proc.Ctx)
	}
	return nil
}

func (group *Group) prepareSortRollup(proc *process.Process) error {
	if group.ctr.sortRollup != nil {
		group.ctr.sortRollup.free(group.ctr.mp)
	}
	state := &sortRollupState{}
	group.ctr.sortRollup = state
	if err := state.init(group); err != nil {
		return err
	}
	// Capacity is checked after the first input/output work unit. This keeps
	// Prepare reusable and preserves the operator's historical error timing.
	return nil
}

func (group *Group) takeSortRollupOutput() vm.CallResult {
	res := vm.NewCallResult()
	res.Batch = group.ctr.sortRollup.output
	group.ctr.sortRollup.lastOutput = res.Batch
	group.ctr.sortRollup.output = nil
	group.ctr.sortRollup.outputRows = 0
	return res
}

func (group *Group) releaseSortRollupOutput() {
	if group == nil || group.ctr.sortRollup == nil ||
		group.ctr.sortRollup.lastOutput == nil {
		return
	}
	group.ctr.sortRollup.lastOutput.Clean(group.ctr.mp)
	group.ctr.sortRollup.lastOutput = nil
}

func (group *Group) callSortRollup(proc *process.Process) (vm.CallResult, error) {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return vm.CancelResult, err
	}
	s := group.ctr.sortRollup
	if s == nil {
		return vm.CancelResult, moerr.NewInternalErrorNoCtx("sort rollup state is not initialized")
	}
	// The previous result has been synchronously consumed by the caller before
	// it asks this operator for another batch. Reclaim it before allocating or
	// admitting the next output batch. The terminal call also releases the last
	// partial result; Free remains a safety net for abandoned pipelines.
	group.releaseSortRollupOutput()

	if group.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	for {
		if s.outputRows >= aggBatchSize {
			// A child may cancel after vm.Exec's entry check. Do not publish
			// buffered output without observing that cancellation first.
			if err, canceled := vm.CancelCheck(proc); canceled {
				return vm.CancelResult, err
			}
			if err := group.checkSortRollupCapacity(proc); err != nil {
				return vm.CancelResult, err
			}
			return group.takeSortRollupOutput(), nil
		}

		if s.pendingClose {
			if err := group.drainSortRollupClose(proc); err != nil {
				return vm.CancelResult, err
			}
			if s.outputRows >= aggBatchSize {
				continue
			}
		}

		if s.input != nil {
			if err := group.processSortRollupInput(proc); err != nil {
				return vm.CancelResult, err
			}
			if s.outputRows >= aggBatchSize {
				continue
			}
			if s.input != nil {
				continue
			}
			continue
		}

		if group.ctr.inputDone {
			if !s.finalizing {
				s.finalizing = true
				if s.haveLast {
					// At end of input the grand total also closes. A regular
					// key boundary keeps prefix zero open for the following rows,
					// while EOF must drain it as well.
					s.pendingClose = true
					s.pendingCloseNext = len(s.levelAggs) - 1
					s.pendingCloseMin = 0
				} else {
					// ROLLUP always includes the empty grouping set. The pre-created
					// prefix-zero aggregate emits COUNT(*) = 0 and NULL for SUM.
					s.pendingClose = true
					s.pendingCloseNext = 0
					s.pendingCloseMin = 0
				}
			}
			if s.pendingClose {
				if err := group.drainSortRollupClose(proc); err != nil {
					return vm.CancelResult, err
				}
				if s.outputRows >= aggBatchSize {
					continue
				}
			}
			if err, canceled := vm.CancelCheck(proc); canceled {
				return vm.CancelResult, err
			}
			if !s.finalized {
				s.finalized = true
				group.ctr.state = vm.End
			}
			if s.outputRows > 0 {
				// Final partial output is still a publication boundary. A
				// cancellation observed after EOF must win over that output.
				if err, canceled := vm.CancelCheck(proc); canceled {
					return vm.CancelResult, err
				}
				if err := group.checkSortRollupCapacity(proc); err != nil {
					return vm.CancelResult, err
				}
				return group.takeSortRollupOutput(), nil
			}
			return vm.CancelResult, nil
		}

		result, err := vm.ChildrenCall(group.GetChildren(0), proc, group.OpAnalyzer)
		if err != nil {
			return vm.CancelResult, err
		}
		if result.Batch == nil {
			group.ctr.inputDone = true
			// Children can cancel the process while returning EOF, after this
			// operator has passed vm.Exec's entry cancellation check.
			if err, canceled := vm.CancelCheck(proc); canceled {
				return vm.CancelResult, err
			}
			continue
		}
		if result.Batch.IsEmpty() {
			continue
		}
		if err := group.evaluateBuildInput(proc, result.Batch); err != nil {
			return vm.CancelResult, err
		}
		s.input = result.Batch
		s.inputRow = 0
		s.inputStartBoundaryHandled = false
	}
}
