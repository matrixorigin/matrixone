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
	"cmp"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var MedianSupportedType = []types.T{
	types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128,
}

func MedianReturnType(args []types.Type) types.Type {
	if args[0].IsDecimal() {
		return types.New(types.T_decimal128, 38, args[0].Scale+1)
	}
	return types.T_float64.ToType()
}

type medianColumnExecSelf[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	distinctHash distinctHash
	ret          aggResultWithFixedType[R]
	groups       []*Vectors[T]
	argType      types.Type
	mp           *mpool.MPool
}

func newMedianColumnExecSelf[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType](mp *mpool.MPool, info singleAggInfo, initial R) medianColumnExecSelf[T, R] {
	self := medianColumnExecSelf[T, R]{
		singleAggInfo: info,
		distinctHash:  newDistinctHash(mp),
		ret:           initAggResultWithFixedTypeResult[R](mp, info.retType, info.emptyNull, initial, false),
		argType:       info.argType,
		mp:            mp,
	}
	return self
}

func (exec *medianColumnExecSelf[T, R]) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *medianColumnExecSelf[T, R]) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}

	oldLength := len(exec.groups)
	if cap(exec.groups) >= oldLength+more {
		exec.groups = exec.groups[:oldLength+more]
	} else {
		exec.groups = append(exec.groups, make([]*Vectors[T], more)...)
	}
	for i := oldLength; i < len(exec.groups); i++ {
		exec.groups[i] = NewVectors[T](exec.argType)
	}
	return exec.ret.grows(more)
}

func (exec *medianColumnExecSelf[T, R]) PreAllocateGroups(more int) error {
	if len(exec.groups) == 0 {
		exec.groups = make([]*Vectors[T], 0, more)
	} else {
		oldLength := len(exec.groups)
		exec.groups = append(exec.groups, make([]*Vectors[T], more)...)
		exec.groups = exec.groups[:oldLength]
	}
	return exec.ret.preExtend(more)
}

func (exec *medianColumnExecSelf[T, R]) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	return marshalRetAndGroupsToBuffer(cnt, flags, buf, &exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *medianColumnExecSelf[T, R]) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	return marshalChunkToBuffer(chunk, buf, &exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *medianColumnExecSelf[T, R]) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	if err := unmarshalFromReaderNoGroup(reader, &exec.ret.optSplitResult); err != nil {
		return err
	}
	exec.ret.setupT()
	expectedGroups := 0
	for _, vec := range exec.ret.resultList {
		expectedGroups += vec.Length()
	}

	ngrp, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	if ngrp < 0 || ngrp != int64(expectedGroups) {
		return moerr.NewInternalErrorNoCtxf("median unmarshal: invalid group count %d, expected %d", ngrp, expectedGroups)
	}
	if ngrp != 0 {
		exec.groups = make([]*Vectors[T], ngrp)
		for i := range exec.groups {
			_, bs, err := types.ReadSizeBytes(reader)
			if err != nil {
				return err
			}
			grp := NewVectors[T](exec.argType)
			for _, vec := range grp.vecs {
				vec.Free(mp)
			}
			grp.vecs = nil
			if err = grp.Unmarshal(bs, exec.argType, mp); err != nil {
				return err
			}
			exec.groups[i] = grp
		}
	}
	if exec.IsDistinct() {
		if err = exec.rebuildDistinctHash(); err != nil {
			return err
		}
	}

	extraCnt, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	for i := int64(0); i < extraCnt; i++ {
		if _, _, err = types.ReadSizeBytes(reader); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) rebuildDistinctHash() error {
	if len(exec.groups) == 0 {
		return nil
	}
	if err := exec.distinctHash.grows(len(exec.groups)); err != nil {
		return err
	}
	for groupIdx, group := range exec.groups {
		for _, vec := range group.vecs {
			vals := vector.MustFixedColWithTypeCheck[T](vec)
			for row := range vals {
				if _, err := exec.distinctHash.fill(groupIdx, []*vector.Vector{vec}, row); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}
	if vectors[0].IsConst() {
		row = 0
	}
	if exec.IsDistinct() {
		need, err := exec.distinctHash.fill(groupIndex, vectors, row)
		if err != nil || !need {
			return err
		}
	}
	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	markMedianGroupNotEmpty(&exec.ret, x, y)
	value := vector.MustFixedColWithTypeCheck[T](vectors[0])[row]
	return appendMedianValue(exec.groups[groupIndex], value, exec.mp)
}

func (exec *medianColumnExecSelf[T, R]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	if exec.IsDistinct() {
		return exec.distinctBulkFill(groupIndex, vectors)
	}

	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	if vectors[0].IsConst() {
		markMedianGroupNotEmpty(&exec.ret, x, y)
		value := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		return AppendMultiFixed(exec.groups[groupIndex], value, false, vectors[0].Length(), exec.mp)
	}

	vals := vector.MustFixedColWithTypeCheck[T](vectors[0])
	mustNotEmpty := false
	for i := range vals {
		if vectors[0].IsNull(uint64(i)) {
			continue
		}
		mustNotEmpty = true
		if err := appendMedianValue(exec.groups[groupIndex], vals[i], exec.mp); err != nil {
			return err
		}
	}
	if mustNotEmpty {
		markMedianGroupNotEmpty(&exec.ret, x, y)
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) distinctBulkFill(groupIndex int, vectors []*vector.Vector) error {
	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	if vectors[0].IsConst() {
		need, err := exec.distinctHash.fill(groupIndex, vectors, 0)
		if err != nil || !need {
			return err
		}
		markMedianGroupNotEmpty(&exec.ret, x, y)
		value := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		return appendMedianValue(exec.groups[groupIndex], value, exec.mp)
	}

	vals := vector.MustFixedColWithTypeCheck[T](vectors[0])
	mustNotEmpty := false
	for i := range vals {
		if vectors[0].IsNull(uint64(i)) {
			continue
		}
		need, err := exec.distinctHash.fill(groupIndex, vectors, i)
		if err != nil {
			return err
		}
		if !need {
			continue
		}
		mustNotEmpty = true
		if err = appendMedianValue(exec.groups[groupIndex], vals[i], exec.mp); err != nil {
			return err
		}
	}
	if mustNotEmpty {
		markMedianGroupNotEmpty(&exec.ret, x, y)
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	if exec.IsDistinct() {
		return exec.distinctBatchFill(offset, groups, vectors)
	}

	if vectors[0].IsConst() {
		value := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		for _, group := range groups {
			if group == GroupNotMatched {
				continue
			}
			groupIndex := int(group - 1)
			x, y := exec.ret.updateNextAccessIdx(groupIndex)
			markMedianGroupNotEmpty(&exec.ret, x, y)
			if err := appendMedianValue(exec.groups[groupIndex], value, exec.mp); err != nil {
				return err
			}
		}
		return nil
	}

	vals := vector.MustFixedColWithTypeCheck[T](vectors[0])
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		groupIndex := int(group - 1)
		x, y := exec.ret.updateNextAccessIdx(groupIndex)
		markMedianGroupNotEmpty(&exec.ret, x, y)
		if err := appendMedianValue(exec.groups[groupIndex], vals[row], exec.mp); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) distinctBatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConst() {
		value := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		for _, group := range groups {
			if group == GroupNotMatched {
				continue
			}
			need, err := exec.distinctHash.fill(int(group-1), vectors, 0)
			if err != nil {
				return err
			}
			if !need {
				continue
			}
			groupIndex := int(group - 1)
			x, y := exec.ret.updateNextAccessIdx(groupIndex)
			markMedianGroupNotEmpty(&exec.ret, x, y)
			if err = appendMedianValue(exec.groups[groupIndex], value, exec.mp); err != nil {
				return err
			}
		}
		return nil
	}

	vals := vector.MustFixedColWithTypeCheck[T](vectors[0])
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsNull(uint64(row)) {
			continue
		}
		need, err := exec.distinctHash.fill(int(group-1), vectors, row)
		if err != nil {
			return err
		}
		if !need {
			continue
		}
		groupIndex := int(group - 1)
		x, y := exec.ret.updateNextAccessIdx(groupIndex)
		markMedianGroupNotEmpty(&exec.ret, x, y)
		if err = appendMedianValue(exec.groups[groupIndex], vals[row], exec.mp); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) mergeDistinctGroup(other *medianColumnExecSelf[T, R], groupIdx1, groupIdx2 int) error {
	for _, vec := range other.groups[groupIdx2].vecs {
		vals := vector.MustFixedColWithTypeCheck[T](vec)
		for row := range vals {
			need, err := exec.distinctHash.fill(groupIdx1, []*vector.Vector{vec}, row)
			if err != nil {
				return err
			}
			if !need {
				continue
			}
			x, y := exec.ret.updateNextAccessIdx(groupIdx1)
			markMedianGroupNotEmpty(&exec.ret, x, y)
			if err = appendMedianValue(exec.groups[groupIdx1], vals[row], exec.mp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) Merge(other *medianColumnExecSelf[T, R], groupIdx1, groupIdx2 int) error {
	if exec.IsDistinct() {
		return exec.mergeDistinctGroup(other, groupIdx1, groupIdx2)
	}
	if other.groups[groupIdx2].Length() == 0 {
		return nil
	}
	x, y := exec.ret.updateNextAccessIdx(groupIdx1)
	markMedianGroupNotEmpty(&exec.ret, x, y)
	return exec.groups[groupIdx1].Union(other.groups[groupIdx2], exec.mp)
}

func (exec *medianColumnExecSelf[T, R]) BatchMerge(next *medianColumnExecSelf[T, R], offset int, groups []uint64) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if err := exec.Merge(next, int(group)-1, i+offset); err != nil {
			return err
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *medianColumnExecSelf[T, R]) Free() {
	for _, group := range exec.groups {
		if group != nil {
			group.Free(exec.mp)
		}
	}
	exec.groups = nil
	exec.ret.free()
	exec.distinctHash.free()
}

func (exec *medianColumnExecSelf[T, R]) Size() int64 {
	var size int64
	for _, group := range exec.groups {
		if group != nil {
			size += group.Size()
		}
	}
	size += int64(cap(exec.groups)) * 8
	return exec.ret.Size() + exec.distinctHash.Size() + size
}

type medianColumnNumericExec[T numeric] struct {
	medianColumnExecSelf[T, float64]
}

func newMedianColumnNumericExec[T numeric](mp *mpool.MPool, info singleAggInfo) AggFuncExec {
	return &medianColumnNumericExec[T]{
		medianColumnExecSelf: newMedianColumnExecSelf[T, float64](mp, info, 0),
	}
}

type medianColumnDecimalExec[T types.Decimal64 | types.Decimal128] struct {
	medianColumnExecSelf[T, types.Decimal128]
}

func newMedianColumnDecimalExec[T types.Decimal64 | types.Decimal128](mp *mpool.MPool, info singleAggInfo) AggFuncExec {
	return &medianColumnDecimalExec[T]{
		medianColumnExecSelf: newMedianColumnExecSelf[T, types.Decimal128](mp, info, types.Decimal128{}),
	}
}

func newMedianExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) (AggFuncExec, error) {
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   MedianReturnType([]types.Type{param}),
		emptyNull: true,
	}

	switch param.Oid {
	case types.T_bit:
		return newMedianColumnNumericExec[uint64](mp, info), nil
	case types.T_int8:
		return newMedianColumnNumericExec[int8](mp, info), nil
	case types.T_int16:
		return newMedianColumnNumericExec[int16](mp, info), nil
	case types.T_int32:
		return newMedianColumnNumericExec[int32](mp, info), nil
	case types.T_int64:
		return newMedianColumnNumericExec[int64](mp, info), nil
	case types.T_uint8:
		return newMedianColumnNumericExec[uint8](mp, info), nil
	case types.T_uint16:
		return newMedianColumnNumericExec[uint16](mp, info), nil
	case types.T_uint32:
		return newMedianColumnNumericExec[uint32](mp, info), nil
	case types.T_uint64:
		return newMedianColumnNumericExec[uint64](mp, info), nil
	case types.T_float32:
		return newMedianColumnNumericExec[float32](mp, info), nil
	case types.T_float64:
		return newMedianColumnNumericExec[float64](mp, info), nil
	case types.T_decimal64:
		return newMedianColumnDecimalExec[types.Decimal64](mp, info), nil
	case types.T_decimal128:
		return newMedianColumnDecimalExec[types.Decimal128](mp, info), nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported type for median()")
	}
}

func (exec *medianColumnNumericExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*medianColumnNumericExec[T])
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

func (exec *medianColumnNumericExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*medianColumnNumericExec[T])
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

func (exec *medianColumnNumericExec[T]) Flush() ([]*vector.Vector, error) {
	vs := exec.ret.values
	groups := len(exec.groups)
	lim := exec.ret.getChunkSize()
	for i, x := 0, 0; i < groups; i += lim {
		n := groups - i
		if n > lim {
			n = lim
		}
		s := i
		for j := 0; j < n; j++ {
			rows := exec.groups[s].Length()
			if rows == 0 {
				s++
				continue
			}
			markMedianGroupNotEmpty(&exec.ret, x, j)
			v, err := MedianNumeric(exec.groups[s])
			if err != nil {
				return nil, err
			}
			vs[x][j] = v
			s++
		}
	}
	return exec.ret.flushAll(), nil
}

func (exec *medianColumnDecimalExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*medianColumnDecimalExec[T])
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

func (exec *medianColumnDecimalExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*medianColumnDecimalExec[T])
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

func (exec *medianColumnDecimalExec[T]) Flush() ([]*vector.Vector, error) {
	vs := exec.ret.values
	argIsDecimal128 := exec.singleAggInfo.argType.Oid == types.T_decimal128
	groups := len(exec.groups)
	lim := exec.ret.getChunkSize()
	for i, x := 0, 0; i < groups; i += lim {
		n := groups - i
		if n > lim {
			n = lim
		}
		s := i
		for j := 0; j < n; j++ {
			rows := exec.groups[s].Length()
			if rows == 0 {
				s++
				continue
			}
			markMedianGroupNotEmpty(&exec.ret, x, j)
			var (
				v   types.Decimal128
				err error
			)
			if argIsDecimal128 {
				v, err = MedianDecimal128(any(exec.groups[s]).(*Vectors[types.Decimal128]))
			} else {
				v, err = MedianDecimal64(any(exec.groups[s]).(*Vectors[types.Decimal64]))
			}
			if err != nil {
				return nil, err
			}
			vs[x][j] = v
			s++
		}
	}
	return exec.ret.flushAll(), nil
}

func appendMedianValue[T numeric | types.Decimal64 | types.Decimal128](vecs *Vectors[T], value T, mp *mpool.MPool) error {
	vec := vecs.getAppendableVector()
	return vector.AppendFixed(vec, value, false, mp)
}

func markMedianGroupNotEmpty[T types.FixedSizeTExceptStrType](ret *aggResultWithFixedType[T], x, y int) {
	if len(ret.bsFromEmptyList) > x && ret.bsFromEmptyList[x] != nil {
		ret.bsFromEmptyList[x][y] = false
	}
}

func medianDecimal64FromState(st aggState, idx uint16, info *aggInfo) (types.Decimal128, error) {
	vals := make([]types.Decimal64, 0, st.argCnt[idx])
	if err := st.iter(idx, func(k []byte) error {
		vals = append(vals, types.DecodeDecimal64(aggPayloadFromKey(info, k)))
		return nil
	}); err != nil {
		return types.Decimal128{}, err
	}
	return medianDecimal64Vals(vals)
}

func medianDecimal128FromState(st aggState, idx uint16, info *aggInfo) (types.Decimal128, error) {
	vals := make([]types.Decimal128, 0, st.argCnt[idx])
	if err := st.iter(idx, func(k []byte) error {
		vals = append(vals, types.DecodeDecimal128(aggPayloadFromKey(info, k)))
		return nil
	}); err != nil {
		return types.Decimal128{}, err
	}
	return medianDecimal128Vals(vals)
}

func MedianNumeric[T numeric](vs *Vectors[T]) (float64, error) {
	vals := collectMedianValues(vs)
	return medianNumericVals(vals), nil
}

func MedianDecimal64(vs *Vectors[types.Decimal64]) (types.Decimal128, error) {
	vals := collectMedianValues(vs)
	return medianDecimal64Vals(vals)
}

func MedianDecimal128(vs *Vectors[types.Decimal128]) (types.Decimal128, error) {
	vals := collectMedianValues(vs)
	return medianDecimal128Vals(vals)
}

func collectMedianValues[T numeric | types.Decimal64 | types.Decimal128](vs *Vectors[T]) []T {
	vals := make([]T, 0, vs.Length())
	for _, vec := range vs.vecs {
		vals = append(vals, vector.MustFixedColWithTypeCheck[T](vec)...)
	}
	return vals
}

func medianNumericVals[T numeric](vals []T) float64 {
	rows := len(vals)
	if rows&1 == 1 {
		return float64(selectKthNumeric(vals, rows>>1))
	}
	v1 := selectKthNumeric(vals, rows>>1-1)
	v2 := selectKthNumeric(vals, rows>>1)
	return (float64(v1) + float64(v2)) / 2
}

func medianDecimal64Vals(vals []types.Decimal64) (types.Decimal128, error) {
	rows := len(vals)
	if rows&1 == 1 {
		return FromD64ToD128(selectKthFunc(vals, rows>>1, func(a, b types.Decimal64) int {
			return a.Compare(b)
		})).Scale(1)
	}
	v1 := FromD64ToD128(selectKthFunc(vals, rows>>1-1, func(a, b types.Decimal64) int {
		return a.Compare(b)
	}))
	v2 := FromD64ToD128(selectKthFunc(vals, rows>>1, func(a, b types.Decimal64) int {
		return a.Compare(b)
	}))
	ret, err := v1.Add128(v2)
	if err != nil {
		return types.Decimal128{}, err
	}
	if ret.Sign() {
		if ret, err = ret.Minus().Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return ret.Right(1).Minus(), nil
	}
	if ret, err = ret.Scale(1); err != nil {
		return types.Decimal128{}, err
	}
	return ret.Right(1), nil
}

func medianDecimal128Vals(vals []types.Decimal128) (types.Decimal128, error) {
	rows := len(vals)
	if rows&1 == 1 {
		ret := selectKthFunc(vals, rows>>1, func(a, b types.Decimal128) int {
			return a.Compare(b)
		})
		return ret.Scale(1)
	}
	v1 := selectKthFunc(vals, rows>>1-1, func(a, b types.Decimal128) int {
		return a.Compare(b)
	})
	v2 := selectKthFunc(vals, rows>>1, func(a, b types.Decimal128) int {
		return a.Compare(b)
	})
	ret, err := v1.Add128(v2)
	if err != nil {
		return types.Decimal128{}, err
	}
	if ret.Sign() {
		if ret, err = ret.Minus().Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return ret.Right(1).Minus(), nil
	}
	if ret, err = ret.Scale(1); err != nil {
		return types.Decimal128{}, err
	}
	return ret.Right(1), nil
}

func selectKthNumeric[T cmp.Ordered](vals []T, k int) T {
	return selectKthFunc(vals, k, func(a, b T) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
}

func selectKthFunc[T any](vals []T, k int, compare func(a, b T) int) T {
	left, right := 0, len(vals)-1
	for {
		if left == right {
			return vals[left]
		}

		lt, gt := partitionAroundPivot(vals, left, right, (left+right)>>1, compare)
		switch {
		case k < lt:
			right = lt - 1
		case k > gt:
			left = gt + 1
		default:
			return vals[k]
		}
	}
}

func partitionAroundPivot[T any](vals []T, left, right, pivot int, compare func(a, b T) int) (int, int) {
	pivotValue := vals[pivot]
	vals[pivot], vals[right] = vals[right], vals[pivot]
	lt, i, gt := left, left, right
	for i <= gt {
		switch cmp := compare(vals[i], pivotValue); {
		case cmp < 0:
			vals[lt], vals[i] = vals[i], vals[lt]
			lt++
			i++
		case cmp > 0:
			vals[i], vals[gt] = vals[gt], vals[i]
			gt--
		default:
			i++
		}
	}
	return lt, gt
}
