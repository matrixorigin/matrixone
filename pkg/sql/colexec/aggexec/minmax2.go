// Copyright 2021 - 2022 Matrix Origin
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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type minMaxExecFixed[T types.FixedSizeT] struct {
	aggExec
	comp     func(T, T) int
	hasExtra bool
	extra    T
}

type minMaxExecBytes struct {
	aggExec
	comp     func([]byte, []byte) int
	hasExtra bool
	extra    []byte
	// JSON extras are validated once at the SetExtraInformation boundary and
	// retained independently of the caller's buffer. When an allocation
	// account is installed after that boundary, the owned copy is moved into
	// the account's argument arena before any state is published.
	isJSONExtra    bool
	extraAccounted bool
}

// Merge copies or compares the source state without transferring ownership or
// mutating it. Window execution can therefore snapshot one running MIN/MAX
// state into successive cumulative result groups.
func (*minMaxExecFixed[T]) sourcePreservingMerge() {}
func (*minMaxExecBytes) sourcePreservingMerge()    {}

func (exec *minMaxExecFixed[T]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if groupIndex == -1 {
		return nil
	}
	vec := vectors[0]
	if vec.IsNull(uint64(row)) {
		return nil
	}
	x, y := exec.getXY(uint64(groupIndex))
	aggVec := exec.state[x].vecs[0]

	// A scalar fill needs no per-batch hash table or local winner arrays:
	// fixed-width min/max carries no row metadata.
	if vec.IsConst() {
		row = 0
	}
	value := vector.GetFixedAtNoTypeCheck[T](vec, row)
	aggs := chunkArr[T](aggVec)
	if aggVec.IsNull(uint64(y)) {
		aggVec.UnsetNull(uint64(y))
		aggs[y] = value
	} else {
		switch cmp := exec.comp(value, aggs[y]); {
		case cmp < 0:
			aggs[y] = value
		case cmp > 0:
			return nil
		}
	}
	return nil
}

func (exec *minMaxExecFixed[T]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *minMaxExecFixed[T]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	n := len(groups)
	if n == 0 {
		return nil
	}
	vals, selection, isConst := fixedAggregateInput[T](vec)

	const slotEmpty = 0xFF
	const maxSlots = 255
	var slotOf [256]uint8
	var localVals [maxSlots]T
	var localGrps [maxSlots]uint64
	nSlots := 0

	for i := range slotOf {
		slotOf[i] = slotEmpty
	}

	hasNull := vec.HasNull()
	for i := 0; i < n; i++ {
		grp := groups[i]
		if grp == GroupNotMatched {
			continue
		}
		if hasNull && vec.IsNull(uint64(i)+uint64(offset)) {
			continue
		}

		g := grp - 1
		var value T
		inputRow := int(uint64(i) + uint64(offset))
		if isConst {
			index := 0
			if selection != nil {
				index = int(selection[inputRow])
			}
			value = vals[index]
		} else {
			value = vals[i+offset]
		}

		h := uint8(g) ^ uint8(g>>8)
		for {
			s := slotOf[h]
			if s == slotEmpty {
				if nSlots >= maxSlots {
					x := int(g >> aggBatchSizeShift)
					y := g & aggBatchSizeMask
					aggs := chunkArr[T](exec.state[x].vecs[0])
					aggVec := exec.state[x].vecs[0]
					if aggVec.IsNull(y) {
						aggVec.UnsetNull(y)
						aggs[y] = value
					} else if exec.comp(value, aggs[y]) < 0 {
						aggs[y] = value
					}
					break
				}
				s = uint8(nSlots)
				slotOf[h] = s
				localGrps[nSlots] = g
				localVals[nSlots] = value
				nSlots++
				break
			}
			if localGrps[s] == g {
				if exec.comp(value, localVals[s]) < 0 {
					localVals[s] = value
				}
				break
			}
			h++
		}
	}

	lastX := -1
	var aggs []T
	var aggVec *vector.Vector
	for s := 0; s < nSlots; s++ {
		g := localGrps[s]
		x := int(g >> aggBatchSizeShift)
		if x != lastX {
			lastX = x
			aggs = chunkArr[T](exec.state[x].vecs[0])
			aggVec = exec.state[x].vecs[0]
		}
		y := g & aggBatchSizeMask
		if aggVec.IsNull(y) {
			aggVec.UnsetNull(y)
			aggs[y] = localVals[s]
		} else if exec.comp(localVals[s], aggs[y]) < 0 {
			aggs[y] = localVals[s]
		}
	}
	return nil
}

func (exec *minMaxExecFixed[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	if groupIdx1 == -1 {
		return exec.BatchMerge(next, groupIdx2, []uint64{GroupNotMatched})
	}
	other := next.(*minMaxExecFixed[T])
	x1, y1 := exec.getXY(uint64(groupIdx1))
	x2, y2 := other.getXY(uint64(groupIdx2))
	dst, src := exec.state[x1].vecs[0], other.state[x2].vecs[0]
	if src.IsNull(uint64(y2)) {
		return nil
	}
	// Scalar window snapshots need only the two touched states.
	// Spill-decoded sources may be compact; only the destination is chunk-sized.
	value := chunkRows[T](src)[y2]
	aggs := chunkArr[T](dst)
	if dst.IsNull(uint64(y1)) {
		dst.UnsetNull(uint64(y1))
		aggs[y1] = value
	} else {
		switch cmp := exec.comp(value, aggs[y1]); {
		case cmp < 0:
			aggs[y1] = value
		case cmp > 0:
			return nil
		}
	}
	return nil
}

func (exec *minMaxExecFixed[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*minMaxExecFixed[T])
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		g1 := grp - 1
		g2 := uint64(offset + i)
		x1 := int(g1 >> aggBatchSizeShift)
		y1 := g1 & aggBatchSizeMask
		x2 := int(g2 >> aggBatchSizeShift)
		y2 := g2 & aggBatchSizeMask

		aggs1 := chunkArr[T](exec.state[x1].vecs[0])
		aggs2 := chunkRows[T](other.state[x2].vecs[0])

		if other.state[x2].vecs[0].IsNull(y2) {
			continue
		}
		if exec.state[x1].vecs[0].IsNull(y1) {
			exec.state[x1].vecs[0].UnsetNull(y1)
			aggs1[y1] = aggs2[y2]
		} else if exec.comp(aggs2[y2], aggs1[y1]) < 0 {
			aggs1[y1] = aggs2[y2]
		}
	}
	return nil
}

func (exec *minMaxExecFixed[T]) SetExtraInformation(partialResult any, _ int) error {
	var ok bool
	exec.extra, ok = partialResult.(T)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("invalid extra information type %T", partialResult)
	}
	exec.hasExtra = true
	return nil
}

func (exec *minMaxExecFixed[T]) Flush() ([]*vector.Vector, error) {
	// transfer vector to result
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}
	freeResultsOnError := func(err error) ([]*vector.Vector, error) {
		for _, vec := range vecs {
			if vec != nil {
				vec.Free(exec.mp)
			}
		}
		return nil, err
	}

	if exec.hasExtra {
		for _, vec := range vecs {
			for i := range vec.Length() {
				if vec.IsNull(uint64(i)) {
					vec.UnsetNull(uint64(i))
					if err := vector.SetFixedAtNoTypeCheck(vec, int(i), exec.extra); err != nil {
						return freeResultsOnError(err)
					}
				} else {
					oldValue := vector.GetFixedAtNoTypeCheck[T](vec, int(i))
					if exec.comp(exec.extra, oldValue) < 0 {
						if err := vector.SetFixedAtNoTypeCheck(vec, int(i), exec.extra); err != nil {
							return freeResultsOnError(err)
						}
					}
				}
			}
		}
	}
	return vecs, nil
}

func (exec *minMaxExecBytes) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *minMaxExecBytes) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *minMaxExecBytes) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := exec.getXY(grp - 1)
			value := vectors[0].GetBytesAt(int(idx))
			if exec.state[x].vecs[0].IsNull(uint64(y)) {
				if err := exec.state[x].vecs[0].SetRawBytesAtFromAndUnsetNull(
					int(y), vectors[0], int(idx), exec.mp); err != nil {
					return err
				}
			} else {
				oldValue := exec.state[x].vecs[0].GetBytesAt(int(y))
				switch cmp := exec.comp(value, oldValue); {
				case cmp < 0:
					if err := vector.SetBytesAtFrom(exec.state[x].vecs[0], int(y), vectors[0], int(idx), exec.mp); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (exec *minMaxExecBytes) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *minMaxExecBytes) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*minMaxExecBytes)
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))

		if other.state[x2].vecs[0].IsNull(uint64(y2)) {
			continue
		}
		if exec.state[x1].vecs[0].IsNull(uint64(y1)) {
			if err := exec.state[x1].vecs[0].SetRawBytesAtFromAndUnsetNull(
				int(y1), other.state[x2].vecs[0], int(y2), exec.mp); err != nil {
				return err
			}
		} else {
			value := other.state[x2].vecs[0].GetBytesAt(int(y2))
			oldValue := exec.state[x1].vecs[0].GetBytesAt(int(y1))
			switch cmp := exec.comp(value, oldValue); {
			case cmp < 0:
				if err := vector.SetBytesAtFrom(
					exec.state[x1].vecs[0], int(y1), other.state[x2].vecs[0], int(y2), exec.mp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (exec *minMaxExecBytes) SetExtraInformation(partialResult any, _ int) error {
	if exec.isJSONExtra {
		raw, ok := partialResult.([]byte)
		if !ok {
			return moerr.NewInternalErrorNoCtxf("invalid extra information type %T", partialResult)
		}
		if len(raw) == 0 {
			return moerr.NewInvalidInputNoCtx("invalid JSON extra information")
		}
		value := bytejson.ByteJson{Type: bytejson.TpCode(raw[0]), Data: raw[1:]}
		if !bytejson.IsValidByteJson(value) {
			return moerr.NewInvalidInputNoCtx("invalid JSON extra information")
		}

		// Allocate before releasing a previously retained extra so a capacity
		// failure leaves the old optimizer state unchanged.
		var retained []byte
		accounted := false
		var err error
		if exec.allocation != nil {
			retained, err = exec.allocation.allocArgumentArena(exec.mp, len(raw))
			accounted = err == nil
		} else {
			retained = slices.Clone(raw)
		}
		if err != nil {
			return err
		}
		if accounted {
			copy(retained, raw)
		}
		exec.releaseJSONExtra()
		exec.extra = retained
		exec.extraAccounted = accounted
		exec.hasExtra = true
		return nil
	}

	var ok bool
	exec.extra, ok = partialResult.([]byte)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("invalid extra information type %T", partialResult)
	}
	exec.hasExtra = true
	return nil
}

func (exec *minMaxExecBytes) SetAllocationAccount(allocation *AllocationAccount) error {
	if err := exec.aggExec.SetAllocationAccount(allocation); err != nil {
		return err
	}
	if !exec.isJSONExtra || !exec.hasExtra || exec.extraAccounted {
		return nil
	}
	retained, err := allocation.allocArgumentArena(exec.mp, len(exec.extra))
	if err != nil {
		// SetAllocationAccount has no externally visible state yet; roll the
		// base pointer back so callers can safely retry with another account.
		_ = exec.aggExec.ClearAllocationAccount(allocation)
		return err
	}
	copy(retained, exec.extra)
	exec.extra = retained
	exec.extraAccounted = true
	return nil
}

func (exec *minMaxExecBytes) ClearAllocationAccount(allocation *AllocationAccount) error {
	if err := exec.aggExec.ClearAllocationAccount(allocation); err != nil {
		return err
	}
	if exec.isJSONExtra && exec.extraAccounted {
		retained := slices.Clone(exec.extra)
		exec.mp.Free(exec.extra)
		exec.extra = retained
		exec.extraAccounted = false
	}
	return nil
}

func (exec *minMaxExecBytes) releaseJSONExtra() {
	if !exec.isJSONExtra {
		return
	}
	if exec.extraAccounted && exec.extra != nil {
		exec.mp.Free(exec.extra)
	}
	exec.extra = nil
	exec.extraAccounted = false
}

func (exec *minMaxExecBytes) Free() {
	exec.releaseJSONExtra()
	exec.aggExec.Free()
}

func (exec *minMaxExecBytes) Flush() ([]*vector.Vector, error) {
	// transfer vector to result
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}
	freeResultsOnError := func(err error) ([]*vector.Vector, error) {
		for _, vec := range vecs {
			if vec != nil {
				vec.Free(exec.mp)
			}
		}
		return nil, err
	}

	if exec.hasExtra {
		for _, vec := range vecs {
			for i := range vec.Length() {
				if vec.IsNull(uint64(i)) {
					vec.UnsetNull(uint64(i))
					if err := vector.SetBytesAt(vec, int(i), exec.extra, exec.mp); err != nil {
						return freeResultsOnError(err)
					}
				} else {
					oldValue := vec.GetBytesAt(int(i))
					if exec.comp(exec.extra, oldValue) < 0 {
						if err := vector.SetBytesAt(vec, int(i), exec.extra, exec.mp); err != nil {
							return freeResultsOnError(err)
						}
					}
				}
			}
		}
	}
	return vecs, nil
}

func makeMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	return makeMinMaxExecWithLegacyText(mp, aggID, isMin, param, false)
}

func makeMinMaxExecWithLegacyText(
	mp *mpool.MPool,
	aggID int64,
	isMin bool,
	param types.Type,
	legacyTextComparator bool,
) AggFuncExec {
	switch param.Oid {
	case types.T_bool:
		return newBoolMinMaxExec(mp, aggID, isMin, param)
	case types.T_bit:
		return newGenericMinMaxExec[uint64](mp, aggID, isMin, param)
	case types.T_int8:
		return newGenericMinMaxExec[int8](mp, aggID, isMin, param)
	case types.T_int16:
		return newGenericMinMaxExec[int16](mp, aggID, isMin, param)
	case types.T_int32:
		return newGenericMinMaxExec[int32](mp, aggID, isMin, param)
	case types.T_int64:
		return newGenericMinMaxExec[int64](mp, aggID, isMin, param)
	case types.T_uint8:
		return newGenericMinMaxExec[uint8](mp, aggID, isMin, param)
	case types.T_uint16:
		return newGenericMinMaxExec[uint16](mp, aggID, isMin, param)
	case types.T_uint32:
		return newGenericMinMaxExec[uint32](mp, aggID, isMin, param)
	case types.T_uint64:
		return newGenericMinMaxExec[uint64](mp, aggID, isMin, param)
	case types.T_float32:
		return newGenericMinMaxExec[float32](mp, aggID, isMin, param)
	case types.T_float64:
		return newGenericMinMaxExec[float64](mp, aggID, isMin, param)
	case types.T_date:
		return newGenericMinMaxExec[types.Date](mp, aggID, isMin, param)
	case types.T_datetime:
		return newGenericMinMaxExec[types.Datetime](mp, aggID, isMin, param)
	case types.T_time:
		return newGenericMinMaxExec[types.Time](mp, aggID, isMin, param)
	case types.T_timestamp:
		return newGenericMinMaxExec[types.Timestamp](mp, aggID, isMin, param)
	case types.T_year:
		return newGenericMinMaxExec[types.MoYear](mp, aggID, isMin, param)
	case types.T_decimal64:
		return newDecimal64MinMaxExec(mp, aggID, isMin, param)
	case types.T_decimal128:
		return newDecimal128MinMaxExec(mp, aggID, isMin, param)
	case types.T_decimal256:
		return newDecimal256MinMaxExec(mp, aggID, isMin, param)
	case types.T_uuid:
		return newUuidMinMaxExec(mp, aggID, isMin, param)
	case types.T_enum:
		return newGenericMinMaxExec[types.Enum](mp, aggID, isMin, param)
	case types.T_char, types.T_varchar, types.T_text:
		if legacyTextComparator || param.Charset == types.CharsetBinary ||
			param.Charset == types.CharsetLegacy {
			return newStrMinMaxExec(mp, aggID, isMin, param)
		}
		if types.IsUnicodeCollation(param.Charset) {
			return newUnicodeMinMaxExec(mp, aggID, isMin, param)
		}
		if param.Charset == types.CharsetUTF8MB4Bin {
			return newUTF8mb4BinMinMaxExec(mp, aggID, isMin, param)
		}
		return newTextMinMaxExec(mp, aggID, isMin, param)
	case types.T_blob, types.T_binary, types.T_varbinary, types.T_datalink:
		return newStrMinMaxExec(mp, aggID, isMin, param)
	case types.T_json:
		return newJSONMinMaxExec(mp, aggID, isMin, param)
	case types.T_array_float32, types.T_array_float64:
		return newArrayMinMaxExec(mp, aggID, isMin, param)
	}
	panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for min/max", param.Oid))
}

func setupAggInfo(a *aggInfo, aggId int64, param types.Type) {
	*a = aggInfo{
		aggId:      aggId,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    param,
		stateTypes: []types.Type{param},
		emptyNull:  true,
		saveArg:    false,
	}
}

func newBoolMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[bool]
	exec.mp = mp
	if isMin {
		exec.comp = types.BoolAscCompare
	} else {
		exec.comp = types.BoolDescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newGenericMinMaxExec[T types.OrderedT](mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[T]
	exec.mp = mp
	if isMin {
		exec.comp = types.GenericAscCompare[T]
	} else {
		exec.comp = types.GenericDescCompare[T]
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newDecimal64MinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[types.Decimal64]
	exec.mp = mp
	if isMin {
		exec.comp = types.Decimal64AscCompare
	} else {
		exec.comp = types.Decimal64DescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newDecimal128MinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[types.Decimal128]
	exec.mp = mp
	if isMin {
		exec.comp = types.Decimal128AscCompare
	} else {
		exec.comp = types.Decimal128DescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newDecimal256MinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[types.Decimal256]
	exec.mp = mp
	if isMin {
		exec.comp = types.Decimal256AscCompare
	} else {
		exec.comp = types.Decimal256DescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newUuidMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[types.Uuid]
	exec.mp = mp
	if isMin {
		exec.comp = types.UuidAscCompare
	} else {
		exec.comp = types.UuidDescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newStrMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		exec.comp = bytes.Compare
	} else {
		exec.comp = func(x, y []byte) int { return -bytes.Compare(x, y) }
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newJSONMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	exec.isJSONExtra = true
	if isMin {
		exec.comp = compareJSONBytes
	} else {
		exec.comp = func(x, y []byte) int { return -compareJSONBytes(x, y) }
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

// compareJSONBytes is used only after vector/extra admission has established
// complete ByteJSON values. Keeping the hot comparison on the trusted path
// avoids recursively validating every candidate in every Fill call.
func compareJSONBytes(x, y []byte) int {
	left := bytejson.ByteJson{Type: bytejson.TpCode(x[0]), Data: x[1:]}
	right := bytejson.ByteJson{Type: bytejson.TpCode(y[0]), Data: y[1:]}
	return bytejson.CompareByteJsonTrusted(left, right)
}

func newTextMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		exec.comp = collation.CompareGeneralCI
	} else {
		exec.comp = func(x, y []byte) int { return -collation.CompareGeneralCI(x, y) }
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newUnicodeMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		exec.comp = func(x, y []byte) int { return types.CompareStringValues(param, x, y) }
	} else {
		exec.comp = func(x, y []byte) int { return -types.CompareStringValues(param, x, y) }
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newUTF8mb4BinMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		exec.comp = compareUTF8mb4Bin
	} else {
		exec.comp = func(x, y []byte) int { return -compareUTF8mb4Bin(x, y) }
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newArrayMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		switch param.Oid {
		case types.T_array_float32:
			exec.comp = func(x, y []byte) int { return types.CompareArrayFromBytes[float32](x, y, false) }
		case types.T_array_float64:
			exec.comp = func(x, y []byte) int { return types.CompareArrayFromBytes[float64](x, y, false) }
		default:
			panic("Unsupported array type")
		}
	} else {
		switch param.Oid {
		case types.T_array_float32:
			exec.comp = func(x, y []byte) int { return types.CompareArrayFromBytes[float32](x, y, true) }
		case types.T_array_float64:
			exec.comp = func(x, y []byte) int { return types.CompareArrayFromBytes[float64](x, y, true) }
		default:
			panic("Unsupported array type")
		}
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}
