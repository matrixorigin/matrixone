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
	"encoding/binary"
	"encoding/json"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type jsonArrayAggExec struct {
	aggExec
	distinct     bool
	distinctHash distinctHash
}

func (exec *jsonArrayAggExec) SetAllocationAccount(
	allocation *AllocationAccount,
) error {
	if err := exec.aggExec.SetAllocationAccount(allocation); err != nil {
		return err
	}
	exec.distinctHash.free()
	return nil
}

func newJsonArrayAggExec(mp *mpool.MPool, info multiAggInfo) *jsonArrayAggExec {
	exec := &jsonArrayAggExec{}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      info.aggID,
		isDistinct: false,
		argTypes:   info.argTypes,
		retType:    info.retType,
		emptyNull:  info.emptyNull,
		saveArg:    true,
		opaqueArg:  true,
	}
	exec.distinct = info.distinct
	exec.distinctHash = newDistinctHash(mp)
	return exec
}

func (exec *jsonArrayAggExec) IsDistinct() bool {
	return exec.distinct
}

func (exec *jsonArrayAggExec) GroupGrow(more int) error {
	if exec.allocation == nil && exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	return nil
}

func (exec *jsonArrayAggExec) PreAllocateGroups(more int) error {
	if exec.allocation == nil && exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.PreAllocateGroups(more); err != nil {
		return err
	}
	return nil
}

func (exec *jsonArrayAggExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *jsonArrayAggExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *jsonArrayAggExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.allocation != nil {
		return exec.batchFillAccounted(offset, groups, vectors)
	}
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		var (
			val bytejson.ByteJson
			err error
		)
		if vectors[0].IsNull(uint64(row)) {
			val = bytejson.Null
		} else {
			if exec.distinct {
				need, err := exec.distinctHash.fill(int(grp-1), vectors, row)
				if err != nil {
					return err
				}
				if !need {
					continue
				}
			}
			val, err = buildValueByteJson(vectors[0], uint64(row))
			if err != nil {
				return err
			}
		}
		bs, err := val.Marshal()
		if err != nil {
			return err
		}
		payload := appendPayloadField(nil, bs, false)
		x, y := exec.getXY(grp - 1)
		if err := exec.state[x].fillArg(exec.mp, y, payload, false); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonArrayAggExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *jsonArrayAggExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*jsonArrayAggExec)
	if exec.allocation == nil && exec.distinct {
		if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
			return err
		}
	}
	return exec.batchMergeArgs(&other.aggExec, offset, groups, false)
}

func (exec *jsonArrayAggExec) SetExtraInformation(_ any, _ int) error {
	return nil
}

func (exec *jsonArrayAggExec) Flush() (_ []*vector.Vector, retErr error) {
	if exec.allocation != nil {
		return exec.flushAccounted()
	}
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(exec.retType)
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}
		for j := 0; j < int(st.length); j++ {
			if st.argCnt[j] == 0 {
				vector.AppendNull(vecs[i], exec.mp)
				continue
			}
			arr := make([]any, 0, st.argCnt[j])
			if err := st.iter(uint16(j), func(k []byte) error {
				payload := aggPayloadFromKey(&exec.aggInfo, k)
				return payloadFieldIterator(payload, 1, func(_ int, isNull bool, data []byte) error {
					if isNull {
						arr = append(arr, bytejson.Null)
					} else {
						arr = append(arr, types.DecodeJson(data))
					}
					return nil
				})
			}); err != nil {
				return nil, err
			}
			bj, err := bytejson.CreateByteJSONWithCheck(arr)
			if err != nil {
				return nil, err
			}
			bs, err := bj.Marshal()
			if err != nil {
				return nil, err
			}
			if err := vector.AppendBytes(vecs[i], bs, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func (exec *jsonArrayAggExec) Size() int64 {
	var size int64
	for _, st := range exec.state {
		size += int64(len(st.argbuf))
		size += int64(cap(st.argCnt)) * 4
	}
	return size + exec.distinctHash.Size()
}

func (exec *jsonArrayAggExec) Free() {
	exec.distinctHash.free()
	exec.aggExec.Free()
}

type jsonObjectAggExec struct {
	aggExec
	distinct     bool
	distinctHash distinctHash
}

func (exec *jsonObjectAggExec) SetAllocationAccount(
	allocation *AllocationAccount,
) error {
	if err := exec.aggExec.SetAllocationAccount(allocation); err != nil {
		return err
	}
	exec.distinctHash.free()
	return nil
}

func newJsonObjectAggExec(mg *mpool.MPool, info multiAggInfo) *jsonObjectAggExec {
	exec := &jsonObjectAggExec{}
	exec.mp = mg
	exec.aggInfo = aggInfo{
		aggId:      info.aggID,
		isDistinct: false,
		argTypes:   info.argTypes,
		retType:    info.retType,
		emptyNull:  info.emptyNull,
		saveArg:    true,
		opaqueArg:  true,
	}
	exec.distinct = info.distinct
	exec.distinctHash = newDistinctHash(mg)
	return exec
}

func (exec *jsonObjectAggExec) IsDistinct() bool {
	return exec.distinct
}

func (exec *jsonObjectAggExec) GroupGrow(more int) error {
	if exec.allocation == nil && exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	return nil
}

func (exec *jsonObjectAggExec) PreAllocateGroups(more int) error {
	if exec.allocation == nil && exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.PreAllocateGroups(more); err != nil {
		return err
	}
	return nil
}

func (exec *jsonObjectAggExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *jsonObjectAggExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *jsonObjectAggExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.allocation != nil {
		return exec.batchFillAccounted(offset, groups, vectors)
	}
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		row := offset + i
		keyRow := row
		valRow := row
		if vectors[0].IsConst() {
			keyRow = 0
		}
		if vectors[1].IsConst() {
			valRow = 0
		}
		if vectors[0].IsNull(uint64(keyRow)) {
			return moerr.NewInvalidInputNoCtx("json_objectagg key cannot be NULL")
		}
		if exec.distinct {
			need, err := exec.distinctHash.fill(int(grp-1), vectors, row)
			if err != nil {
				return err
			}
			if !need {
				continue
			}
		}
		key, err := getStringKey(vectors[0], uint64(keyRow))
		if err != nil {
			return err
		}
		val := bytejson.Null
		if !vectors[1].IsNull(uint64(valRow)) {
			val, err = buildValueByteJson(vectors[1], uint64(valRow))
			if err != nil {
				return err
			}
		}
		valBytes, err := val.Marshal()
		if err != nil {
			return err
		}
		payload := appendPayloadField(nil, []byte(key), false)
		payload = appendPayloadField(payload, valBytes, false)
		x, y := exec.getXY(grp - 1)
		if err := exec.state[x].fillArg(exec.mp, y, payload, false); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonObjectAggExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *jsonObjectAggExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*jsonObjectAggExec)
	if exec.allocation == nil && exec.distinct {
		if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
			return err
		}
	}
	return exec.batchMergeArgs(&other.aggExec, offset, groups, false)
}

func (exec *jsonObjectAggExec) SetExtraInformation(_ any, _ int) error {
	return nil
}

func (exec *jsonObjectAggExec) Flush() (_ []*vector.Vector, retErr error) {
	if exec.allocation != nil {
		return exec.flushAccounted()
	}
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(exec.retType)
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}
		for j := 0; j < int(st.length); j++ {
			if st.argCnt[j] == 0 {
				vector.AppendNull(vecs[i], exec.mp)
				continue
			}
			obj := make(map[string]any, int(st.argCnt[j]))
			if err := st.iter(uint16(j), func(k []byte) error {
				var (
					key string
					val any = bytejson.Null
				)
				payload := aggPayloadFromKey(&exec.aggInfo, k)
				if err := payloadFieldIterator(payload, 2, func(field int, isNull bool, data []byte) error {
					if isNull {
						return nil
					}
					if field == 0 {
						key = string(data)
					} else {
						val = types.DecodeJson(data)
					}
					return nil
				}); err != nil {
					return err
				}
				obj[key] = val
				return nil
			}); err != nil {
				return nil, err
			}
			bj, err := bytejson.CreateByteJSONWithCheck(obj)
			if err != nil {
				return nil, err
			}
			bs, err := bj.Marshal()
			if err != nil {
				return nil, err
			}
			if err := vector.AppendBytes(vecs[i], bs, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func (exec *jsonObjectAggExec) Size() int64 {
	var size int64
	for _, st := range exec.state {
		size += int64(len(st.argbuf))
		size += int64(cap(st.argCnt)) * 4
	}
	return size + exec.distinctHash.Size()
}

func (exec *jsonObjectAggExec) Free() {
	exec.distinctHash.free()
	exec.aggExec.Free()
}

func appendJSONPayloadField(dst []byte, data []byte) []byte {
	dst = append(dst, 1)
	var size [4]byte
	binary.NativeEndian.PutUint32(size[:], uint32(len(data)))
	dst = append(dst, size[:]...)
	return append(dst, data...)
}

func jsonAggregateValueSize(vec *vector.Vector, row uint64) (int, error) {
	if vec.IsNull(row) || vec.GetType().Oid == types.T_any || vec.GetType().Oid == types.T_bool {
		return 2, nil
	}
	typ := vec.GetType()
	switch typ.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64:
		return 9, nil
	case types.T_decimal64:
		value := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)[row].Format(typ.Scale)
		return jsonAggregateNumberSize(value)
	case types.T_decimal128:
		value := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[row].Format(typ.Scale)
		return jsonAggregateNumberSize(value)
	case types.T_date:
		length := len(vector.MustFixedColNoTypeCheck[types.Date](vec)[row].String())
		return 1 + jsonUvarintSize(uint64(length)) + length, nil
	case types.T_time:
		length := len(vector.MustFixedColNoTypeCheck[types.Time](vec)[row].String())
		return 1 + jsonUvarintSize(uint64(length)) + length, nil
	case types.T_datetime:
		length := len(vector.MustFixedColNoTypeCheck[types.Datetime](vec)[row].String())
		return 1 + jsonUvarintSize(uint64(length)) + length, nil
	case types.T_timestamp:
		length := len(vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[row].String())
		return 1 + jsonUvarintSize(uint64(length)) + length, nil
	case types.T_char, types.T_varchar, types.T_text:
		length := len(vec.GetBytesAt(int(row)))
		return 1 + jsonUvarintSize(uint64(length)) + length, nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		return 0, moerr.NewInvalidInputNoCtxf(
			"binary data not supported json aggregate: %v", typ.String())
	case types.T_uuid:
		return 1 + jsonUvarintSize(36) + 36, nil
	case types.T_json:
		data := vec.GetBytesAt(int(row))
		if len(data) == 0 {
			return 0, moerr.NewInvalidInputNoCtx("invalid json aggregate value")
		}
		return len(data), nil
	case types.T_array_float32:
		return jsonArrayAggregateSize[float32](vec.GetBytesAt(int(row)))
	case types.T_array_float64:
		return jsonArrayAggregateSize[float64](vec.GetBytesAt(int(row)))
	case types.T_array_bf16:
		return jsonArrayAggregateSize[types.BF16](vec.GetBytesAt(int(row)))
	case types.T_array_float16:
		return jsonArrayAggregateSize[types.Float16](vec.GetBytesAt(int(row)))
	case types.T_array_int8:
		return jsonArrayAggregateSize[int8](vec.GetBytesAt(int(row)))
	case types.T_array_uint8:
		return jsonArrayAggregateSize[uint8](vec.GetBytesAt(int(row)))
	default:
		return 0, moerr.NewInvalidInputNoCtxf(
			"unsupported type for json aggregate: %v", typ.String())
	}
}

func jsonAggregateNumberSize(value string) (int, error) {
	var data [8]byte
	_, encoded, err := bytejson.AppendBinaryNumber(data[:0], json.Number(value))
	if err != nil {
		return 0, err
	}
	return 1 + len(encoded), nil
}

func appendJSONAggregateNumber(dst []byte, value string) ([]byte, error) {
	var data [8]byte
	numberType, encoded, err := bytejson.AppendBinaryNumber(
		data[:0], json.Number(value))
	if err != nil {
		return nil, err
	}
	dst = append(dst, byte(numberType))
	return append(dst, encoded...), nil
}

func jsonUvarintSize(value uint64) int {
	var scratch [binary.MaxVarintLen64]byte
	return binary.PutUvarint(scratch[:], value)
}

func jsonArrayAggregateSize[T types.ArrayElement](raw []byte) (int, error) {
	count := len(types.BytesToArray[T](raw))
	if uint64(count) > (math.MaxUint32-8)/13 {
		return 0, moerr.NewInvalidInputNoCtx("json array is too large")
	}
	return 1 + 8 + count*13, nil
}

func appendJSONBinaryString(dst, value []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendJSONInt64(dst []byte, value int64) []byte {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], uint64(value))
	return append(dst, data[:]...)
}

func appendJSONUint64(dst []byte, value uint64) []byte {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	return append(dst, data[:]...)
}

func appendJSONFloat64(dst []byte, value float64) []byte {
	return appendJSONUint64(dst, math.Float64bits(value))
}

func appendJSONAggregateValue(
	dst []byte,
	vec *vector.Vector,
	row uint64,
) ([]byte, error) {
	if vec.IsNull(row) {
		return append(dst, bytejson.TpCodeLiteral, bytejson.LiteralNull), nil
	}
	typ := vec.GetType()
	switch typ.Oid {
	case types.T_any:
		return append(dst, bytejson.TpCodeLiteral, bytejson.LiteralNull), nil
	case types.T_bool:
		literal := bytejson.LiteralFalse
		if vector.MustFixedColNoTypeCheck[bool](vec)[row] {
			literal = bytejson.LiteralTrue
		}
		return append(dst, bytejson.TpCodeLiteral, literal), nil
	case types.T_int8:
		dst = append(dst, bytejson.TpCodeInt64)
		return appendJSONInt64(dst, int64(vector.MustFixedColNoTypeCheck[int8](vec)[row])), nil
	case types.T_int16:
		dst = append(dst, bytejson.TpCodeInt64)
		return appendJSONInt64(dst, int64(vector.MustFixedColNoTypeCheck[int16](vec)[row])), nil
	case types.T_int32:
		dst = append(dst, bytejson.TpCodeInt64)
		return appendJSONInt64(dst, int64(vector.MustFixedColNoTypeCheck[int32](vec)[row])), nil
	case types.T_int64:
		dst = append(dst, bytejson.TpCodeInt64)
		return appendJSONInt64(dst, vector.MustFixedColNoTypeCheck[int64](vec)[row]), nil
	case types.T_uint8:
		dst = append(dst, bytejson.TpCodeUint64)
		return appendJSONUint64(dst, uint64(vector.MustFixedColNoTypeCheck[uint8](vec)[row])), nil
	case types.T_uint16:
		dst = append(dst, bytejson.TpCodeUint64)
		return appendJSONUint64(dst, uint64(vector.MustFixedColNoTypeCheck[uint16](vec)[row])), nil
	case types.T_uint32:
		dst = append(dst, bytejson.TpCodeUint64)
		return appendJSONUint64(dst, uint64(vector.MustFixedColNoTypeCheck[uint32](vec)[row])), nil
	case types.T_uint64:
		dst = append(dst, bytejson.TpCodeUint64)
		return appendJSONUint64(dst, vector.MustFixedColNoTypeCheck[uint64](vec)[row]), nil
	case types.T_float32:
		dst = append(dst, bytejson.TpCodeFloat64)
		return appendJSONFloat64(dst, float64(vector.MustFixedColNoTypeCheck[float32](vec)[row])), nil
	case types.T_float64:
		dst = append(dst, bytejson.TpCodeFloat64)
		return appendJSONFloat64(dst, vector.MustFixedColNoTypeCheck[float64](vec)[row]), nil
	case types.T_decimal64:
		value := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)[row].Format(typ.Scale)
		return appendJSONAggregateNumber(dst, value)
	case types.T_decimal128:
		value := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[row].Format(typ.Scale)
		return appendJSONAggregateNumber(dst, value)
	case types.T_date:
		value := vector.MustFixedColNoTypeCheck[types.Date](vec)[row].String()
		dst = append(dst, bytejson.TpCodeString)
		return appendJSONBinaryString(dst, []byte(value)), nil
	case types.T_time:
		value := vector.MustFixedColNoTypeCheck[types.Time](vec)[row].String()
		dst = append(dst, bytejson.TpCodeString)
		return appendJSONBinaryString(dst, []byte(value)), nil
	case types.T_datetime:
		value := vector.MustFixedColNoTypeCheck[types.Datetime](vec)[row].String()
		dst = append(dst, bytejson.TpCodeString)
		return appendJSONBinaryString(dst, []byte(value)), nil
	case types.T_timestamp:
		value := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[row].String()
		dst = append(dst, bytejson.TpCodeString)
		return appendJSONBinaryString(dst, []byte(value)), nil
	case types.T_char, types.T_varchar, types.T_text:
		dst = append(dst, bytejson.TpCodeString)
		return appendJSONBinaryString(dst, vec.GetBytesAt(int(row))), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		return nil, moerr.NewInvalidInputNoCtxf(
			"binary data not supported json aggregate: %v", typ.String())
	case types.T_uuid:
		value := vector.MustFixedColNoTypeCheck[types.Uuid](vec)[row].String()
		dst = append(dst, bytejson.TpCodeString)
		return appendJSONBinaryString(dst, []byte(value)), nil
	case types.T_json:
		data := vec.GetBytesAt(int(row))
		if len(data) == 0 {
			return nil, moerr.NewInvalidInputNoCtx("invalid json aggregate value")
		}
		return append(dst, data...), nil
	case types.T_array_float32:
		return appendJSONArray[float32](dst, vec.GetBytesAt(int(row)), func(v float32) float64 { return float64(v) })
	case types.T_array_float64:
		return appendJSONArray[float64](dst, vec.GetBytesAt(int(row)), func(v float64) float64 { return v })
	case types.T_array_bf16:
		return appendJSONArray[types.BF16](dst, vec.GetBytesAt(int(row)), func(v types.BF16) float64 { return float64(v.ToFloat32()) })
	case types.T_array_float16:
		return appendJSONArray[types.Float16](dst, vec.GetBytesAt(int(row)), func(v types.Float16) float64 { return float64(v.ToFloat32()) })
	case types.T_array_int8:
		return appendJSONArray[int8](dst, vec.GetBytesAt(int(row)), func(v int8) float64 { return float64(v) })
	case types.T_array_uint8:
		return appendJSONArray[uint8](dst, vec.GetBytesAt(int(row)), func(v uint8) float64 { return float64(v) })
	default:
		return nil, moerr.NewInvalidInputNoCtxf(
			"unsupported type for json aggregate: %v", typ.String())
	}
}

func appendJSONArray[T types.ArrayElement](
	dst []byte,
	raw []byte,
	widen func(T) float64,
) ([]byte, error) {
	values := types.BytesToArray[T](raw)
	if uint64(len(values)) > math.MaxUint32 ||
		uint64(len(values)) > (math.MaxUint32-8)/13 {
		return nil, moerr.NewInvalidInputNoCtx("json array is too large")
	}
	dst = append(dst, bytejson.TpCodeArray)
	dataStart := len(dst)
	dataSize := 8 + len(values)*13
	end := dataStart + dataSize
	if end < dataStart || end > cap(dst) {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	dst = dst[:end]
	clear(dst[dataStart:end])
	binary.LittleEndian.PutUint32(dst[dataStart:], uint32(len(values)))
	binary.LittleEndian.PutUint32(dst[dataStart+4:], uint32(dataSize))
	payload := dataStart + 8 + len(values)*5
	for i, value := range values {
		entry := dataStart + 8 + i*5
		dst[entry] = bytejson.TpCodeFloat64
		binary.LittleEndian.PutUint32(dst[entry+1:], uint32(payload-dataStart))
		binary.LittleEndian.PutUint64(dst[payload:], math.Float64bits(widen(value)))
		payload += 8
	}
	return dst, nil
}

func (exec *jsonArrayAggExec) batchFillAccounted(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if len(vectors) != 1 {
		return mpool.ErrAllocationAccountInvalid
	}
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		valueSize, err := jsonAggregateValueSize(vectors[0], uint64(row))
		if err != nil {
			return err
		}
		x, y := exec.getXY(group - 1)
		state := &exec.state[x]
		header := kAggArgPrefixSz + kAggArgOrdinalSz
		key, err := state.resizeArgScratch(exec.mp, header+5+valueSize)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint16(key[:kAggArgPrefixSz], y)
		binary.BigEndian.PutUint32(key[kAggArgPrefixSz:header], state.argCnt[y])
		payload := key[header : header+5]
		payload[0] = 1
		binary.NativeEndian.PutUint32(payload[1:], uint32(valueSize))
		payload, err = appendJSONAggregateValue(payload, vectors[0], uint64(row))
		if err != nil {
			return err
		}
		if err := state.insertPreparedArg(exec.mp, y, key[:header+len(payload)], false); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonObjectAggExec) batchFillAccounted(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	if len(vectors) != 2 {
		return mpool.ErrAllocationAccountInvalid
	}
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		logicalRow := offset + i
		keyRow, valueRow := logicalRow, logicalRow
		if vectors[0].IsConst() {
			keyRow = 0
		}
		if vectors[1].IsConst() {
			valueRow = 0
		}
		if vectors[0].IsNull(uint64(keyRow)) {
			return moerr.NewInvalidInputNoCtx("json_objectagg key cannot be NULL")
		}
		key, err := getStringKey(vectors[0], uint64(keyRow))
		if err != nil {
			return err
		}
		valueSize, err := jsonAggregateValueSize(vectors[1], uint64(valueRow))
		if err != nil {
			return err
		}
		x, y := exec.getXY(group - 1)
		state := &exec.state[x]
		header := kAggArgPrefixSz + kAggArgOrdinalSz
		total := header + 10 + len(key) + valueSize
		scratch, err := state.resizeArgScratch(exec.mp, total)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint16(scratch[:kAggArgPrefixSz], y)
		binary.BigEndian.PutUint32(scratch[kAggArgPrefixSz:header], state.argCnt[y])
		payload := appendJSONPayloadField(scratch[header:header], []byte(key))
		valueHeader := len(payload)
		payload = payload[:valueHeader+5]
		payload[valueHeader] = 1
		binary.NativeEndian.PutUint32(payload[valueHeader+1:], uint32(valueSize))
		payload, err = appendJSONAggregateValue(payload, vectors[1], uint64(valueRow))
		if err != nil {
			return err
		}
		if err := state.insertPreparedArg(exec.mp, y, scratch[:header+len(payload)], false); err != nil {
			return err
		}
	}
	return nil
}

type jsonAggregateEntry struct {
	key     []byte
	value   []byte
	ordinal uint32
}

func parseJSONArrayEntry(info *aggInfo, raw []byte) (jsonAggregateEntry, error) {
	if len(raw) < kAggArgPrefixSz+kAggArgOrdinalSz {
		return jsonAggregateEntry{}, moerr.NewInvalidInputNoCtx("invalid json aggregate entry")
	}
	entry := jsonAggregateEntry{
		ordinal: binary.BigEndian.Uint32(raw[kAggArgPrefixSz : kAggArgPrefixSz+kAggArgOrdinalSz]),
	}
	err := payloadFieldIterator(aggPayloadFromKey(info, raw), 1,
		func(_ int, isNull bool, data []byte) error {
			if isNull || len(data) == 0 {
				return moerr.NewInvalidInputNoCtx("invalid json aggregate value")
			}
			entry.value = data
			return nil
		})
	return entry, err
}

func parseJSONObjectEntry(info *aggInfo, raw []byte) (jsonAggregateEntry, error) {
	if len(raw) < kAggArgPrefixSz+kAggArgOrdinalSz {
		return jsonAggregateEntry{}, moerr.NewInvalidInputNoCtx("invalid json aggregate entry")
	}
	entry := jsonAggregateEntry{
		ordinal: binary.BigEndian.Uint32(raw[kAggArgPrefixSz : kAggArgPrefixSz+kAggArgOrdinalSz]),
	}
	err := payloadFieldIterator(aggPayloadFromKey(info, raw), 2,
		func(field int, isNull bool, data []byte) error {
			if isNull {
				return moerr.NewInvalidInputNoCtx("invalid json object aggregate entry")
			}
			if field == 0 {
				entry.key = data
			} else {
				if len(data) == 0 {
					return moerr.NewInvalidInputNoCtx("invalid json aggregate value")
				}
				entry.value = data
			}
			return nil
		})
	return entry, err
}

func jsonEntryValue(entry jsonAggregateEntry) (bytejson.ByteJson, error) {
	if len(entry.value) == 0 {
		return bytejson.ByteJson{}, moerr.NewInvalidInputNoCtx("invalid json aggregate value")
	}
	return types.DecodeJson(entry.value), nil
}

type jsonArrayEntrySource []jsonAggregateEntry

func (s jsonArrayEntrySource) Len() int { return len(s) }
func (s jsonArrayEntrySource) Value(index int) (bytejson.ByteJson, error) {
	return jsonEntryValue(s[index])
}

type jsonObjectEntrySource []jsonAggregateEntry

func (s jsonObjectEntrySource) Len() int             { return len(s) }
func (s jsonObjectEntrySource) Key(index int) []byte { return s[index].key }
func (s jsonObjectEntrySource) Value(index int) (bytejson.ByteJson, error) {
	return jsonEntryValue(s[index])
}

func (exec *jsonArrayAggExec) flushAccounted() (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, vec := range vecs {
				if vec != nil {
					vec.Free(exec.mp)
				}
			}
		}
	}()
	for chunk := range exec.state {
		state := &exec.state[chunk]
		var err error
		vecs[chunk], err = exec.allocation.newVector(exec.retType)
		if err != nil {
			return nil, err
		}
		if err := vecs[chunk].PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		for group := 0; group < int(state.length); group++ {
			if state.argCnt[group] == 0 {
				if err := vector.AppendNull(vecs[chunk], exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			entries, err := makeAccountedScratch[jsonAggregateEntry](
				exec.allocation, exec.mp, int(state.argCnt[group]))
			if err != nil {
				return nil, err
			}
			count := 0
			err = state.iter(uint16(group), func(raw []byte) error {
				entry, err := parseJSONArrayEntry(&exec.aggInfo, raw)
				if err == nil {
					entries[count] = entry
					count++
				}
				return err
			})
			if err == nil && exec.distinct {
				slices.SortFunc(entries[:count], func(left, right jsonAggregateEntry) int {
					leftNull := len(left.value) == 2 && left.value[0] == bytejson.TpCodeLiteral && left.value[1] == bytejson.LiteralNull
					rightNull := len(right.value) == 2 && right.value[0] == bytejson.TpCodeLiteral && right.value[1] == bytejson.LiteralNull
					if leftNull != rightNull {
						if leftNull {
							return -1
						}
						return 1
					}
					if leftNull {
						return int(left.ordinal) - int(right.ordinal)
					}
					if cmp := bytes.Compare(left.value, right.value); cmp != 0 {
						return cmp
					}
					return int(left.ordinal) - int(right.ordinal)
				})
				out := 0
				for _, entry := range entries[:count] {
					isNull := len(entry.value) == 2 && entry.value[0] == bytejson.TpCodeLiteral && entry.value[1] == bytejson.LiteralNull
					if !isNull && out > 0 && bytes.Equal(entries[out-1].value, entry.value) {
						continue
					}
					entries[out] = entry
					out++
				}
				count = out
			}
			if err == nil {
				slices.SortFunc(entries[:count], func(left, right jsonAggregateEntry) int {
					return int(left.ordinal) - int(right.ordinal)
				})
				encoder := bytejson.NewIndexedArrayEncoder(
					jsonArrayEntrySource(entries[:count]))
				err = vector.AppendByteJsonEncoded(vecs[chunk], encoder, exec.mp)
			}
			mpool.FreeSlice(exec.mp, entries)
			if err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func (exec *jsonObjectAggExec) flushAccounted() (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, vec := range vecs {
				if vec != nil {
					vec.Free(exec.mp)
				}
			}
		}
	}()
	for chunk := range exec.state {
		state := &exec.state[chunk]
		var err error
		vecs[chunk], err = exec.allocation.newVector(exec.retType)
		if err != nil {
			return nil, err
		}
		if err := vecs[chunk].PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		for group := 0; group < int(state.length); group++ {
			if state.argCnt[group] == 0 {
				if err := vector.AppendNull(vecs[chunk], exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			entries, err := makeAccountedScratch[jsonAggregateEntry](
				exec.allocation, exec.mp, int(state.argCnt[group]))
			if err != nil {
				return nil, err
			}
			count := 0
			err = state.iter(uint16(group), func(raw []byte) error {
				entry, err := parseJSONObjectEntry(&exec.aggInfo, raw)
				if err == nil {
					entries[count] = entry
					count++
				}
				return err
			})
			if err == nil {
				slices.SortFunc(entries[:count], func(left, right jsonAggregateEntry) int {
					if cmp := bytes.Compare(left.key, right.key); cmp != 0 {
						return cmp
					}
					return int(left.ordinal) - int(right.ordinal)
				})
				out := 0
				for index := 0; index < count; {
					last := index
					for last+1 < count && bytes.Equal(entries[last+1].key, entries[index].key) {
						last++
					}
					entries[out] = entries[last]
					out++
					index = last + 1
				}
				count = out
				encoder := bytejson.NewIndexedObjectEncoder(
					jsonObjectEntrySource(entries[:count]))
				err = vector.AppendByteJsonEncoded(vecs[chunk], encoder, exec.mp)
			}
			mpool.FreeSlice(exec.mp, entries)
			if err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func buildValueByteJson(vec *vector.Vector, row uint64) (bytejson.ByteJson, error) {
	typ := vec.GetType()
	switch typ.Oid {
	case types.T_any:
		return bytejson.Null, nil
	case types.T_bool:
		val := vector.MustFixedColNoTypeCheck[bool](vec)[int(row)]
		return bytejson.CreateByteJSONWithCheck(val)
	case types.T_int8:
		return bytejson.CreateByteJSONWithCheck(int64(vector.MustFixedColNoTypeCheck[int8](vec)[int(row)]))
	case types.T_int16:
		return bytejson.CreateByteJSONWithCheck(int64(vector.MustFixedColNoTypeCheck[int16](vec)[int(row)]))
	case types.T_int32:
		return bytejson.CreateByteJSONWithCheck(int64(vector.MustFixedColNoTypeCheck[int32](vec)[int(row)]))
	case types.T_int64:
		return bytejson.CreateByteJSONWithCheck(vector.MustFixedColNoTypeCheck[int64](vec)[int(row)])
	case types.T_uint8:
		return bytejson.CreateByteJSONWithCheck(uint64(vector.MustFixedColNoTypeCheck[uint8](vec)[int(row)]))
	case types.T_uint16:
		return bytejson.CreateByteJSONWithCheck(uint64(vector.MustFixedColNoTypeCheck[uint16](vec)[int(row)]))
	case types.T_uint32:
		return bytejson.CreateByteJSONWithCheck(uint64(vector.MustFixedColNoTypeCheck[uint32](vec)[int(row)]))
	case types.T_uint64:
		return bytejson.CreateByteJSONWithCheck(vector.MustFixedColNoTypeCheck[uint64](vec)[int(row)])
	case types.T_float32:
		return bytejson.CreateByteJSONWithCheck(float64(vector.MustFixedColNoTypeCheck[float32](vec)[int(row)]))
	case types.T_float64:
		return bytejson.CreateByteJSONWithCheck(vector.MustFixedColNoTypeCheck[float64](vec)[int(row)])
	case types.T_decimal64:
		val := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)[int(row)]
		return bytejson.CreateByteJSONWithCheck(json.Number(val.Format(typ.Scale)))
	case types.T_decimal128:
		val := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[int(row)]
		return bytejson.CreateByteJSONWithCheck(json.Number(val.Format(typ.Scale)))
	case types.T_date:
		val := vector.MustFixedColNoTypeCheck[types.Date](vec)[int(row)]
		return bytejson.CreateByteJSONWithCheck(val.String())
	case types.T_time:
		val := vector.MustFixedColNoTypeCheck[types.Time](vec)[int(row)]
		return bytejson.CreateByteJSONWithCheck(val.String())
	case types.T_datetime:
		val := vector.MustFixedColNoTypeCheck[types.Datetime](vec)[int(row)]
		return bytejson.CreateByteJSONWithCheck(val.String())
	case types.T_timestamp:
		val := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[int(row)]
		return bytejson.CreateByteJSONWithCheck(val.String())
	case types.T_char, types.T_varchar, types.T_text:
		val := vector.GenerateFunctionStrParameter(vec)
		data, _ := val.GetStrValue(row)
		return bytejson.CreateByteJSONWithCheck(string(data))
	case types.T_binary, types.T_varbinary, types.T_blob:
		return bytejson.ByteJson{}, moerr.NewInvalidInputNoCtxf("binary data not supported json aggregate: %v", typ.String())
	case types.T_array_float32:
		val := vector.GenerateFunctionStrParameter(vec)
		data, _ := val.GetStrValue(row)
		arr := types.BytesToArray[float32](data)
		res := make([]any, 0, len(arr))
		for i := range arr {
			res = append(res, float64(arr[i]))
		}
		return bytejson.CreateByteJSONWithCheck(res)
	case types.T_array_float64:
		val := vector.GenerateFunctionStrParameter(vec)
		data, _ := val.GetStrValue(row)
		arr := types.BytesToArray[float64](data)
		res := make([]any, 0, len(arr))
		for i := range arr {
			res = append(res, arr[i])
		}
		return bytejson.CreateByteJSONWithCheck(res)
	// Narrow vector element types. BF16/Float16 widen to float32 for JSON;
	// int8/uint8 emit as JSON numbers. Each needs its own decode because
	// BytesToArray is element-typed, so a single IsArrayRelate arm cannot work.
	case types.T_array_bf16:
		val := vector.GenerateFunctionStrParameter(vec)
		data, _ := val.GetStrValue(row)
		arr := types.BytesToArray[types.BF16](data)
		res := make([]any, 0, len(arr))
		for i := range arr {
			res = append(res, float64(arr[i].ToFloat32()))
		}
		return bytejson.CreateByteJSONWithCheck(res)
	case types.T_array_float16:
		val := vector.GenerateFunctionStrParameter(vec)
		data, _ := val.GetStrValue(row)
		arr := types.BytesToArray[types.Float16](data)
		res := make([]any, 0, len(arr))
		for i := range arr {
			res = append(res, float64(arr[i].ToFloat32()))
		}
		return bytejson.CreateByteJSONWithCheck(res)
	case types.T_array_int8:
		val := vector.GenerateFunctionStrParameter(vec)
		data, _ := val.GetStrValue(row)
		arr := types.BytesToArray[int8](data)
		res := make([]any, 0, len(arr))
		for i := range arr {
			res = append(res, float64(arr[i]))
		}
		return bytejson.CreateByteJSONWithCheck(res)
	case types.T_array_uint8:
		val := vector.GenerateFunctionStrParameter(vec)
		data, _ := val.GetStrValue(row)
		arr := types.BytesToArray[uint8](data)
		res := make([]any, 0, len(arr))
		for i := range arr {
			res = append(res, float64(arr[i]))
		}
		return bytejson.CreateByteJSONWithCheck(res)
	case types.T_uuid:
		val := vector.MustFixedColNoTypeCheck[types.Uuid](vec)[int(row)]
		return bytejson.CreateByteJSONWithCheck(val.String())
	case types.T_json:
		val := vector.GenerateFunctionStrParameter(vec)
		data, _ := val.GetStrValue(row)
		return types.DecodeJson(data), nil
	default:
		return bytejson.ByteJson{}, moerr.NewInvalidInputNoCtxf("unsupported type for json aggregate: %v", typ.String())
	}
}

func getStringKey(vec *vector.Vector, row uint64) (string, error) {
	if !vec.GetType().Oid.IsMySQLString() {
		return "", moerr.NewInvalidInputNoCtxf("json_objectagg key must be a string, got %v", vec.GetType().String())
	}
	param := vector.GenerateFunctionStrParameter(vec)
	data, _ := param.GetStrValue(row)
	return string(data), nil
}
