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
	"encoding/json"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func RegisterJsonArrayAgg(id int64) {
	specialAgg[id] = true
	AggIdOfJsonArrayAgg = id
}

func RegisterJsonObjectAgg(id int64) {
	specialAgg[id] = true
	AggIdOfJsonObjectAgg = id
}

type jsonArrayAggExec struct {
	aggExec
	distinct     bool
	distinctHash distinctHash
	groups       []struct{}
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
	if exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	exec.groups = append(exec.groups, make([]struct{}, more)...)
	return nil
}

func (exec *jsonArrayAggExec) PreAllocateGroups(more int) error {
	if exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.PreAllocateGroups(more); err != nil {
		return err
	}
	exec.groups = append(exec.groups, make([]struct{}, more)...)
	return nil
}

func (exec *jsonArrayAggExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *jsonArrayAggExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *jsonArrayAggExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
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
	if exec.distinct {
		if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
			return err
		}
	}
	return exec.batchMergeArgs(&other.aggExec, offset, groups, false)
}

func (exec *jsonArrayAggExec) SetExtraInformation(_ any, _ int) error {
	return nil
}

func (exec *jsonArrayAggExec) Flush() ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(exec.state))
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
	groups       []struct{}
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
	if exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	exec.groups = append(exec.groups, make([]struct{}, more)...)
	return nil
}

func (exec *jsonObjectAggExec) PreAllocateGroups(more int) error {
	if exec.distinct {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	if err := exec.aggExec.PreAllocateGroups(more); err != nil {
		return err
	}
	exec.groups = append(exec.groups, make([]struct{}, more)...)
	return nil
}

func (exec *jsonObjectAggExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *jsonObjectAggExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *jsonObjectAggExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
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
	if exec.distinct {
		if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
			return err
		}
	}
	return exec.batchMergeArgs(&other.aggExec, offset, groups, false)
}

func (exec *jsonObjectAggExec) SetExtraInformation(_ any, _ int) error {
	return nil
}

func (exec *jsonObjectAggExec) Flush() ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(exec.state))
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
