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

type jsonArrayAggGroup struct {
	values []bytejson.ByteJson
}

type jsonArrayAggExec struct {
	multiAggInfo
	ret aggResultWithBytesType

	distinctHash
	groups []jsonArrayAggGroup
}

func (exec *jsonArrayAggExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *jsonArrayAggExec) marshal() ([]byte, error) {
	d := exec.multiAggInfo.getEncoded()
	r, em, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}

	var groupData [][]byte
	if len(exec.groups) > 0 {
		groupData = make([][]byte, 0, len(exec.groups)+1)
		if exec.IsDistinct() {
			data, err := exec.distinctHash.marshal()
			if err != nil {
				return nil, err
			}
			groupData = append(groupData, data)
		}
		for i := range exec.groups {
			data, err := marshalJsonArrayGroup(exec.groups[i])
			if err != nil {
				return nil, err
			}
			groupData = append(groupData, data)
		}
	}

	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  groupData,
	}
	return encoded.Marshal()
}

func (exec *jsonArrayAggExec) unmarshal(_ *mpool.MPool, result, empties, groups [][]byte) error {
	if err := exec.ret.unmarshalFromBytes(result, empties); err != nil {
		return err
	}

	offset := 0
	if exec.IsDistinct() {
		if len(groups) == 0 {
			return moerr.NewInternalErrorNoCtx("json_arrayagg distinct data missing")
		}
		if err := exec.distinctHash.unmarshal(groups[0]); err != nil {
			return err
		}
		offset = 1
	}

	groupCount := exec.ret.totalGroupCount()
	exec.groups = make([]jsonArrayAggGroup, groupCount)
	for i := 0; i < groupCount && offset+i < len(groups); i++ {
		if len(groups[offset+i]) == 0 {
			continue
		}
		if err := unmarshalJsonArrayGroup(&exec.groups[i], groups[offset+i]); err != nil {
			return err
		}
	}
	return nil
}

func newJsonArrayAggExec(mg AggMemoryManager, info multiAggInfo) *jsonArrayAggExec {
	return &jsonArrayAggExec{
		multiAggInfo: info,
		ret:          initAggResultWithBytesTypeResult(mg, info.retType, info.emptyNull, ""),
	}
}

func (exec *jsonArrayAggExec) ensureGroup(idx int) {
	if len(exec.groups) <= idx {
		exec.groups = append(exec.groups, make([]jsonArrayAggGroup, idx-len(exec.groups)+1)...)
	}
}

func (exec *jsonArrayAggExec) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	exec.groups = append(exec.groups, make([]jsonArrayAggGroup, more)...)
	return exec.ret.grows(more)
}

func (exec *jsonArrayAggExec) PreAllocateGroups(more int) error {
	exec.groups = append(exec.groups, make([]jsonArrayAggGroup, more)...)
	return exec.ret.preExtend(more)
}

func (exec *jsonArrayAggExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsConst() {
		row = 0
	}
	if vectors[0].IsNull(uint64(row)) {
		// NULL becomes JSON null inside the array
		return exec.appendValue(groupIndex, bytejson.Null)
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); err != nil || !need {
			return err
		}
	}

	val, err := buildValueByteJson(vectors[0], uint64(row))
	if err != nil {
		return err
	}
	return exec.appendValue(groupIndex, val)
}

func (exec *jsonArrayAggExec) appendValue(groupIndex int, val bytejson.ByteJson) error {
	exec.ensureGroup(groupIndex)
	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	exec.ret.setGroupNotEmpty(x, y)

	exec.groups[groupIndex].values = append(exec.groups[groupIndex].values, val)
	return exec.updateResult(groupIndex)
}

func (exec *jsonArrayAggExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	for row := 0; row < vectors[0].Length(); row++ {
		if err := exec.Fill(groupIndex, row, vectors); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonArrayAggExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, g := range groups {
		if g == GroupNotMatched {
			continue
		}
		if err := exec.Fill(int(g-1), offset+i, vectors); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonArrayAggExec) SetExtraInformation(_ any, _ int) error {
	return nil
}

func (exec *jsonArrayAggExec) updateResult(groupIndex int) error {
	arr := make([]any, len(exec.groups[groupIndex].values))
	for i, v := range exec.groups[groupIndex].values {
		arr[i] = v
	}
	bj, err := bytejson.CreateByteJSONWithCheck(arr)
	if err != nil {
		return err
	}
	data, err := bj.Marshal()
	if err != nil {
		return err
	}
	return exec.ret.set(data)
}

func (exec *jsonArrayAggExec) merge(other *jsonArrayAggExec, idx1, idx2 int) error {
	x1, y1 := exec.ret.updateNextAccessIdx(idx1)
	x2, y2 := other.ret.updateNextAccessIdx(idx2)
	exec.ret.MergeAnotherEmpty(x1, y1, other.ret.isGroupEmpty(x2, y2))

	if other.ret.isGroupEmpty(x2, y2) {
		return nil
	}

	exec.ret.setGroupNotEmpty(x1, y1)
	exec.ensureGroup(idx1)
	other.ensureGroup(idx2)
	exec.groups[idx1].values = append(exec.groups[idx1].values, other.groups[idx2].values...)
	return exec.updateResult(idx1)
}

func (exec *jsonArrayAggExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.merge(&next.(*jsonArrayAggExec).distinctHash); err != nil {
			return err
		}
	}
	return exec.merge(next.(*jsonArrayAggExec), groupIdx1, groupIdx2)
}

func (exec *jsonArrayAggExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*jsonArrayAggExec)
	if exec.IsDistinct() {
		if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
			return err
		}
	}
	for i, g := range groups {
		if g == GroupNotMatched {
			continue
		}
		if err := exec.merge(other, offset+i, int(g-1)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonArrayAggExec) Flush() ([]*vector.Vector, error) {
	return exec.ret.flushAll(), nil
}

func (exec *jsonArrayAggExec) Free() {
	exec.distinctHash.free()
	exec.ret.free()
	exec.groups = nil
}

func (exec *jsonArrayAggExec) Size() int64 {
	return exec.ret.Size() + exec.distinctHash.Size()
}

type jsonObjectAggGroup struct {
	values map[string]bytejson.ByteJson
}

type jsonObjectAggExec struct {
	multiAggInfo
	ret aggResultWithBytesType

	distinctHash
	groups []jsonObjectAggGroup
}

func (exec *jsonObjectAggExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *jsonObjectAggExec) marshal() ([]byte, error) {
	d := exec.multiAggInfo.getEncoded()
	r, em, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}

	var groupData [][]byte
	if len(exec.groups) > 0 {
		groupData = make([][]byte, 0, len(exec.groups)+1)
		if exec.IsDistinct() {
			data, err := exec.distinctHash.marshal()
			if err != nil {
				return nil, err
			}
			groupData = append(groupData, data)
		}
		for i := range exec.groups {
			data, err := marshalJsonObjectGroup(exec.groups[i])
			if err != nil {
				return nil, err
			}
			groupData = append(groupData, data)
		}
	}

	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  groupData,
	}
	return encoded.Marshal()
}

func (exec *jsonObjectAggExec) unmarshal(_ *mpool.MPool, result, empties, groups [][]byte) error {
	if err := exec.ret.unmarshalFromBytes(result, empties); err != nil {
		return err
	}

	offset := 0
	if exec.IsDistinct() {
		if len(groups) == 0 {
			return moerr.NewInternalErrorNoCtx("json_objectagg distinct data missing")
		}
		if err := exec.distinctHash.unmarshal(groups[0]); err != nil {
			return err
		}
		offset = 1
	}

	groupCount := exec.ret.totalGroupCount()
	exec.groups = make([]jsonObjectAggGroup, groupCount)
	for i := 0; i < groupCount && offset+i < len(groups); i++ {
		if len(groups[offset+i]) == 0 {
			continue
		}
		if err := unmarshalJsonObjectGroup(&exec.groups[i], groups[offset+i]); err != nil {
			return err
		}
	}
	return nil
}

func newJsonObjectAggExec(mg AggMemoryManager, info multiAggInfo) *jsonObjectAggExec {
	return &jsonObjectAggExec{
		multiAggInfo: info,
		ret:          initAggResultWithBytesTypeResult(mg, info.retType, info.emptyNull, ""),
	}
}

func (exec *jsonObjectAggExec) ensureGroup(idx int) {
	if len(exec.groups) <= idx {
		exec.groups = append(exec.groups, make([]jsonObjectAggGroup, idx-len(exec.groups)+1)...)
	}
	if exec.groups[idx].values == nil {
		exec.groups[idx].values = make(map[string]bytejson.ByteJson)
	}
}

func (exec *jsonObjectAggExec) GroupGrow(more int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	exec.groups = append(exec.groups, make([]jsonObjectAggGroup, more)...)
	return exec.ret.grows(more)
}

func (exec *jsonObjectAggExec) PreAllocateGroups(more int) error {
	exec.groups = append(exec.groups, make([]jsonObjectAggGroup, more)...)
	return exec.ret.preExtend(more)
}

func (exec *jsonObjectAggExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	keyVec := vectors[0]
	valVec := vectors[1]

	rowKey := row
	rowVal := row
	if keyVec.IsConst() {
		rowKey = 0
	}
	if valVec.IsConst() {
		rowVal = 0
	}
	if keyVec.IsNull(uint64(rowKey)) {
		return moerr.NewInvalidInputNoCtx("json_objectagg key cannot be NULL")
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); err != nil || !need {
			return err
		}
	}

	key, err := getStringKey(keyVec, uint64(rowKey))
	if err != nil {
		return err
	}
	val := bytejson.Null
	if !valVec.IsNull(uint64(rowVal)) {
		val, err = buildValueByteJson(valVec, uint64(rowVal))
		if err != nil {
			return err
		}
	}

	exec.ensureGroup(groupIndex)
	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	exec.ret.setGroupNotEmpty(x, y)

	exec.groups[groupIndex].values[key] = val
	return exec.updateResult(groupIndex)
}

func (exec *jsonObjectAggExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	for row := 0; row < vectors[0].Length(); row++ {
		if err := exec.Fill(groupIndex, row, vectors); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonObjectAggExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, g := range groups {
		if g == GroupNotMatched {
			continue
		}
		if err := exec.Fill(int(g-1), offset+i, vectors); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonObjectAggExec) SetExtraInformation(_ any, _ int) error {
	return nil
}

func (exec *jsonObjectAggExec) updateResult(groupIndex int) error {
	m := exec.groups[groupIndex].values
	obj := make(map[string]any, len(m))
	for k, v := range m {
		obj[k] = v
	}
	bj, err := bytejson.CreateByteJSONWithCheck(obj)
	if err != nil {
		return err
	}
	data, err := bj.Marshal()
	if err != nil {
		return err
	}
	return exec.ret.set(data)
}

func (exec *jsonObjectAggExec) merge(other *jsonObjectAggExec, idx1, idx2 int) error {
	x1, y1 := exec.ret.updateNextAccessIdx(idx1)
	x2, y2 := other.ret.updateNextAccessIdx(idx2)
	exec.ret.MergeAnotherEmpty(x1, y1, other.ret.isGroupEmpty(x2, y2))

	if other.ret.isGroupEmpty(x2, y2) {
		return nil
	}

	exec.ret.setGroupNotEmpty(x1, y1)
	exec.ensureGroup(idx1)
	other.ensureGroup(idx2)

	for k, v := range other.groups[idx2].values {
		exec.groups[idx1].values[k] = v
	}
	return exec.updateResult(idx1)
}

func (exec *jsonObjectAggExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	if exec.IsDistinct() {
		if err := exec.distinctHash.merge(&next.(*jsonObjectAggExec).distinctHash); err != nil {
			return err
		}
	}
	return exec.merge(next.(*jsonObjectAggExec), groupIdx1, groupIdx2)
}

func (exec *jsonObjectAggExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*jsonObjectAggExec)
	if exec.IsDistinct() {
		if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
			return err
		}
	}
	for i, g := range groups {
		if g == GroupNotMatched {
			continue
		}
		if err := exec.merge(other, offset+i, int(g-1)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *jsonObjectAggExec) Flush() ([]*vector.Vector, error) {
	return exec.ret.flushAll(), nil
}

func (exec *jsonObjectAggExec) Free() {
	exec.distinctHash.free()
	exec.ret.free()
	exec.groups = nil
}

func (exec *jsonObjectAggExec) Size() int64 {
	return exec.ret.Size() + exec.distinctHash.Size()
}

func marshalJsonArrayGroup(g jsonArrayAggGroup) ([]byte, error) {
	if len(g.values) == 0 {
		return nil, nil
	}
	return json.Marshal(g.values)
}

func unmarshalJsonArrayGroup(g *jsonArrayAggGroup, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, &g.values)
}

func marshalJsonObjectGroup(g jsonObjectAggGroup) ([]byte, error) {
	if len(g.values) == 0 {
		return nil, nil
	}
	return json.Marshal(g.values)
}

func unmarshalJsonObjectGroup(g *jsonObjectAggGroup, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, &g.values)
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
