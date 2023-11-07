// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package group_concat

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

// +------+------+------+
// | a    | b    | c    |
// +------+------+------+
// |    1 |    2 |    3 |
// |    4 |    5 |    6 |
// +------+------+------+
// select group_concat(a,b,c separator "|") from t;
// res[0] = "123|456"
// inserts = "encode(1,2,3)|encode(4,5,6)
// we need inserts to store the source keys, so we can use then where merge
type GroupConcat struct {
	arg     *Argument
	res     []string
	inserts []string
	maps    []*hashmap.StrHashMap
	// in group_concat(distinct a,b ), the itype will be a and b's types
	ityp   []types.Type
	groups int // groups record the real group number
}

type EncodeGroupConcat struct {
	ResStrData     []byte
	InsertsStrData []byte
	Arg            *Argument
	Ityp           []types.Type
	Groups         int
}

func (m *EncodeGroupConcat) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *EncodeGroupConcat) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func NewGroupConcat(arg *Argument, typs []types.Type) agg.Agg[any] {
	return &GroupConcat{
		arg:  arg,
		ityp: typs,
	}
}

// todo need improve performance
func (gc *GroupConcat) Dup(_ *mpool.MPool) agg.Agg[any] {
	val := &GroupConcat{
		arg: &Argument{
			Dist:        gc.arg.Dist,
			GroupExpr:   make([]*plan.Expr, len(gc.arg.GroupExpr)),
			OrderByExpr: make([]*plan.Expr, len(gc.arg.GroupExpr)),
			Separator:   gc.arg.Separator,
			OrderId:     gc.arg.OrderId,
		},
		res:     make([]string, len(gc.res)),
		inserts: make([]string, len(gc.inserts)),
		ityp:    make([]types.Type, len(gc.ityp)),
		groups:  gc.groups,
	}
	for i, expr := range gc.arg.GroupExpr {
		gc.arg.GroupExpr[i] = plan.DeepCopyExpr(expr)
	}
	for i, expr := range gc.arg.OrderByExpr {
		gc.arg.OrderByExpr[i] = plan.DeepCopyExpr(expr)
	}
	copy(val.res, gc.res)
	copy(val.inserts, gc.inserts)
	copy(val.ityp, gc.ityp)

	return val
}

// We need to implements the interface of Agg
func (gc *GroupConcat) MarshalBinary() (data []byte, err error) {
	eg := &EncodeGroupConcat{
		ResStrData:     types.EncodeStringSlice(gc.res),
		InsertsStrData: types.EncodeStringSlice(gc.inserts),
		Arg:            gc.arg,
		Groups:         gc.groups,
		Ityp:           gc.ityp,
	}
	return types.Encode(eg)
}

// encoding.BinaryUnmarshaler
func (gc *GroupConcat) UnmarshalBinary(data []byte) error {
	eg := &EncodeGroupConcat{}
	types.Decode(data, eg)
	m := mpool.MustNewZeroNoFixed()
	da1, err := m.Alloc(len(eg.InsertsStrData))
	if err != nil {
		return err
	}
	copy(da1, eg.InsertsStrData)
	gc.inserts = types.DecodeStringSlice(da1)
	da2, err := m.Alloc(len(eg.ResStrData))
	if err != nil {
		return err
	}
	copy(da2, eg.ResStrData)
	gc.res = types.DecodeStringSlice(da2)
	gc.arg = eg.Arg
	gc.groups = eg.Groups
	gc.ityp = eg.Ityp
	gc.maps = make([]*hashmap.StrHashMap, gc.groups)
	for i := 0; i < gc.groups; i++ {
		gc.maps[i], err = hashmap.NewStrMap(false, 0, 0, m)
		if err != nil {
			return err
		}
		for k := range gc.inserts {
			gc.maps[i].InsertValue(gc.inserts[k])
		}
	}
	return nil
}

// Type return the type of the agg's result.
func (gc *GroupConcat) OutputType() types.Type {
	typ := types.T_text.ToType()
	if gc.ityp[0].Oid == types.T_binary || gc.ityp[0].Oid == types.T_varbinary || gc.ityp[0].Oid == types.T_blob {
		typ = types.T_blob.ToType()
	}
	// set to largest length
	typ.Width = types.MaxVarcharLen
	return typ
}

// group_concat is not a normal agg func, we don't need this func
func (gc *GroupConcat) InputTypes() []types.Type {
	return gc.ityp
}

// Free the agg.
func (gc *GroupConcat) Free(*mpool.MPool) {
	for _, mp := range gc.maps {
		mp.Free()
	}
	gc.maps = nil
}

// Grows allocates n groups for the agg.
func (gc *GroupConcat) Grows(n int, m *mpool.MPool) error {
	if len(gc.res) == 0 {
		gc.res = make([]string, 0, n)
		gc.inserts = make([]string, 0, n)
		for i := 0; i < n; i++ {
			gc.res = append(gc.res, "")
			gc.inserts = append(gc.inserts, "")
		}
		if gc.arg.Dist {
			gc.maps = make([]*hashmap.StrHashMap, 0, n)
			for i := 0; i < n; i++ {
				mp, err := hashmap.NewStrMap(false, 0, 0, m)
				if err != nil {
					return err
				}
				gc.maps = append(gc.maps, mp)
			}
		}
	} else {
		for i := 0; i < n; i++ {
			gc.res = append(gc.res, "")
			gc.inserts = append(gc.inserts, "")
			if gc.arg.Dist {
				mp, err := hashmap.NewStrMap(false, 0, 0, m)
				if err != nil {
					return err
				}
				gc.maps = append(gc.maps, mp)
			}
		}
	}
	return nil
}

// Eval method calculates and returns the final result of the aggregate function.
func (gc *GroupConcat) Eval(m *mpool.MPool) (*vector.Vector, error) {
	vec := vector.NewVec(gc.OutputType())
	nsp := nulls.NewWithSize(gc.groups)
	vec.SetNulls(nsp)
	for _, v := range gc.res {
		if err := vector.AppendBytes(vec, []byte(v), false, m); err != nil {
			vec.Free(m)
			return nil, err
		}
	}
	return vec, nil
}

// Fill use the rowIndex-rows of vector to update the data of groupIndex-group.
// rowCount indicates the number of times the rowIndex-row is repeated.
// for group_concat(distinct a,b,c separator '|'); vecs is: a,b,c
// remember that, we won't do evalExpr here, so the groupExpr is not used here
func (gc *GroupConcat) Fill(groupIndex int64, rowIndex int64, vecs []*vector.Vector) error {
	if hasNull(vecs, rowIndex) {
		return nil
	}
	length := len(gc.arg.GroupExpr)
	var res_row string
	var insert_row string
	var flag bool
	var err error
	for i := 0; i < length; i++ {
		s, _ := VectorToString(vecs[i], int(rowIndex))
		res_row += s
		// prefix length + data
		l := len(s)
		if l > math.MaxUint16 {
			panic("too long")
		}
		bs := make([]byte, 2)
		binary.LittleEndian.PutUint16(bs, uint16(l))
		insert_row += unsafe.String(&bs[0], 2) + s
	}
	if gc.arg.Dist {
		if flag, err = gc.maps[groupIndex].InsertValue(insert_row); err != nil {
			return err
		}
		if flag {
			if len(gc.res[groupIndex]) != 0 {
				gc.res[groupIndex] += gc.arg.Separator
				gc.inserts[groupIndex] += gc.arg.Separator
			} else {
				gc.groups++
			}
			gc.res[groupIndex] += res_row
			gc.inserts[groupIndex] += insert_row
		}
	} else {
		if len(gc.res[groupIndex]) != 0 {
			gc.res[groupIndex] += gc.arg.Separator
			gc.inserts[groupIndex] += gc.arg.Separator
		} else {
			gc.groups++
		}
		gc.res[groupIndex] += res_row
		gc.inserts[groupIndex] += insert_row
	}
	return nil
}

func (gc *GroupConcat) BulkFill(groupIndex int64, vecs []*vector.Vector) error {
	length := vecs[0].Length()
	for i := 0; i < length; i++ {
		if err := gc.Fill(groupIndex, int64(i), vecs); err != nil {
			return err
		}
	}
	return nil
}

// BatchFill use part of the vector to update the data of agg's group
//
//	os(origin-s) records information about which groups need to be updated
//	if length of os is N, we use first N of vps to do update work.
//	And if os[i] > 0, it means the agg's (vps[i]-1)th group is a new one (never been assigned a value),
//	Maybe this feature can help us to do some optimization work.
//	So we use the os as a parameter but not len(os).
//
//	agg's (vps[i]-1)th group is related to vector's (offset+i)th row.
//	rowCounts[i] is count number of the row[i]
//
// For a more detailed introduction of rowCounts, please refer to comments of Function Fill.
func (gc *GroupConcat) BatchFill(offset int64, os []uint8, vps []uint64, vecs []*vector.Vector) error {
	for i := range os {
		if vps[i] == 0 {
			continue
		}
		if err := gc.Fill(int64(vps[i]-1), offset+int64(i), vecs); err != nil {
			return err
		}
	}
	return nil
}

// Merge will merge a couple of group between 2 aggregate function structures.
// It merges the groupIndex1-group of agg1 and
// groupIndex2-group of agg2
func (gc *GroupConcat) Merge(agg2 agg.Agg[any], groupIndex1 int64, groupIndex2 int64) error {
	gc2 := agg2.(*GroupConcat)
	if gc.arg.Dist {
		rows := strings.Split(gc2.inserts[groupIndex2], gc2.arg.Separator)
		ress := strings.Split(gc2.res[groupIndex2], gc2.arg.Separator)
		for i, row := range rows {
			if len(row) == 0 {
				continue
			}
			flag, err := gc.maps[groupIndex1].InsertValue(row)
			if err != nil {
				return err
			}
			if flag {
				if len(gc.res[groupIndex1]) > 0 {
					gc.res[groupIndex1] += gc.arg.Separator
				} else {
					gc.groups++
				}
				gc.res[groupIndex1] += ress[i]
			}
		}
	} else {
		if len(gc.res[groupIndex1]) > 0 {
			gc.res[groupIndex1] += gc.arg.Separator
		} else {
			gc.groups++
		}
		gc.res[groupIndex1] += gc2.res[groupIndex2]
	}
	return nil
}

// BatchMerge merges multi groups of agg1 and agg2
//
//	agg1's (vps[i]-1)th group is related to agg2's (start+i)th group
//
// For more introduction of os, please refer to comments of Function BatchFill.
func (gc *GroupConcat) BatchMerge(agg2 agg.Agg[any], start int64, os []uint8, vps []uint64) error {
	gc2 := agg2.(*GroupConcat)
	for i := range os {
		if vps[i] == 0 {
			continue
		}
		if err := gc.Merge(gc2, int64(vps[i]-1), int64(i+int(start))); err != nil {
			return err
		}
	}
	return nil
}

// GetOperatorId get types of aggregate's aggregate id.
// this is used to print log in group string();
func (gc *GroupConcat) GetOperatorId() int64 {
	return function.GroupConcatFunctionID
}

func (gc *GroupConcat) IsDistinct() bool {
	return gc.arg.Dist
}

// WildAggReAlloc reallocate for agg structure from memory pool.
func (gc *GroupConcat) WildAggReAlloc(m *mpool.MPool) error {
	for i := 0; i < len(gc.res); i++ {
		d, err := m.Alloc(len(gc.res[i]))
		if err != nil {
			return err
		}
		copy(d, []byte(gc.res[i]))
		gc.res[i] = string(d)
	}
	for i := 0; i < len(gc.inserts); i++ {
		d, err := m.Alloc(len(gc.inserts[i]))
		if err != nil {
			return err
		}
		copy(d, []byte(gc.inserts[i]))
		gc.inserts[i] = string(d)
	}
	return nil
}

func VectorToString(vec *vector.Vector, rowIndex int) (string, error) {
	//if nulls.Any(vec.GetNulls()) {
	//	return "", nil
	//}
	switch vec.GetType().Oid {
	case types.T_bool:
		flag := vector.GetFixedAt[bool](vec, rowIndex)
		if flag {
			return "1", nil
		}
		return "0", nil
	case types.T_int8:
		return fmt.Sprintf("%v", vector.GetFixedAt[int8](vec, rowIndex)), nil
	case types.T_int16:
		return fmt.Sprintf("%v", vector.GetFixedAt[int16](vec, rowIndex)), nil
	case types.T_int32:
		return fmt.Sprintf("%v", vector.GetFixedAt[int32](vec, rowIndex)), nil
	case types.T_int64:
		return fmt.Sprintf("%v", vector.GetFixedAt[int64](vec, rowIndex)), nil
	case types.T_uint8:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint8](vec, rowIndex)), nil
	case types.T_uint16:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint16](vec, rowIndex)), nil
	case types.T_uint32:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint32](vec, rowIndex)), nil
	case types.T_uint64:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint64](vec, rowIndex)), nil
	case types.T_float32:
		return fmt.Sprintf("%v", vector.GetFixedAt[float32](vec, rowIndex)), nil
	case types.T_float64:
		return fmt.Sprintf("%v", vector.GetFixedAt[float64](vec, rowIndex)), nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob:
		return vec.GetStringAt(rowIndex), nil
	case types.T_array_float32:
		return types.ArrayToString[float32](vector.GetArrayAt[float32](vec, rowIndex)), nil
	case types.T_array_float64:
		return types.ArrayToString[float64](vector.GetArrayAt[float64](vec, rowIndex)), nil
	case types.T_decimal64:
		val := vector.GetFixedAt[types.Decimal64](vec, rowIndex)
		return val.Format(vec.GetType().Scale), nil
	case types.T_decimal128:
		val := vector.GetFixedAt[types.Decimal128](vec, rowIndex)
		return val.Format(vec.GetType().Scale), nil
	case types.T_json:
		val := vec.GetBytesAt(rowIndex)
		byteJson := types.DecodeJson(val)
		return byteJson.String(), nil
	case types.T_uuid:
		val := vector.GetFixedAt[types.Uuid](vec, rowIndex)
		return val.ToString(), nil
	case types.T_date:
		val := vector.GetFixedAt[types.Date](vec, rowIndex)
		return val.String(), nil
	case types.T_time:
		val := vector.GetFixedAt[types.Time](vec, rowIndex)
		return val.String(), nil
	case types.T_datetime:
		val := vector.GetFixedAt[types.Datetime](vec, rowIndex)
		return val.String(), nil
	case types.T_enum:
		return fmt.Sprintf("%v", vector.GetFixedAt[uint16](vec, rowIndex)), nil
	default:
		return "", nil
	}
}

func hasNull(vecs []*vector.Vector, rowIdx int64) bool {
	for i := 0; i < len(vecs); i++ {
		if vecs[i].IsConstNull() || vecs[i].GetNulls().Contains(uint64(rowIdx)) {
			return true
		}
	}
	return false
}
