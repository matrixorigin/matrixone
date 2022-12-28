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
	"bytes"
	"fmt"
	"strings"
	"unsafe"

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
	res_strData     []byte
	inserts_strData []byte
	arg             *Argument
	ityp            []types.Type
	groups          int
}

func NewGroupConcat(arg *Argument, typs []types.Type) agg.Agg[any] {
	return &GroupConcat{
		arg:  arg,
		ityp: typs,
	}
}

// We need to implements the interface of Agg
func (gc *GroupConcat) MarshalBinary() (data []byte, err error) {
	eg := &EncodeGroupConcat{
		res_strData:     types.EncodeStringSlice(gc.res),
		inserts_strData: types.EncodeStringSlice(gc.inserts),
		arg:             gc.arg,
		groups:          gc.groups,
		ityp:            gc.ityp,
	}
	return types.Encode(eg)
}

// encoding.BinaryUnmarshaler
func (gc *GroupConcat) UnmarshalBinary(data []byte, m *mpool.MPool) error {
	eg := &EncodeGroupConcat{}
	types.Decode(data, eg)
	da1, err := m.Alloc(len(eg.inserts_strData))
	if err != nil {
		return err
	}
	copy(da1, eg.inserts_strData)
	gc.inserts = types.DecodeStringSlice(da1)
	da2, err := m.Alloc(len(eg.res_strData))
	if err != nil {
		return err
	}
	copy(da2, eg.res_strData)
	gc.res = types.DecodeStringSlice(da2)
	gc.arg = eg.arg
	gc.groups = eg.groups
	gc.ityp = eg.ityp
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

// Dup will duplicate a new agg with the same type.
func (gc *GroupConcat) Dup() agg.Agg[any] {
	var newRes []string = make([]string, len(gc.res))
	copy(newRes, gc.res)
	var newItyp []types.Type = make([]types.Type, 0, len(gc.ityp))
	copy(newItyp, gc.ityp)
	var inserts []string = make([]string, 0, len(gc.inserts))
	return &GroupConcat{
		arg:     gc.arg,
		res:     newRes,
		inserts: inserts,
		ityp:    newItyp,
	}
}

// Type return the type of the agg's result.
func (gc *GroupConcat) OutputType() types.Type {
	typ := types.T_text.ToType()
	// set to largest length
	typ.Width = types.MaxVarcharLen
	return typ
}

// group_concat is not a normal agg func, we don't need this func
func (gc *GroupConcat) InputTypes() []types.Type {
	return gc.ityp
}

// String return related information of the agg.
// used to show query plans.
func (gc *GroupConcat) String() string {
	buf := new(bytes.Buffer)
	buf.WriteString("group_concat( ")
	if gc.arg.Dist {
		buf.WriteString("distinct ")
	}
	for i, expr := range gc.arg.GroupExpr {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v", expr))
	}
	if len(gc.arg.OrderByExpr) > 0 {
		buf.WriteString(" order by ")
	}
	for i, expr := range gc.arg.OrderByExpr {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v", expr))
	}
	buf.WriteString(fmt.Sprintf(" separtor %v)", gc.arg.Separator))
	return buf.String()
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
	vec := vector.New(gc.OutputType())
	nsp := nulls.NewWithSize(gc.groups)
	vec.Nsp = nsp
	for _, v := range gc.res {
		if err := vec.Append([]byte(v), false, m); err != nil {
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
func (gc *GroupConcat) Fill(groupIndex int64, rowIndex int64, rowCount int64, vecs []*vector.Vector) error {
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
		length := uint16(len(s))
		insert_row += string(unsafe.Slice((*byte)(unsafe.Pointer(&length)), 2)) + s
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
		for k := 0; k < int(rowCount); k++ {
			if len(gc.res[groupIndex]) != 0 {
				gc.res[groupIndex] += gc.arg.Separator
				gc.inserts[groupIndex] += gc.arg.Separator
			} else {
				gc.groups++
			}
			gc.res[groupIndex] += res_row
			gc.inserts[groupIndex] += insert_row
		}
	}
	return nil
}

// BulkFill use a whole vector to update the data of agg's group
// groupIndex is the index number of the group
// rowCounts is the count number of each row.
func (gc *GroupConcat) BulkFill(groupIndex int64, rowCounts []int64, vecs []*vector.Vector) error {
	length := vecs[0].Length()
	for i := 0; i < length; i++ {
		if err := gc.Fill(groupIndex, int64(i), rowCounts[i], vecs); err != nil {
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
func (gc *GroupConcat) BatchFill(offset int64, os []uint8, vps []uint64, rowCounts []int64, vecs []*vector.Vector) error {
	for i := range os {
		if vps[i] == 0 {
			continue
		}
		if err := gc.Fill(int64(vps[i]-1), offset+int64(i), rowCounts[i+int(offset)], vecs); err != nil {
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

// GetInputTypes get types of aggregate's input arguments.
func (gc *GroupConcat) GetInputTypes() []types.Type {
	return gc.ityp
}

// GetOperatorId get types of aggregate's aggregate id.
// this is used to print log in group string();
func (gc *GroupConcat) GetOperatorId() int {
	return agg.AggregateGroupConcat
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
	if nulls.Any(vec.Nsp) {
		return "", nil
	}
	switch vec.Typ.Oid {
	case types.T_bool:
		flag := vector.GetValueAt[bool](vec, int64(rowIndex))
		if flag {
			return "1", nil
		}
		return "0", nil
	case types.T_int8:
		return fmt.Sprintf("%v", vector.GetValueAt[int8](vec, int64(rowIndex))), nil
	case types.T_int16:
		return fmt.Sprintf("%v", vector.GetValueAt[int16](vec, int64(rowIndex))), nil
	case types.T_int32:
		return fmt.Sprintf("%v", vector.GetValueAt[int32](vec, int64(rowIndex))), nil
	case types.T_int64:
		return fmt.Sprintf("%v", vector.GetValueAt[int64](vec, int64(rowIndex))), nil
	case types.T_uint8:
		return fmt.Sprintf("%v", vector.GetValueAt[uint8](vec, int64(rowIndex))), nil
	case types.T_uint16:
		return fmt.Sprintf("%v", vector.GetValueAt[uint16](vec, int64(rowIndex))), nil
	case types.T_uint32:
		return fmt.Sprintf("%v", vector.GetValueAt[uint32](vec, int64(rowIndex))), nil
	case types.T_uint64:
		return fmt.Sprintf("%v", vector.GetValueAt[uint64](vec, int64(rowIndex))), nil
	case types.T_float32:
		return fmt.Sprintf("%v", vector.GetValueAt[float32](vec, int64(rowIndex))), nil
	case types.T_float64:
		return fmt.Sprintf("%v", vector.GetValueAt[float64](vec, int64(rowIndex))), nil
	case types.T_char, types.T_varchar, types.T_text, types.T_blob:
		return vec.GetString(int64(rowIndex)), nil
	case types.T_decimal64:
		val := vector.GetValueAt[types.Decimal64](vec, int64(rowIndex))
		return val.String(), nil
	case types.T_decimal128:
		val := vector.GetValueAt[types.Decimal128](vec, int64(rowIndex))
		return val.String(), nil
	case types.T_json:
		val := vec.GetBytes(int64(rowIndex))
		byteJson := types.DecodeJson(val)
		return byteJson.String(), nil
	case types.T_uuid:
		val := vector.GetValueAt[types.Uuid](vec, int64(rowIndex))
		return val.ToString(), nil
	case types.T_date:
		val := vector.GetValueAt[types.Date](vec, int64(rowIndex))
		return val.String(), nil
	case types.T_time:
		val := vector.GetValueAt[types.Time](vec, int64(rowIndex))
		return val.String(), nil
	case types.T_datetime:
		val := vector.GetValueAt[types.Datetime](vec, int64(rowIndex))
		return val.String(), nil
	default:
		return "", nil
	}
}

func hasNull(vecs []*vector.Vector, rowIdx int64) bool {
	for i := 0; i < len(vecs); i++ {
		if vecs[i].Nsp.Contains(uint64(rowIdx)) {
			return true
		}
	}
	return false
}
