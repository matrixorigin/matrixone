// Copyright 2021 Matrix Origin
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

package count

import (
	"fmt"
	"hash/crc32"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func ReturnType(_ types.Type) types.Type {
	return types.New(types.T_int64, 0, 0, 0)
}

func New(ityp, otyp types.Type) agg.Agg[*Count] {
	return &Count{
		Ityp: []types.Type{ityp},
		Otyp: otyp,
	}
}

func (r *Count) String() string {
	return fmt.Sprintf("%v", r.Vs)
}

func (r *Count) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
	}
}

func (r *Count) Dup() agg.Agg[any] {
	return &Count{
		Ityp: r.Ityp,
		Otyp: r.Otyp,
	}
}

func (r *Count) Type() types.Type {
	return r.Otyp
}

func (r *Count) InputType() []types.Type {
	return r.Ityp
}

func (r *Count) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Da = data
		r.Vs = encoding.DecodeSlice[int64](r.Da, 8)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeSlice[int64](r.Da, 8)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*8]
	return nil
}

func (r *Count) Fill(i int64, sel, z int64, vec []*vector.Vector) {
	if !nulls.Contains(vec[0].Nsp, uint64(sel)) {
		r.Vs[i] += z
	}
}

func (r *Count) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec []*vector.Vector) {
	if nulls.Any(vec[0].Nsp) {
		for i := range os {
			if !nulls.Contains(vec[0].Nsp, uint64(i)+uint64(start)) {
				r.Vs[vps[i]-1] += zs[int64(i)+start]
			}
		}
		return
	}
	for i := range os {
		r.Vs[vps[i]-1] += zs[int64(i)+start]
	}
}

func (r *Count) BulkFill(i int64, zs []int64, vec []*vector.Vector) {
	if nulls.Any(vec[0].Nsp) {
		for j, z := range zs {
			if !nulls.Contains(vec[0].Nsp, uint64(j)) {
				r.Vs[i] += z
			}
		}
	}
	for _, z := range zs {
		r.Vs[i] += z
	}
}

// r[x] += a[y]
func (r *Count) Merge(a agg.Agg[any], x, y int64) {
	ar := a.(*Count)
	r.Vs[x] += ar.Vs[y]
}

func (r *Count) BatchMerge(a agg.Agg[any], start int64, os []uint8, vps []uint64) {
	ar := a.(*Count)
	for i := range os {
		r.Vs[vps[i]-1] += ar.Vs[int64(i)+start]
	}
}

func (r *Count) Eval(zs []int64, _ *mheap.Mheap) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
	}()
	return &vector.Vector{
		Typ:  r.Otyp,
		Col:  r.Vs,
		Data: r.Da,
		Nsp:  &nulls.Nulls{},
	}
}

//for distinct count, the arguments can be many, So I make a simple way to splve it (need to optimize)
//like count(distinct a,b,c),I will parse a,b,c as string type to get stra,strb,strc. Then I will use
//Crc32 to hash them and I get uint32 of a,b,c, they are u32a,u32b,u32c. then add them all u32a+u32b+u32c
//so I get the hashvalue of multicols. But I don't know whether its effect is good.(todo: optimize it)
func NewDistinctCount(ityp, otyp types.Type) agg.Agg[*DistCount] {
	return &DistCount{
		Ityp: []types.Type{ityp},
		Otyp: otyp,
	}
}
func CRC32(input string) uint32 {
	bytes := []byte(input)
	return crc32.ChecksumIEEE(bytes)
}
func getValue(vec *vector.Vector, sel int64) uint32 {
	switch vec.Typ.Oid {
	case types.T_bool:
		return CRC32(strconv.FormatBool(vec.Col.([]bool)[sel]))
	case types.T_int8:
		return CRC32(strconv.FormatInt(int64(vec.Col.([]int8)[sel]), 10))
	case types.T_int16:
		return CRC32(strconv.FormatInt(int64(vec.Col.([]int16)[sel]), 10))
	case types.T_int32:
		return CRC32(strconv.FormatInt(int64(vec.Col.([]int32)[sel]), 10))
	case types.T_int64:
		return CRC32(strconv.FormatInt(int64(vec.Col.([]int64)[sel]), 10))
	case types.T_uint8:
		return CRC32(strconv.FormatUint(uint64(vec.Col.([]uint8)[sel]), 10))
	case types.T_uint16:
		return CRC32(strconv.FormatUint(uint64(vec.Col.([]uint16)[sel]), 10))
	case types.T_uint32:
		return CRC32(strconv.FormatUint(uint64(vec.Col.([]uint32)[sel]), 10))
	case types.T_uint64:
		return CRC32(strconv.FormatUint(uint64(vec.Col.([]uint64)[sel]), 10))
	case types.T_float32:
		return CRC32(strconv.FormatFloat(float64(vec.Col.([]float32)[sel]), byte('f'), 10, 32))
	case types.T_float64:
		return CRC32(strconv.FormatFloat(float64(vec.Col.([]float64)[sel]), byte('f'), 10, 64))
	case types.T_date:
		return CRC32(strconv.FormatInt(int64(vec.Col.([]types.Date)[sel]), 10))
	case types.T_datetime:
		return CRC32(strconv.FormatInt(int64(vec.Col.([]types.Datetime)[sel]), 10))
	case types.T_timestamp:
		return CRC32(strconv.FormatInt(int64(vec.Col.([]types.Timestamp)[sel]), 10))
	case types.T_decimal64:
		return CRC32(strconv.FormatInt(int64(vec.Col.([]types.Decimal64)[sel]), 10))
	case types.T_decimal128:
		Hi := strconv.FormatInt(int64(vec.Col.([]types.Decimal128)[sel].Hi), 10)
		Lo := strconv.FormatInt(int64(vec.Col.([]types.Decimal128)[sel].Lo), 10)
		return CRC32(Hi + Lo)
	case types.T_char, types.T_varchar, types.T_json:
		vs := vec.Col.(*types.Bytes)
		return CRC32(string(vs.Get(sel)))
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.SetLength", vec.Typ))
	}
}

func GetHashOfVecs(vecs []*vector.Vector, sel int64) (res uint64) {
	for _, vec := range vecs {
		res += uint64(getValue(vec, sel))
	}
	return
}

func (r *DistCount) String() string {
	return fmt.Sprintf("%v", r.Vs)
}

func (r *DistCount) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
	}
}

func (r *DistCount) Dup() agg.Agg[any] {
	return &DistCount{
		Ityp: r.Ityp,
		Otyp: r.Otyp,
	}
}

func (r *DistCount) Type() types.Type {
	return r.Otyp
}

func (r *DistCount) InputType() []types.Type {
	return r.Ityp
}

func (r *DistCount) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Da = data
		r.Vs = encoding.DecodeSlice[int64](r.Da, 8)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeSlice[int64](r.Da, 8)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*8]
	for i := 0; i < size; i++ {
		r.Ms = append(r.Ms, make(map[any]int64))
	}
	return nil
}

func insertIntoMap(mp map[any]int64, v any, z int64) bool {
	if _, ok := mp[v]; ok {
		return false
	}
	mp[v] = z
	return true
}

func (r *DistCount) Add(i int64, sel, z int64, vec []*vector.Vector) {
	v := GetHashOfVecs(vec, sel)
	if insertIntoMap(r.Ms[i], v, z) {
		r.Vs[i] += z
	}
}

func (r *DistCount) Fill(i int64, sel, z int64, vec []*vector.Vector) {
	if ContainsNullSel(vec, sel) {
		return
	}
	r.Add(i, sel, z, vec)
}

func ContainsNull(vec []*vector.Vector) bool {
	for _, val := range vec {
		if nulls.Any(val.Nsp) {
			return true
		}
	}
	return false
}

func ContainsNullSel(vec []*vector.Vector, sel int64) bool {
	for _, v := range vec {
		if nulls.Contains(v.Nsp, uint64(sel)) {
			return true
		}
	}
	return false
}

func (r *DistCount) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec []*vector.Vector) {
	if ContainsNull(vec) {
		for i := range os {
			if !ContainsNullSel(vec, int64(i)+start) {
				r.Add(int64(vps[i]-1), int64(i)+start, zs[int64(i)+start], vec)
			}
		}
		return
	}
	for i := range os {
		r.Add(int64(vps[i]-1), int64(i)+start, zs[int64(i)+start], vec)
	}
}

func (r *DistCount) BulkFill(i int64, zs []int64, vec []*vector.Vector) {
	if ContainsNull(vec) {
		for j, z := range zs {
			if !ContainsNullSel(vec, int64(j)) {
				r.Add(i, int64(j), int64(z), vec)
			}
		}
		return
	}
	for j, z := range zs {
		r.Add(i, int64(j), int64(z), vec)
	}
}

// r[x] += a[y]
// the old implementation in ring is strange and obviously wrong.
func (r *DistCount) Merge(a agg.Agg[any], x, y int64) {
	ar := a.(*DistCount)
	for k := range ar.Ms[y] {
		z, ok := r.Ms[x][k]
		if !ok {
			r.Vs[x] += z
		}
	}
}

func (r *DistCount) BatchMerge(a agg.Agg[any], start int64, os []uint8, vps []uint64) {
	ar := a.(*DistCount)
	for i := range os {
		r.Merge(ar, int64(vps[i]-1), int64(i)+start)
	}
}

func (r *DistCount) Eval(zs []int64, _ *mheap.Mheap) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
	}()
	return &vector.Vector{
		Typ:  r.Otyp,
		Col:  r.Vs,
		Data: r.Da,
		Nsp:  &nulls.Nulls{},
	}
}
