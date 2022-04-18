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

package compile

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/join"
)

const (
	UnitLimit = 256
)

var OneInt64s []int64

func init() {
	OneInt64s = make([]int64, UnitLimit)
	for i := range OneInt64s {
		OneInt64s[i] = 1
	}
}

func constructViews(bats []*batch.Batch, vars [][]string) {
	for i := range vars {
		constructView(bats[i], vars[i])
	}
}

func constructView(bat *batch.Batch, vars []string) {
	var rows uint64

	batch.Reorder(bat, vars)
	if len(vars) == 1 {
		constructViewWithOneVar(bat, vars[0])
		return
	}
	ht := &join.HashTable{
		StrHashMap: &hashtable.StringHashMap{},
	}
	ht.StrHashMap.Init()
	keys := make([][]byte, UnitLimit)
	values := make([]uint64, UnitLimit)
	strHashStates := make([][3]uint64, UnitLimit)
	vecs := make([]*vector.Vector, len(vars))
	{ // fill vectors
		for i := range vars {
			vecs[i] = batch.GetVector(bat, vars[i])
		}
	}
	count := int64(len(bat.Zs))
	zValues := make([]int64, UnitLimit)
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(zValues[:n], OneInt64s[:n])
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vecs[j].Col.([]int8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
						}
					}
				}
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
						}
					}
				}
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
						}
					}
				}
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
						}
					}
				}
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_date:
				vs := vecs[j].Col.([]types.Date)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_datetime:
				vs := vecs[j].Col.([]types.Datetime)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], vs.Get(i+k)...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], vs.Get(i+k)...)
						}
					}
				}
			}
		}
		for k := int64(0); k < n; k++ {
			if l := len(keys[k]); l < 16 {
				keys[k] = append(keys[k], hashtable.StrKeyPadding[l:]...)
			}
		}
		ht.StrHashMap.InsertStringBatchWithRing(zValues, strHashStates, keys[:n], values)
		{
			for k, v := range values[:n] {
				keys[k] = keys[k][:0]
				if v > rows {
					ht.Sels = append(ht.Sels, make([]int64, 0, 8))
				}
				ai := int64(v) - 1
				ht.Sels[ai] = append(ht.Sels[ai], i+int64(k))
			}
		}
	}
	bat.Ht = ht
}

func constructViewWithOneVar(bat *batch.Batch, fvar string) {
	var rows uint64

	vec := batch.GetVector(bat, fvar)
	switch vec.Typ.Oid {
	case types.T_int8:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]int8)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_uint8:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]uint8)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_int16:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]int16)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_uint16:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]uint16)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_int32:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]int32)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_uint32:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]uint32)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_int64:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]int64)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_uint64:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]uint64)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_float32:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]float32)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_date:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]types.Date)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_float64:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]float64)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_datetime:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]types.Datetime)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_char, types.T_varchar:
		ht := &join.HashTable{
			StrHashMap: &hashtable.StringHashMap{},
		}
		ht.StrHashMap.Init()
		vs := vec.Col.(*types.Bytes)
		keys := make([][]byte, UnitLimit)
		values := make([]uint64, UnitLimit)
		strHashStates := make([][3]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = append(keys[k], vs.Get(int64(i+k))...)
					}
				}
				for k := 0; k < n; k++ {
					if l := len(keys[k]); l < 16 {
						keys[k] = append(keys[k], hashtable.StrKeyPadding[l:]...)
					}
				}
				ht.StrHashMap.InsertStringBatch(strHashStates, keys[:n], values)
				for k, v := range values[:n] {
					keys[k] = keys[k][:0]
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = append(keys[k], vs.Get(int64(i+k))...)
					}
				}
				for k := 0; k < n; k++ {
					if l := len(keys[k]); l < 16 {
						keys[k] = append(keys[k], hashtable.StrKeyPadding[l:]...)
					}
				}
				ht.StrHashMap.InsertStringBatchWithRing(zValues, strHashStates, keys[:n], values)
				for k, v := range values[:n] {
					keys[k] = keys[k][:0]
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	}
}
