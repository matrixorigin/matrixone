// Copyright 2022 Matrix Origin
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

package memorystorage

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type BatchIter func() (tuple []Nullable)

func NewBatchIter(b *batch.Batch) BatchIter {
	i := 0
	iter := func() (tuple []Nullable) {
		for {
			if i >= b.Vecs[0].Length() {
				return
			}
			if i < len(b.Zs) && b.Zs[i] == 0 {
				i++
				continue
			}
			break
		}
		for _, vec := range b.Vecs {
			value := vectorAt(vec, i)
			tuple = append(tuple, value)
		}
		i++
		return
	}
	return iter
}

func vectorAt(vec *vector.Vector, i int) (value Nullable) {
	if vec.IsConst() {
		i = 0
	}
	switch vec.Typ.Oid {

	case types.T_bool:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  false,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]bool)[i],
		}
		return

	case types.T_int8:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  int8(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]int8)[i],
		}
		return

	case types.T_int16:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  int16(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]int16)[i],
		}
		return

	case types.T_int32:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  int32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]int32)[i],
		}
		return

	case types.T_int64:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  int64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]int64)[i],
		}
		return

	case types.T_uint8:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint8(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]uint8)[i],
		}
		return

	case types.T_uint16:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint16(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]uint16)[i],
		}
		return

	case types.T_uint32:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]uint32)[i],
		}
		return

	case types.T_uint64:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  uint64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]uint64)[i],
		}
		return

	case types.T_float32:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  float32(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]float32)[i],
		}
		return

	case types.T_float64:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  float64(0),
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]float64)[i],
		}
		return

	case types.T_tuple:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  []any{},
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([][]any)[i],
		}
		return

	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		if vec.IsScalarNull() {
			value = Nullable{
				IsNull: true,
				Value:  []byte{},
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.GetBytes(int64(i)),
		}
		return

	case types.T_date:
		if vec.IsScalarNull() {
			var zero types.Date
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]types.Date)[i],
		}
		return

	case types.T_datetime:
		if vec.IsScalarNull() {
			var zero types.Datetime
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]types.Datetime)[i],
		}
		return

	case types.T_timestamp:
		if vec.IsScalarNull() {
			var zero types.Timestamp
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]types.Timestamp)[i],
		}
		return

	case types.T_decimal64:
		if vec.IsScalarNull() {
			var zero types.Decimal64
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]types.Decimal64)[i],
		}
		return

	case types.T_decimal128:
		if vec.IsScalarNull() {
			var zero types.Decimal128
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]types.Decimal128)[i],
		}
		return

	case types.T_Rowid:
		if vec.IsScalarNull() {
			var zero types.Rowid
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]types.Rowid)[i],
		}
		return

	case types.T_uuid:
		if vec.IsScalarNull() {
			var zero types.Uuid
			value = Nullable{
				IsNull: true,
				Value:  zero,
			}
			return
		}
		value = Nullable{
			IsNull: vec.GetNulls().Contains(uint64(i)),
			Value:  vec.Col.([]types.Uuid)[i],
		}
		return

	}

	panic(fmt.Sprintf("unknown column type: %v", vec.Typ))
}

func appendNamedRow(
	tx *Transaction,
	handler *MemHandler,
	offset int,
	bat *batch.Batch,
	row NamedRow,
) error {
	row.SetHandler(handler)
	for i := offset; i < len(bat.Attrs); i++ {
		name := bat.Attrs[i]
		value, err := row.AttrByName(tx, name)
		if err != nil {
			return err
		}
		value.AppendVector(bat.Vecs[i], handler.mheap)
	}
	return nil
}
