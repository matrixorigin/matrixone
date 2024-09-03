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

package sample

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type poolData struct {
	// validBatch stores the valid rows.
	validBatch *batch.Batch

	// invalidBatch stores the invalid rows.
	// in fact, we only store one invalid row.
	invalidBatch *batch.Batch
}

func (pd *poolData) appendValidRow(mp *mpool.MPool, bat *batch.Batch, offset int, length int) error {
	if pd.validBatch == nil {
		pd.validBatch = batch.NewWithSize(len(bat.Vecs))
		for i := range pd.validBatch.Vecs {
			pd.validBatch.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
		}
	}

	for i := range pd.validBatch.Vecs {
		if err := pd.validBatch.Vecs[i].UnionBatch(bat.Vecs[i], int64(offset), length, nil, mp); err != nil {
			return err
		}
	}
	pd.validBatch.AddRowCount(length)
	return nil
}

func (pd *poolData) appendInvalidRow(mp *mpool.MPool, bat *batch.Batch, row int) error {
	if pd.invalidBatch != nil {
		return nil
	}
	pd.invalidBatch = batch.NewWithSize(len(bat.Vecs))
	for i := range pd.invalidBatch.Vecs {
		pd.invalidBatch.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
	}

	for i := range pd.invalidBatch.Vecs {
		if err := pd.invalidBatch.Vecs[i].UnionBatch(bat.Vecs[i], int64(row), 1, nil, mp); err != nil {
			return err
		}
	}
	pd.invalidBatch.SetRowCount(1)
	return nil
}

func (pd *poolData) replaceValidRow(mp *mpool.MPool, bat *batch.Batch, row1, row2 int) (err error) {
	var right int
	for i, vec := range bat.Vecs {
		right = row2
		if vec.IsConst() {
			right = 0
		}
		if f := replaceMethods[vec.GetType().Oid]; f != nil {
			err = f(pd.validBatch.Vecs[i], vec, row1, right, mp)
		} else {
			return moerr.NewInternalErrorNoCtx("unsupported type for sample pool.")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// flush returns the result of poolData and set the source pointer to be nil.
// priority: validBatch > invalidBatch.
func (pd *poolData) flush() (bat *batch.Batch) {
	if pd.validBatch != nil {
		bat = pd.validBatch
		pd.validBatch = nil
	} else {
		bat = pd.invalidBatch
		pd.invalidBatch = nil
	}
	return bat
}

func (pd *poolData) clean(mp *mpool.MPool) {
	if pd.validBatch != nil {
		pd.validBatch.Clean(mp)
	}
	if pd.invalidBatch != nil {
		pd.invalidBatch.Clean(mp)
	}
}

type replaceFunc func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error

var replaceMethods []replaceFunc

func init() {
	replaceMethods = make([]replaceFunc, 256)
	replaceMethods[types.T_bit] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[uint64](toVec, row1, vector.GetFixedAtNoTypeCheck[uint64](fromVec, row2))
	}
	replaceMethods[types.T_int8] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[int8](toVec, row1, vector.GetFixedAtNoTypeCheck[int8](fromVec, row2))
	}
	replaceMethods[types.T_int16] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[int16](toVec, row1, vector.GetFixedAtNoTypeCheck[int16](fromVec, row2))
	}
	replaceMethods[types.T_int32] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[int32](toVec, row1, vector.GetFixedAtNoTypeCheck[int32](fromVec, row2))
	}
	replaceMethods[types.T_int64] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[int64](toVec, row1, vector.GetFixedAtNoTypeCheck[int64](fromVec, row2))
	}
	replaceMethods[types.T_uint8] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[uint8](toVec, row1, vector.GetFixedAtNoTypeCheck[uint8](fromVec, row2))
	}
	replaceMethods[types.T_uint16] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[uint16](toVec, row1, vector.GetFixedAtNoTypeCheck[uint16](fromVec, row2))
	}
	replaceMethods[types.T_uint32] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[uint32](toVec, row1, vector.GetFixedAtNoTypeCheck[uint32](fromVec, row2))
	}
	replaceMethods[types.T_uint64] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[uint64](toVec, row1, vector.GetFixedAtNoTypeCheck[uint64](fromVec, row2))
	}
	replaceMethods[types.T_float32] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[float32](toVec, row1, vector.GetFixedAtNoTypeCheck[float32](fromVec, row2))
	}
	replaceMethods[types.T_float64] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[float64](toVec, row1, vector.GetFixedAtNoTypeCheck[float64](fromVec, row2))
	}
	replaceMethods[types.T_date] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.Date](toVec, row1, vector.GetFixedAtNoTypeCheck[types.Date](fromVec, row2))
	}
	replaceMethods[types.T_datetime] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.Datetime](toVec, row1, vector.GetFixedAtNoTypeCheck[types.Datetime](fromVec, row2))
	}
	replaceMethods[types.T_timestamp] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.Timestamp](toVec, row1, vector.GetFixedAtNoTypeCheck[types.Timestamp](fromVec, row2))
	}
	replaceMethods[types.T_time] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.Time](toVec, row1, vector.GetFixedAtNoTypeCheck[types.Time](fromVec, row2))
	}
	replaceMethods[types.T_enum] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.Enum](toVec, row1, vector.GetFixedAtNoTypeCheck[types.Enum](fromVec, row2))
	}
	replaceMethods[types.T_decimal64] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.Decimal64](toVec, row1, vector.GetFixedAtNoTypeCheck[types.Decimal64](fromVec, row2))
	}
	replaceMethods[types.T_decimal128] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.Decimal128](toVec, row1, vector.GetFixedAtNoTypeCheck[types.Decimal128](fromVec, row2))
	}
	replaceMethods[types.T_TS] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.TS](toVec, row1, vector.GetFixedAtNoTypeCheck[types.TS](fromVec, row2))
	}
	replaceMethods[types.T_Rowid] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
		return vector.SetFixedAtWithTypeCheck[types.Rowid](toVec, row1, vector.GetFixedAtNoTypeCheck[types.Rowid](fromVec, row2))
	}

	for _, oid := range []types.T{types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text, types.T_datalink,
		types.T_array_float32, types.T_array_float64} {
		replaceMethods[oid] = func(toVec, fromVec *vector.Vector, row1, row2 int, mp *mpool.MPool) error {
			return vector.SetBytesAt(toVec, row1, fromVec.GetBytesAt(row2), mp)
		}
	}
}
