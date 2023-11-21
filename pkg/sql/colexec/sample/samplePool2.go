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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type poolData struct {
	// validBatch stores the valid rows.
	validBatch *batch.Batch

	// invalidBatch stores the invalid rows.
	// in fact, we only store one invalid row.
	invalidBatch *batch.Batch
}

// maybe helpful for code performance sometimes.
func (pd *poolData) needAppendInvalid() bool {
	return pd.invalidBatch == nil
}

func (pd *poolData) appendValidRow(proc *process.Process, mp *mpool.MPool, bat *batch.Batch, offset int, length int) error {
	if pd.validBatch == nil {
		pd.validBatch = batch.NewWithSize(len(bat.Vecs))
		for i := range pd.validBatch.Vecs {
			pd.validBatch.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
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

func (pd *poolData) appendInvalidRow(proc *process.Process, mp *mpool.MPool, bat *batch.Batch, row int) error {
	if pd.invalidBatch != nil {
		return nil
	}
	pd.invalidBatch = batch.NewWithSize(len(bat.Vecs))
	for i := range pd.invalidBatch.Vecs {
		pd.invalidBatch.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
	}

	for i := range pd.invalidBatch.Vecs {
		if err := pd.invalidBatch.Vecs[i].UnionBatch(bat.Vecs[i], int64(row), 1, nil, mp); err != nil {
			return err
		}
	}
	pd.invalidBatch.SetRowCount(1)
	return nil
}

func (pd *poolData) replaceValidRow(mp *mpool.MPool, bat *batch.Batch, row1, row2 int) error {
	return batRowReplace(mp, pd.validBatch, bat, row1, row2)
}

// batRowReplace replaces the row1 of toBatch with the bat's row2.
// TODO: need an optimized function to do the row replace work.
func batRowReplace(mp *mpool.MPool, toBatch *batch.Batch, bat *batch.Batch, row1, row2 int) (err error) {
	var right int
	for i, vec := range bat.Vecs {
		right = row2
		if vec.IsConst() {
			right = 0
		}
		switch vec.GetType().Oid {
		case types.T_int8:
			err = vector.SetFixedAt[int8](toBatch.Vecs[i], row1, vector.GetFixedAt[int8](vec, right))
		case types.T_int16:
			err = vector.SetFixedAt[int16](toBatch.Vecs[i], row1, vector.GetFixedAt[int16](vec, right))
		case types.T_int32:
			err = vector.SetFixedAt[int32](toBatch.Vecs[i], row1, vector.GetFixedAt[int32](vec, right))
		case types.T_int64:
			err = vector.SetFixedAt[int64](toBatch.Vecs[i], row1, vector.GetFixedAt[int64](vec, right))
		case types.T_uint8:
			err = vector.SetFixedAt[uint8](toBatch.Vecs[i], row1, vector.GetFixedAt[uint8](vec, right))
		case types.T_uint16:
			err = vector.SetFixedAt[uint16](toBatch.Vecs[i], row1, vector.GetFixedAt[uint16](vec, right))
		case types.T_uint32:
			err = vector.SetFixedAt[uint32](toBatch.Vecs[i], row1, vector.GetFixedAt[uint32](vec, right))
		case types.T_uint64:
			err = vector.SetFixedAt[uint64](toBatch.Vecs[i], row1, vector.GetFixedAt[uint64](vec, right))
		case types.T_float32:
			err = vector.SetFixedAt[float32](toBatch.Vecs[i], row1, vector.GetFixedAt[float32](vec, right))
		case types.T_float64:
			err = vector.SetFixedAt[float64](toBatch.Vecs[i], row1, vector.GetFixedAt[float64](vec, right))
		case types.T_date:
			err = vector.SetFixedAt[types.Date](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Date](vec, right))
		case types.T_datetime:
			err = vector.SetFixedAt[types.Datetime](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Datetime](vec, right))
		case types.T_timestamp:
			err = vector.SetFixedAt[types.Timestamp](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Timestamp](vec, right))
		case types.T_time:
			err = vector.SetFixedAt[types.Time](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Time](vec, right))
		case types.T_enum:
			err = vector.SetFixedAt[types.Enum](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Enum](vec, right))
		case types.T_decimal64:
			err = vector.SetFixedAt[types.Decimal64](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Decimal64](vec, right))
		case types.T_decimal128:
			err = vector.SetFixedAt[types.Decimal128](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Decimal128](vec, right))
		case types.T_TS:
			err = vector.SetFixedAt[types.TS](toBatch.Vecs[i], row1, vector.GetFixedAt[types.TS](vec, right))
		case types.T_Rowid:
			err = vector.SetFixedAt[types.Rowid](toBatch.Vecs[i], row1, vector.GetFixedAt[types.Rowid](vec, right))
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
			types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64:
			err = vector.SetBytesAt(toBatch.Vecs[i], row1, vec.GetBytesAt(right), mp)
		default:
			err = moerr.NewInternalErrorNoCtx("unsupported type for sample pool.")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// output1 returns the result of poolData and set the source pointer to be nil.
// priority: validBatch > invalidBatch.
func (pd *poolData) output() (bat *batch.Batch) {
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
