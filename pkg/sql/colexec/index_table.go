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

package colexec

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func BuildRowidBatch(ctx context.Context, rel engine.Relation, b *batch.Batch, proc *process.Process) (*batch.Batch, error) {
	// the b is delete pkey batch, readBat is [row id, pkey] index table, new batch is row id batch
	// I will read the index table, compare pkey value with the delete batch
	// when two values are equal, I will put the row id to the new batch
	v := b.Vecs[0]
	var rds []engine.Reader
	ret, err := rel.Ranges(ctx, nil)
	if err != nil {
		return nil, err
	}
	rds, _ = rel.NewReader(ctx, 1, nil, ret)

	readBat, err := rds[0].Read([]string{catalog.Row_ID, b.Attrs[0]}, nil, proc.Mp())
	if err != nil {
		return nil, err
	}
	r := vector.MustTCols[types.Rowid](readBat.Vecs[0])
	newVec := vector.New(readBat.Vecs[0].Typ)
	switch v.Typ.Oid {
	case types.T_bool:
		s := vector.MustTCols[bool](v)
		ns := vector.MustTCols[bool](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_int8:
		s := vector.MustTCols[int8](v)
		ns := vector.MustTCols[int8](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_int16:
		s := vector.MustTCols[int16](v)
		ns := vector.MustTCols[int16](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_int32:
		s := vector.MustTCols[int32](v)
		ns := vector.MustTCols[int32](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_int64:
		s := vector.MustTCols[int64](v)
		ns := vector.MustTCols[int64](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_uint8:
		s := vector.MustTCols[uint8](v)
		ns := vector.MustTCols[uint8](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_uint16:
		s := vector.MustTCols[uint16](v)
		ns := vector.MustTCols[uint16](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_uint32:
		s := vector.MustTCols[uint32](v)
		ns := vector.MustTCols[uint32](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_uint64:
		s := vector.MustTCols[uint64](v)
		ns := vector.MustTCols[uint64](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_float32:
		s := vector.MustTCols[float32](v)
		ns := vector.MustTCols[float32](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_float64:
		s := vector.MustTCols[float64](v)
		ns := vector.MustTCols[float64](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_date:
		s := vector.MustTCols[types.Date](v)
		ns := vector.MustTCols[types.Date](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_datetime:
		s := vector.MustTCols[types.Datetime](v)
		ns := vector.MustTCols[types.Datetime](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_timestamp:
		s := vector.MustTCols[types.Timestamp](v)
		ns := vector.MustTCols[types.Timestamp](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_decimal64:
		s := vector.MustTCols[types.Decimal64](v)
		ns := vector.MustTCols[types.Decimal64](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_decimal128:
		s := vector.MustTCols[types.Decimal128](v)
		ns := vector.MustTCols[types.Decimal128](readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	case types.T_json, types.T_char, types.T_varchar, types.T_blob:
		s := vector.GetStrVectorValues(v)
		ns := vector.GetStrVectorValues(readBat.Vecs[1])
		for _, b := range s {
			for i, nb := range ns {
				if b == nb {
					newVec.Append(r[i], false, proc.Mp())
					break
				}
			}
		}
	}

	b.Clean(proc.Mp())
	newBatch := &batch.Batch{
		Cnt:   1,
		Vecs:  []*vector.Vector{newVec},
		Attrs: []string{catalog.Row_ID},
	}
	newBatch.SetZs(newBatch.GetVector(0).Length(), proc.Mp())
	return newBatch, nil
}
