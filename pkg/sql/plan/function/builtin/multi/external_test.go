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

package multi

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorize/external"
	"github.com/stretchr/testify/require"
)

var (
	line = []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "2020-09-07",
		"2020-09-07 00:00:00", "16", "17", "2020-09-07 00:00:00"}
	atrrs = []string{"col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10",
		"col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18"}
	cols = []*plan.ColDef{
		{
			Typ: &plan.Type{
				Id: int32(types.T_bool),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_int8),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_int16),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_int32),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_int64),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_uint8),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_uint16),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_uint32),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_uint64),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_float32),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_float64),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_varchar),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_json),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_date),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_datetime),
			},
		},
		{
			Typ: &plan.Type{
				Id:    int32(types.T_decimal64),
				Width: 15,
				Scale: 0,
			},
		},
		{
			Typ: &plan.Type{
				Id:    int32(types.T_decimal128),
				Width: 17,
				Scale: 0,
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_timestamp),
			},
		},
	}
)

func makeOriginBatch(attrs []string, bSize int, mp *mpool.MPool) *batch.Batch {
	batchData := batch.New(true, attrs)
	batchSize := bSize
	//alloc space for vector
	for i := 0; i < len(attrs); i++ {
		typ := types.New(types.T_varchar, 0, 0, 0)
		vec := vector.NewOriginal(typ)
		vector.PreAlloc(vec, batchSize, batchSize, mp)
		batchData.Vecs[i] = vec
	}
	return batchData
}
func makeBatch(attrs []string, bSize int, cols []*plan.ColDef, mp *mpool.MPool) *batch.Batch {
	batchData := batch.New(true, attrs)
	batchSize := bSize
	//alloc space for vector
	for i := 0; i < len(attrs); i++ {
		typ := types.New(types.T(cols[i].Typ.Id), cols[i].Typ.Width, cols[i].Typ.Scale, cols[i].Typ.Precision)
		vec := vector.NewOriginal(typ)
		vector.PreAlloc(vec, batchSize, batchSize, mp)
		batchData.Vecs[i] = vec
	}
	return batchData
}

func TestExternal(t *testing.T) {
	var err error
	mp := testutil.TestUtilMp
	bat := makeBatch(atrrs, 1, cols, mp)
	originBat := makeOriginBatch(atrrs, 1, mp)
	for i := 0; i < len(line); i++ {
		err = vector.SetStringAt(originBat.Vecs[i], 0, line[i], mp)
		require.Nil(t, err)
	}
	for i := range cols {
		id := types.T(cols[i].Typ.Id)
		nullList := []string{""}
		nullVec := vector.NewWithStrings(types.Type{Oid: types.T_varchar}, nullList, nil, proc.Mp())
		vectors := []*vector.Vector{
			originBat.GetVector(int32(i)),
			bat.GetVector(int32(i)),
			nullVec,
		}
		switch id {
		case types.T_bool:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseBool)
		case types.T_int8:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseInt8)
		case types.T_int16:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseInt16)
		case types.T_int32:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseInt32)
		case types.T_int64:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseInt64)
		case types.T_uint8:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseUint8)
		case types.T_uint16:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseUint16)
		case types.T_uint32:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseUint32)
		case types.T_uint64:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseUint64)
		case types.T_float32:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseFloat32)
		case types.T_float64:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseFloat64)
		case types.T_date:
			bat.Vecs[i], err = ParseNumber(vectors, proc, external.ParseDate)
		case types.T_datetime:
			bat.Vecs[i], err = ParseTime(vectors, proc, external.ParseDateTime)
		case types.T_timestamp:
			bat.Vecs[i], err = ParseTime(vectors, proc, external.ParseTimeStamp)
		case types.T_decimal64:
			bat.Vecs[i], err = ParseDecimal(vectors, proc, external.ParseDecimal64)
		case types.T_decimal128:
			bat.Vecs[i], err = ParseDecimal(vectors, proc, external.ParseDecimal128)
		case types.T_char, types.T_varchar, types.T_blob:
			bat.Vecs[i], err = ParseString(vectors, proc)
		case types.T_json:
			bat.Vecs[i], err = ParseJson(vectors, proc)

		default:
			err = moerr.NewNotSupported("the value type %d is not support now", cols[i].Typ.Id)
		}
		require.Nil(t, err)
	}
}
