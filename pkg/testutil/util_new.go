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

package testutil

import (
	"context"
	"math/rand"
	"strconv"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewProcess() *process.Process {
	mp := mpool.MustNewZero()
	return NewProcessWithMPool(mp)
}

func NewProcessWithMPool(mp *mpool.MPool) *process.Process {
	proc := process.New(
		context.Background(),
		mp,
		nil, // no txn client can be set
		nil, // no txn operator can be set
		NewFS(),
		nil, // no get cluster details func can be set
	)
	proc.Lim.Size = 1 << 20
	proc.Lim.BatchRows = 1 << 20
	proc.Lim.BatchSize = 1 << 20
	proc.Lim.ReaderSize = 1 << 20
	proc.SessionInfo.TimeZone = time.Local
	return proc
}

var NewProc = NewProcess

func NewFS() *fileservice.FileServices {
	local, err := fileservice.NewMemoryFS("local")
	if err != nil {
		panic(err)
	}
	s3, err := fileservice.NewMemoryFS("s3")
	if err != nil {
		panic(err)
	}
	etl, err := fileservice.NewMemoryFS("etl")
	if err != nil {
		panic(err)
	}
	fs, err := fileservice.NewFileServices(
		"local",
		local,
		s3,
		etl,
	)
	if err != nil {
		panic(err)
	}
	return fs
}

func NewBatch(ts []types.Type, random bool, n int, m *mpool.MPool) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.InitZsOne(n)
	for i := range bat.Vecs {
		bat.Vecs[i] = NewVector(n, ts[i], m, random, nil)
		nulls.New(bat.Vecs[i].Nsp, n)
	}
	return bat
}

func NewBatchWithNulls(ts []types.Type, random bool, n int, m *mpool.MPool) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.InitZsOne(n)
	for i := range bat.Vecs {
		bat.Vecs[i] = NewVector(n, ts[i], m, random, nil)
		nulls.New(bat.Vecs[i].Nsp, n)
		nsp := bat.Vecs[i].GetNulls()
		for j := 0; j < n; j++ {
			if j%2 == 0 {
				nsp.Set(uint64(j))
			}
		}
	}
	return bat
}

func NewBatchWithVectors(vs []*vector.Vector, zs []int64) *batch.Batch {
	bat := batch.NewWithSize(len(vs))
	if len(vs) > 0 {
		l := vector.Length(vs[0])
		if zs == nil {
			zs = MakeBatchZs(l, false)
		}
		bat.Zs = append([]int64{}, zs...)
		bat.Vecs = vs
	}
	return bat
}

func NewVector(n int, typ types.Type, m *mpool.MPool, random bool, Values interface{}) *vector.Vector {
	switch typ.Oid {
	case types.T_bool:
		if vs, ok := Values.([]bool); ok {
			return NewBoolVector(n, typ, m, random, vs)
		}
		return NewBoolVector(n, typ, m, random, nil)
	case types.T_int8:
		if vs, ok := Values.([]int8); ok {
			return NewInt8Vector(n, typ, m, random, vs)
		}
		return NewInt8Vector(n, typ, m, random, nil)
	case types.T_int16:
		if vs, ok := Values.([]int16); ok {
			return NewInt16Vector(n, typ, m, random, vs)
		}
		return NewInt16Vector(n, typ, m, random, nil)
	case types.T_int32:
		if vs, ok := Values.([]int32); ok {
			return NewInt32Vector(n, typ, m, random, vs)
		}
		return NewInt32Vector(n, typ, m, random, nil)
	case types.T_int64:
		if vs, ok := Values.([]int64); ok {
			return NewInt64Vector(n, typ, m, random, vs)
		}
		return NewInt64Vector(n, typ, m, random, nil)
	case types.T_uint8:
		if vs, ok := Values.([]uint8); ok {
			return NewUInt8Vector(n, typ, m, random, vs)
		}
		return NewUInt8Vector(n, typ, m, random, nil)
	case types.T_uint16:
		if vs, ok := Values.([]uint16); ok {
			return NewUInt16Vector(n, typ, m, random, vs)
		}
		return NewUInt16Vector(n, typ, m, random, nil)
	case types.T_uint32:
		if vs, ok := Values.([]uint32); ok {
			return NewUInt32Vector(n, typ, m, random, vs)
		}
		return NewUInt32Vector(n, typ, m, random, nil)
	case types.T_uint64:
		if vs, ok := Values.([]uint64); ok {
			return NewUInt64Vector(n, typ, m, random, vs)
		}
		return NewUInt64Vector(n, typ, m, random, nil)
	case types.T_float32:
		if vs, ok := Values.([]float32); ok {
			return NewFloat32Vector(n, typ, m, random, vs)
		}
		return NewFloat32Vector(n, typ, m, random, nil)
	case types.T_float64:
		if vs, ok := Values.([]float64); ok {
			return NewFloat64Vector(n, typ, m, random, vs)
		}
		return NewFloat64Vector(n, typ, m, random, nil)
	case types.T_date:
		if vs, ok := Values.([]string); ok {
			return NewDateVector(n, typ, m, random, vs)
		}
		return NewDateVector(n, typ, m, random, nil)
	case types.T_time:
		if vs, ok := Values.([]string); ok {
			return NewTimeVector(n, typ, m, random, vs)
		}
		return NewTimeVector(n, typ, m, random, nil)
	case types.T_datetime:
		if vs, ok := Values.([]string); ok {
			return NewDatetimeVector(n, typ, m, random, vs)
		}
		return NewDatetimeVector(n, typ, m, random, nil)
	case types.T_timestamp:
		if vs, ok := Values.([]string); ok {
			return NewTimestampVector(n, typ, m, random, vs)
		}
		return NewTimestampVector(n, typ, m, random, nil)
	case types.T_decimal64:
		if vs, ok := Values.([]types.Decimal64); ok {
			return NewDecimal64Vector(n, typ, m, random, vs)
		}
		return NewDecimal64Vector(n, typ, m, random, nil)
	case types.T_decimal128:
		if vs, ok := Values.([]types.Decimal128); ok {
			return NewDecimal128Vector(n, typ, m, random, vs)
		}
		return NewDecimal128Vector(n, typ, m, random, nil)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		if vs, ok := Values.([]string); ok {
			return NewStringVector(n, typ, m, random, vs)
		}
		return NewStringVector(n, typ, m, random, nil)
	case types.T_json:
		if vs, ok := Values.([]string); ok {
			return NewJsonVector(n, typ, m, random, vs)
		}
		return NewJsonVector(n, typ, m, random, nil)
	case types.T_TS:
		if vs, ok := Values.([]types.TS); ok {
			return NewTsVector(n, typ, m, random, vs)
		}
		return NewTsVector(n, typ, m, random, nil)
	case types.T_Rowid:
		if vs, ok := Values.([]types.Rowid); ok {
			return NewRowidVector(n, typ, m, random, vs)
		}
		return NewRowidVector(n, typ, m, random, nil)
	default:
		panic(moerr.NewInternalErrorNoCtx("unsupport vector's type '%v", typ))
	}
}

func NewTsVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []types.TS) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		var t timestamp.Timestamp

		t.PhysicalTime = int64(i)
		if err := vec.Append(types.TimestampToTS(t), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewRowidVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []types.Rowid) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		var rowId [2]int64

		rowId[1] = int64(i)
		if err := vec.Append(*(*types.Rowid)(unsafe.Pointer(&rowId[0])), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewJsonVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []string) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for _, v := range vs {
			json, err := types.ParseStringToByteJson(v)
			if err != nil {
				vec.Free(m)
				return nil
			}
			jbytes, err := json.Marshal()
			if err != nil {
				vec.Free(m)
				return nil
			}
			if err := vec.Append(jbytes, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		json, _ := types.ParseStringToByteJson(`{"a":1}`)
		jbytes, _ := json.Marshal()
		if err := vec.Append(jbytes, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewBoolVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []bool) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		if err := vec.Append(bool(i%2 == 0), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt8Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int8) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int8(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt16Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int16) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int16(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int32) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int64) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt8Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint8) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint8(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt16Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint16) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint16(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint32) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint64) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloat32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []float32) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := float32(i)
		if random {
			v = rand.Float32()
		}
		if err := vec.Append(float32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloat64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []float64) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := float64(i)
		if random {
			v = rand.Float64()
		}
		if err := vec.Append(float64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDecimal64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal64) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		d, _ := types.InitDecimal64(int64(v), 64, 0)
		if err := vec.Append(d, false, m); err != nil {

			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDecimal128Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal128) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		d, _ := types.InitDecimal128(int64(v), 64, 0)
		if err := vec.Append(d, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDateVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseDateCast(vs[i])
			if err != nil {
				return nil
			}
			if err := vec.Append(d, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Date(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewTimeVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseTime(vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vec.Append(d, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Time(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDatetimeVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseDatetime(vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vec.Append(d, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Datetime(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewTimestampVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseTimestamp(time.Local, vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vec.Append(d, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Timestamp(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewStringVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append([]byte(vs[i]), false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append([]byte(strconv.Itoa(v)), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}
