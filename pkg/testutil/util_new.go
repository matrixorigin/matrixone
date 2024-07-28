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
	"encoding/binary"
	"math/rand"
	"strconv"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewProcess() *process.Process {
	mp := mpool.MustNewZeroNoFixed()
	return NewProcessWithMPool("", mp)
}

func SetupAutoIncrService(sid string) {
	rt := runtime.ServiceRuntime(sid)
	if rt == nil {
		rt = runtime.DefaultRuntime()
		runtime.SetupServiceBasedRuntime(sid, rt)
	}
	rt.SetGlobalVariables(
		runtime.AutoIncrementService,
		incrservice.NewIncrService(
			"",
			incrservice.NewMemStore(),
			incrservice.Config{}))
}

func NewProcessWithMPool(sid string, mp *mpool.MPool) *process.Process {
	SetupAutoIncrService(sid)
	proc := process.New(
		context.Background(),
		mp,
		nil, // no txn client can be set
		nil, // no txn operator can be set
		NewFS(),
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	proc.Base.Lim.Size = 1 << 20
	proc.Base.Lim.BatchRows = 1 << 20
	proc.Base.Lim.BatchSize = 1 << 20
	proc.Base.Lim.ReaderSize = 1 << 20
	proc.Base.SessionInfo.TimeZone = time.Local
	return proc
}

var NewProc = NewProcess

func NewFS() *fileservice.FileServices {
	local, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		panic(err)
	}
	s3, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		panic(err)
	}
	etl, err := fileservice.NewMemoryFS(defines.ETLFileServiceName, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		panic(err)
	}
	fs, err := fileservice.NewFileServices(
		"",
		local,
		s3,
		etl,
	)
	if err != nil {
		panic(err)
	}
	return fs
}

func NewSharedFS() fileservice.FileService {
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		panic(err)
	}
	return fs
}

func NewETLFS() fileservice.FileService {
	fs, err := fileservice.NewMemoryFS(defines.ETLFileServiceName, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		panic(err)
	}
	return fs
}

func NewBatch(ts []types.Type, random bool, n int, m *mpool.MPool) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.SetRowCount(n)
	for i := range bat.Vecs {
		bat.Vecs[i] = NewVector(n, ts[i], m, random, nil)
		// XXX do we need to init nulls here?   can we be lazy?
		bat.Vecs[i].GetNulls().InitWithSize(n)
	}
	return bat
}

func NewBatchWithNulls(ts []types.Type, random bool, n int, m *mpool.MPool) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.SetRowCount(n)
	for i := range bat.Vecs {
		bat.Vecs[i] = NewVector(n, ts[i], m, random, nil)
		bat.Vecs[i].GetNulls().InitWithSize(n)
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
		bat.SetRowCount(vs[0].Length())
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
	case types.T_bit:
		if vs, ok := Values.([]uint64); ok {
			return NewUInt64Vector(n, typ, m, random, vs)
		}
		return NewUInt64Vector(n, typ, m, random, nil)
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
	case types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		if vs, ok := Values.([]string); ok {
			return NewStringVector(n, typ, m, random, vs)
		}
		return NewStringVector(n, typ, m, random, nil)
	case types.T_array_float32:
		if vs, ok := Values.([][]float32); ok {
			return NewArrayVector[float32](n, typ, m, random, vs)
		}
		return NewArrayVector[float32](n, typ, m, random, nil)
	case types.T_array_float64:
		if vs, ok := Values.([][]float64); ok {
			return NewArrayVector[float64](n, typ, m, random, vs)
		}
		return NewArrayVector[float64](n, typ, m, random, nil)
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
	case types.T_Blockid:
		if vs, ok := Values.([]types.Blockid); ok {
			return NewBlockidVector(n, typ, m, random, vs)
		}
		return NewBlockidVector(n, typ, m, random, nil)
	case types.T_enum:
		if vs, ok := Values.([]uint16); ok {
			return NewUInt16Vector(n, typ, m, random, vs)
		}
		return NewUInt16Vector(n, typ, m, random, nil)
	default:
		panic(moerr.NewInternalErrorNoCtx("unsupport vector's type '%v", typ))
	}
}

func NewTsVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []types.TS) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		var t timestamp.Timestamp

		t.PhysicalTime = int64(i)
		if err := vector.AppendFixed(vec, types.TimestampToTS(t), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewRowidVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []types.Rowid) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		var rowId types.Rowid
		binary.LittleEndian.PutUint64(
			unsafe.Slice(&rowId[types.RowidSize/2], 8),
			uint64(i),
		)
		if err := vector.AppendFixed(vec, rowId, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewBlockidVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []types.Blockid) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		var blockId types.Blockid
		binary.LittleEndian.PutUint64(
			unsafe.Slice(&blockId[types.BlockidSize/2], 8),
			uint64(i),
		)
		if err := vector.AppendFixed(vec, blockId, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewJsonVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
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
			if err := vector.AppendFixed(vec, jbytes, false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		json, _ := types.ParseStringToByteJson(`{"a":1}`)
		jbytes, _ := json.Marshal()
		if err := vector.AppendBytes(vec, jbytes, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewBoolVector(n int, typ types.Type, m *mpool.MPool, _ bool, vs []bool) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		if err := vector.AppendFixed(vec, bool(i%2 == 0), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt8Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int8) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, int8(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt16Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int16) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, int16(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int32) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, int32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, int64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt8Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint8) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, uint8(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt16Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint16) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, uint16(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint32) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, uint32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, uint64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloat32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []float32) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, float32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloat64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []float64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendFixed(vec, float64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDecimal64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal64) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		d := types.Decimal64(v)
		if err := vector.AppendFixed(vec, d, false, m); err != nil {

			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDecimal128Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal128) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, vs[i], false, m); err != nil {
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
		d := types.Decimal128{B0_63: uint64(v), B64_127: 0}
		if err := vector.AppendFixed(vec, d, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDateVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseDateCast(vs[i])
			if err != nil {
				return nil
			}
			if err := vector.AppendFixed(vec, d, false, m); err != nil {
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
		if err := vector.AppendFixed(vec, types.Date(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewTimeVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseTime(vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vector.AppendFixed(vec, d, false, m); err != nil {
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
		if err := vector.AppendFixed(vec, types.Time(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewEnumVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint16) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendFixed(vec, types.Enum(vs[i]), false, m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 1; i <= n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.AppendFixed(vec, types.Enum(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDatetimeVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseDatetime(vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vector.AppendFixed(vec, d, false, m); err != nil {
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
		if err := vector.AppendFixed(vec, types.Datetime(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewTimestampVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			d, err := types.ParseTimestamp(time.Local, vs[i], 6)
			if err != nil {
				return nil
			}
			if err := vector.AppendFixed(vec, d, false, m); err != nil {
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
		if err := vector.AppendFixed(vec, types.Timestamp(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewStringVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendBytes(vec, []byte(vs[i]), false, m); err != nil {
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
		if err := vector.AppendBytes(vec, []byte(strconv.Itoa(v)), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewArrayVector[T types.RealNumbers](n int, typ types.Type, m *mpool.MPool, random bool, vs [][]T) *vector.Vector {
	vec := vector.NewVec(typ)
	if vs != nil {
		for i := range vs {
			if err := vector.AppendArray[T](vec, vs[i], false, m); err != nil {
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
		if err := vector.AppendArray[T](vec, []T{T(v), T(v + 1)}, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewRegMsg(bat *batch.Batch) *process.RegisterMessage {
	return &process.RegisterMessage{
		Batch: bat,
		Err:   nil,
	}
}
