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
	"fmt"
	"math/rand"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewBatch(ts []types.Type, random bool, n int, m *mheap.Mheap) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.InitZsOne(n)
	for i := range bat.Vecs {
		bat.Vecs[i] = NewVector(n, ts[i], m, random)
	}
	return bat
}

func NewVector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	switch typ.Oid {
	case types.T_bool:
		return NewBoolVector(n, typ, m, random)
	case types.T_int8:
		return NewInt8Vector(n, typ, m, random)
	case types.T_int16:
		return NewInt16Vector(n, typ, m, random)
	case types.T_int32:
		return NewInt32Vector(n, typ, m, random)
	case types.T_int64:
		return NewInt64Vector(n, typ, m, random)
	case types.T_uint8:
		return NewUInt8Vector(n, typ, m, random)
	case types.T_uint16:
		return NewUInt16Vector(n, typ, m, random)
	case types.T_uint32:
		return NewUInt32Vector(n, typ, m, random)
	case types.T_uint64:
		return NewUInt64Vector(n, typ, m, random)
	case types.T_float32:
		return NewFloat32Vector(n, typ, m, random)
	case types.T_float64:
		return NewFloat64Vector(n, typ, m, random)
	case types.T_date:
		return NewDateVector(n, typ, m, random)
	case types.T_datetime:
		return NewDatetimeVector(n, typ, m, random)
	case types.T_timestamp:
		return NewTimestampVector(n, typ, m, random)
	case types.T_decimal64:
		return NewDecimal64Vector(n, typ, m, random)
	case types.T_decimal128:
		return NewDecimal128Vector(n, typ, m, random)
	case types.T_char, types.T_varchar:
		return NewStringVector(n, typ, m, random)
	default:
		panic(fmt.Errorf("unsupport vector's type '%v", typ))
	}
}

func NewBoolVector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		if err := vec.Append(bool(i%2 == 0), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt8Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int8(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt16Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int16(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt32Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int32(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewInt64Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int64(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt8Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint8(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt16Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint16(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt32Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint32(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewUInt64Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint64(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloat32Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := float32(i)
		if random {
			v = rand.Float32()
		}
		if err := vec.Append(float32(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloat64Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := float64(i)
		if random {
			v = rand.Float64()
		}
		if err := vec.Append(float64(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDecimal64Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Decimal64(v), m); err != nil {

			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDecimal128Vector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Decimal128{Lo: int64(v), Hi: int64(v)}, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDateVector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Date(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDatetimeVector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Datetime(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewTimestampVector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Timestamp(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewStringVector(n int, typ types.Type, m *mheap.Mheap, random bool) *vector.Vector {
	vec := vector.New(typ)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append([]byte(strconv.Itoa(v)), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}
