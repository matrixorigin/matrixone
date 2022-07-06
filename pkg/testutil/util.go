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
	bat := batch.New(len(ts))
	bat.InitZs(n)
	for i := range bat.Vecs {
		switch ts[i].Oid {
		case types.T_int32:
			bat.Vecs[i] = NewIntVector(n, m, random)
		case types.T_int64:
			bat.Vecs[i] = NewLongVector(n, m, random)
		case types.T_float32:
			bat.Vecs[i] = NewFloatVector(n, m, random)
		case types.T_float64:
			bat.Vecs[i] = NewDoubleVector(n, m, random)
		case types.T_bool:
			bat.Vecs[i] = NewBoolVector(n, m, random)
		case types.T_char:
			bat.Vecs[i] = NewStringVector(n, m, random)
		default:
			panic(fmt.Errorf("unsupport type '%v", ts[i]))
		}
	}
	return bat
}

func NewBoolVector(n int, m *mheap.Mheap, random bool) *vector.Vector[types.Bool] {
	vec := vector.New[types.Bool](types.New(types.T_bool, 0, 0, 0))
	vec.NewNulls(n)
	for i := 0; i < n; i++ {
		if err := vec.Append(types.Bool(i%2 == 0), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewIntVector(n int, m *mheap.Mheap, random bool) *vector.Vector[types.Int32] {
	vec := vector.New[types.Int32](types.New(types.T_int32, 0, 0, 0))
	vec.NewNulls(n)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Int32(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewLongVector(n int, m *mheap.Mheap, random bool) *vector.Vector[types.Int64] {
	vec := vector.New[types.Int64](types.New(types.T_int64, 0, 0, 0))
	vec.NewNulls(n)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.Int64(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewFloatVector(n int, m *mheap.Mheap, random bool) *vector.Vector[types.Float32] {
	vec := vector.New[types.Float32](types.New(types.T_float32, 0, 0, 0))
	vec.NewNulls(n)
	for i := 0; i < n; i++ {
		v := float32(i)
		if random {
			v = rand.Float32()
		}
		if err := vec.Append(types.Float32(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewDoubleVector(n int, m *mheap.Mheap, random bool) *vector.Vector[types.Float64] {
	vec := vector.New[types.Float64](types.New(types.T_float64, 0, 0, 0))
	vec.NewNulls(n)
	for i := 0; i < n; i++ {
		v := float64(i)
		if random {
			v = rand.Float64()
		}
		if err := vec.Append(types.Float64(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func NewStringVector(n int, m *mheap.Mheap, random bool) *vector.Vector[types.String] {
	vec := vector.New[types.String](types.New(types.T_char, 0, 0, 0))
	vec.NewNulls(n)
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.String(strconv.Itoa(v)), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}
