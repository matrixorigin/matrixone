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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

func TestNewBatch(t *testing.T) {
	m := mpool.MustNewZero()
	bat := NewBatch([]types.Type{types.New(types.T_int8, 0, 0, 0)}, true, Rows, m)
	bat.Clean(m)
	require.Equal(t, int64(0), m.CurrNB())
}

func TestVector(t *testing.T) {
	m := mpool.MustNewZero()
	{
		vec := NewVector(Rows, types.New(types.T_bool, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_int8, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_int16, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_int32, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_int64, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_uint8, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_uint16, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_uint32, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_uint64, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_date, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_datetime, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_timestamp, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_float32, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_float64, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_decimal64, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_decimal128, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_char, 0, 0, 0), m, true, nil)
		vec.Free(m)
	}
	require.Equal(t, int64(0), m.CurrNB())
}
