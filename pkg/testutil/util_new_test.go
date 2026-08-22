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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

func TestNewBatch(t *testing.T) {
	m := mpool.MustNewZero()
	bat := NewBatch([]types.Type{types.New(types.T_int8, 0, 0)}, true, Rows, m)
	bat.Clean(m)
	require.Equal(t, int64(0), m.CurrNB())
}

func TestNilTBProcessesShareAutoIncrService(t *testing.T) {
	first := NewProcessWithMPool(nil, "", mpool.MustNewZeroNoFixed())
	second := NewProcessWithMPool(nil, "", mpool.MustNewZeroNoFixed())

	require.True(t, first.GetIncrService() == second.GetIncrService())

	setupAutoIncrServiceForProcess(nil, "nil-tb-first")
	firstSID := incrservice.GetAutoIncrementService("nil-tb-first")
	setupAutoIncrServiceForProcess(nil, "nil-tb-other")
	otherSID := incrservice.GetAutoIncrementService("nil-tb-other")
	require.True(t, firstSID != otherSID)
}

func TestNilTBProcessRestoresSharedAutoIncrService(t *testing.T) {
	const sid = ""
	shared := NewProcessWithMPool(nil, sid, mpool.MustNewZeroNoFixed()).GetIncrService()
	replacement := incrservice.NewIncrService(
		"",
		incrservice.NewMemStore(),
		incrservice.Config{})
	t.Cleanup(replacement.Close)
	incrservice.SetAutoIncrementServiceByID(sid, replacement)

	restored := NewProcessWithMPool(nil, sid, mpool.MustNewZeroNoFixed()).GetIncrService()
	require.True(t, shared == restored)
}

func TestNilTBProcessesShareAutoIncrServiceConcurrently(t *testing.T) {
	const count = 16
	const sid = ""
	services := make([]incrservice.AutoIncrementService, count)
	var wg sync.WaitGroup
	for i := range services {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			proc := NewProcessWithMPool(nil, sid, mpool.MustNewZeroNoFixed())
			services[index] = proc.GetIncrService()
		}(i)
	}
	wg.Wait()

	for i := 1; i < count; i++ {
		require.True(t, services[0] == services[i], "service %d", i)
	}
}

func TestVector(t *testing.T) {
	m := mpool.MustNewZero()
	{
		vec := NewVector(Rows, types.New(types.T_bool, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_int8, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_int16, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_int32, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_int64, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_uint8, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_uint16, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_uint32, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_uint64, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_date, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_time, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_datetime, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_timestamp, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_float32, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_float64, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_decimal64, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_decimal128, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_char, 0, 0), m, true, nil)
		vec.Free(m)
	}
	{
		vec := NewVector(Rows, types.New(types.T_json, 0, 0), m, true, nil)
		vec.Free(m)
	}
	require.Equal(t, int64(0), m.CurrNB())
}
