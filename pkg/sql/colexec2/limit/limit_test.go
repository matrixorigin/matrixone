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

package limit

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10      // default rows
	BenchmarkRows = 1000000 // default rows for benchmark
)

// add unit tests for cases
type limitTestCase struct {
	arg   *Argument
	attrs []string
	types []types.Type
	proc  *process.Process
}

var (
	tcs []limitTestCase
)

func init() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []limitTestCase{
		{
			proc:  process.New(mheap.New(gm)),
			attrs: []string{"1"},
			types: []types.Type{
				{Oid: types.T_int8},
			},
			arg: &Argument{
				Seen:  0,
				Limit: 8,
			},
		},
		{
			proc:  process.New(mheap.New(gm)),
			attrs: []string{"1"},
			types: []types.Type{
				{Oid: types.T_int8},
			},
			arg: &Argument{
				Seen:  0,
				Limit: 10,
			},
		},
		{
			proc:  process.New(mheap.New(gm)),
			attrs: []string{"1"},
			types: []types.Type{
				{Oid: types.T_int8},
			},
			arg: &Argument{
				Seen:  0,
				Limit: 12,
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		String(tc.arg, buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		Prepare(tc.proc, tc.arg)
	}
}

func TestLimit(t *testing.T) {
	for _, tc := range tcs {
		Prepare(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.attrs, tc.proc)
		Call(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.attrs, tc.proc)
		Call(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = &batch.Batch{}
		Call(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = nil
		Call(tc.proc, tc.arg)
	}
}

func BenchmarkLimit(b *testing.B) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []limitTestCase{
		{
			proc:  process.New(mheap.New(gm)),
			attrs: []string{"1"},
			types: []types.Type{
				{Oid: types.T_int8},
			},
			arg: &Argument{
				Seen:  0,
				Limit: 8,
			},
		},
	}

	t := new(testing.T)
	for _, tc := range tcs {
		Prepare(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.attrs, tc.proc)
		Call(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = &batch.Batch{}
		Call(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = nil
		Call(tc.proc, tc.arg)
	}
}

// create a new block based on the attribute information, flg indicates if the data is all duplicated
func newBatch(t *testing.T, ts []types.Type, attrs []string, proc *process.Process) *batch.Batch {
	bat := batch.New(true, attrs)
	bat.Zs = make([]int64, Rows)
	bat.Ht = []*vector.Vector{}
	for i := range bat.Zs {
		bat.Zs[i] = 1
	}
	for i := range bat.Vecs {
		vec := vector.New(ts[i])
		switch vec.Typ.Oid {
		case types.T_int8:
			data, err := mheap.Alloc(proc.Mp, Rows*1)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt8Slice(vec.Data)[:Rows]
			for i := range vs {
				vs[i] = int8(i)
			}
			vec.Col = vs
		case types.T_int64:
			data, err := mheap.Alloc(proc.Mp, Rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt64Slice(vec.Data)[:Rows]
			for i := range vs {
				vs[i] = int64(i)
			}
			vec.Col = vs
		case types.T_float64:
			data, err := mheap.Alloc(proc.Mp, Rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeFloat64Slice(vec.Data)[:Rows]
			for i := range vs {
				vs[i] = float64(i)
			}
			vec.Col = vs
		case types.T_date:
			data, err := mheap.Alloc(proc.Mp, Rows*4)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeDateSlice(vec.Data)[:Rows]
			for i := range vs {
				vs[i] = types.Date(i)
			}
			vec.Col = vs
		case types.T_char, types.T_varchar:
			size := 0
			vs := make([][]byte, Rows)
			for i := range vs {
				vs[i] = []byte(strconv.Itoa(i))
				size += len(vs[i])
			}
			data, err := mheap.Alloc(proc.Mp, int64(size))
			require.NoError(t, err)
			data = data[:0]
			col := new(types.Bytes)
			o := uint32(0)
			for _, v := range vs {
				data = append(data, v...)
				col.Offsets = append(col.Offsets, o)
				o += uint32(len(v))
				col.Lengths = append(col.Lengths, uint32(len(v)))
			}
			col.Data = data
			vec.Col = col
			vec.Data = data
		}
		bat.Vecs[i] = vec
	}
	return bat
}
