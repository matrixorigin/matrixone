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

package semi

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
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
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg    *Argument
	flgs   []bool // flgs[i] == true: nullable
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []joinTestCase
)

func init() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []joinTestCase{
		newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_int8}}, []int32{0},
			[][]Condition{
				{
					{0, 0, types.Type{Oid: types.T_int8}},
				},
				{
					{0, 0, types.Type{Oid: types.T_int8}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_int8}}, []int32{0},
			[][]Condition{
				{
					{0, 0, types.Type{Oid: types.T_int8}},
				},
				{
					{0, 0, types.Type{Oid: types.T_int8}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_decimal64}}, []int32{0},
			[][]Condition{
				{
					{0, 0, types.Type{Oid: types.T_decimal64}},
				},
				{
					{0, 1, types.Type{Oid: types.T_decimal64}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_decimal64}}, []int32{0},
			[][]Condition{
				{
					{0, 0, types.Type{Oid: types.T_decimal64}},
				},
				{
					{0, 1, types.Type{Oid: types.T_decimal64}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_decimal128}}, []int32{0},
			[][]Condition{
				{
					{0, 0, types.Type{Oid: types.T_decimal128}},
				},
				{
					{0, 1, types.Type{Oid: types.T_decimal128}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_decimal128}}, []int32{0},
			[][]Condition{
				{
					{0, 0, types.Type{Oid: types.T_decimal128}},
				},
				{
					{0, 1, types.Type{Oid: types.T_decimal128}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, []int32{0},
			[][]Condition{
				{
					{1, 0, types.Type{Oid: types.T_int64}},
				},
				{
					{1, 0, types.Type{Oid: types.T_int64}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, []int32{0},
			[][]Condition{
				{
					{1, 0, types.Type{Oid: types.T_int64}},
				},
				{
					{1, 0, types.Type{Oid: types.T_int64}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_decimal64}}, []int32{0},
			[][]Condition{
				{
					{1, 0, types.Type{Oid: types.T_decimal64}},
				},
				{
					{1, 1, types.Type{Oid: types.T_decimal64}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_decimal64}}, []int32{0},
			[][]Condition{
				{
					{1, 0, types.Type{Oid: types.T_decimal64}},
				},
				{
					{1, 1, types.Type{Oid: types.T_decimal64}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{false, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_decimal128}}, []int32{0},
			[][]Condition{
				{
					{1, 0, types.Type{Oid: types.T_decimal128}},
				},
				{
					{1, 1, types.Type{Oid: types.T_decimal128}},
				},
			}),
		newTestCase(mheap.New(gm), []bool{true, true}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_decimal128}}, []int32{0},
			[][]Condition{
				{
					{1, 0, types.Type{Oid: types.T_decimal128}},
				},
				{
					{1, 1, types.Type{Oid: types.T_decimal128}},
				},
			}),
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

func TestJoin(t *testing.T) {
	for _, tc := range tcs {
		Prepare(tc.proc, tc.arg)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			if ok, err := Call(tc.proc, tc.arg); ok || err != nil {
				break
			}
			tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
		}
		require.Equal(t, mheap.Size(tc.proc.Mp), int64(0))
	}
}

func BenchmarkJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hm := host.New(1 << 30)
		gm := guest.New(1<<30, hm)
		tcs = []joinTestCase{
			newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_int8}}, []int32{0},
				[][]Condition{
					{
						{0, 0, types.Type{Oid: types.T_int8}},
					},
					{
						{0, 0, types.Type{Oid: types.T_int8}},
					},
				}),
			newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_int8}}, []int32{0},
				[][]Condition{
					{
						{0, 0, types.Type{Oid: types.T_int8}},
					},
					{
						{0, 0, types.Type{Oid: types.T_int8}},
					},
				}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			Prepare(tc.proc, tc.arg)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
			tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[1].Ch <- nil
			for {
				if ok, err := Call(tc.proc, tc.arg); ok || err != nil {
					break
				}
				tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
			}
		}
	}
}

func newTestCase(m *mheap.Mheap, flgs []bool, ts []types.Type, rp []int32, cs [][]Condition) joinTestCase {
	proc := process.New(m)
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 10),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	return joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &Argument{
			Result:     rp,
			Conditions: cs,
		},
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.InitZsOne(int(rows))
	for i := range bat.Vecs {
		vec := vector.New(ts[i])
		switch vec.Typ.Oid {
		case types.T_int8:
			data, err := mheap.Alloc(proc.Mp, rows*1)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt8Slice(vec.Data)[:rows]
			for i := range vs {
				vs[i] = int8(i)
			}
			if flgs[i] {
				nulls.Add(vec.Nsp, uint64(0))
			}
			vec.Col = vs
		case types.T_int16:
			data, err := mheap.Alloc(proc.Mp, rows*2)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt16Slice(vec.Data)[:rows]
			for i := range vs {
				vs[i] = int16(i)
			}
			if flgs[i] {
				nulls.Add(vec.Nsp, uint64(0))
			}
			vec.Col = vs
		case types.T_int32:
			data, err := mheap.Alloc(proc.Mp, rows*4)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt32Slice(vec.Data)[:rows]
			for i := range vs {
				vs[i] = int32(i)
			}
			if flgs[i] {
				nulls.Add(vec.Nsp, uint64(0))
			}
			vec.Col = vs
		case types.T_int64:
			data, err := mheap.Alloc(proc.Mp, rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt64Slice(vec.Data)[:rows]
			for i := range vs {
				vs[i] = int64(i)
			}
			if flgs[i] {
				nulls.Add(vec.Nsp, uint64(0))
			}
			vec.Col = vs
		case types.T_decimal64:
			data, err := mheap.Alloc(proc.Mp, rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeDecimal64Slice(vec.Data)[:rows]
			for i := range vs {
				vs[i] = types.Decimal64(i)
			}
			if flgs[i] {
				nulls.Add(vec.Nsp, uint64(0))
			}
			vec.Col = vs
		case types.T_decimal128:
			data, err := mheap.Alloc(proc.Mp, rows*16)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeDecimal128Slice(vec.Data)[:rows]
			for i := range vs {
				vs[i].Lo = int64(i)
				vs[i].Hi = int64(i)
			}
			if flgs[i] {
				nulls.Add(vec.Nsp, uint64(0))
			}
			vec.Col = vs

		case types.T_char, types.T_varchar:
			size := 0
			vs := make([][]byte, rows)
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
			if flgs[i] {
				nulls.Add(vec.Nsp, uint64(0))
			}
			col.Data = data
			vec.Col = col
			vec.Data = data
		}
		bat.Vecs[i] = vec
	}
	return bat
}
