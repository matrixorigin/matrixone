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

package mergetop

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	top "github.com/matrixorigin/matrixone/pkg/sql/colexec2/top"
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
type topTestCase struct {
	ds     []bool // Directions, ds[i] == true: the attrs[i] are in descending order
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []topTestCase
)

func init() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []topTestCase{
		newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_int8}}, 3, []top.Field{{Pos: 0, Type: 0}}),
		newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_int8}}, 3, []top.Field{{Pos: 0, Type: 2}}),
		newTestCase(mheap.New(gm), []bool{false, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, 3, []top.Field{{Pos: 1, Type: 0}}),
		newTestCase(mheap.New(gm), []bool{true, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, 3, []top.Field{{Pos: 0, Type: 2}}),
		newTestCase(mheap.New(gm), []bool{true, false}, []types.Type{{Oid: types.T_int8}, {Oid: types.T_int64}}, 3, []top.Field{{Pos: 0, Type: 2}, {Pos: 1, Type: 0}}),
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

func TestTop(t *testing.T) {
	for _, tc := range tcs {
		Prepare(tc.proc, tc.arg)
		tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.ds, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[0].Ch <- nil
		tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.ds, tc.types, tc.proc, Rows)
		tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
		tc.proc.Reg.MergeReceivers[1].Ch <- nil
		for {
			if ok, err := Call(tc.proc, tc.arg); ok || err != nil {
				if tc.proc.Reg.InputBatch != nil {
					tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
				}
				break
			}
		}
		for i := 0; i < len(tc.proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
			for len(tc.proc.Reg.MergeReceivers[i].Ch) > 0 {
				bat := <-tc.proc.Reg.MergeReceivers[i].Ch
				if bat != nil {
					bat.Clean(tc.proc.Mp)
				}
			}
		}
		require.Equal(t, mheap.Size(tc.proc.Mp), int64(0))
	}
}

func BenchmarkTop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hm := host.New(1 << 30)
		gm := guest.New(1<<30, hm)
		tcs = []topTestCase{
			newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_int8}}, 3, []top.Field{{Pos: 0, Type: 0}}),
			newTestCase(mheap.New(gm), []bool{true}, []types.Type{{Oid: types.T_int8}}, 3, []top.Field{{Pos: 0, Type: 2}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			Prepare(tc.proc, tc.arg)
			tc.proc.Reg.MergeReceivers[0].Ch <- newBatch(t, tc.ds, tc.types, tc.proc, BenchmarkRows)
			tc.proc.Reg.MergeReceivers[0].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[0].Ch <- nil
			tc.proc.Reg.MergeReceivers[1].Ch <- newBatch(t, tc.ds, tc.types, tc.proc, BenchmarkRows)
			tc.proc.Reg.MergeReceivers[1].Ch <- &batch.Batch{}
			tc.proc.Reg.MergeReceivers[1].Ch <- nil
			for {
				if ok, err := Call(tc.proc, tc.arg); ok || err != nil {
					if tc.proc.Reg.InputBatch != nil {
						tc.proc.Reg.InputBatch.Clean(tc.proc.Mp)
					}
					break
				}
			}
			for i := 0; i < len(tc.proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
				for len(tc.proc.Reg.MergeReceivers[i].Ch) > 0 {
					bat := <-tc.proc.Reg.MergeReceivers[i].Ch
					if bat != nil {
						bat.Clean(tc.proc.Mp)
					}
				}
			}
		}
	}
}

func newTestCase(m *mheap.Mheap, ds []bool, ts []types.Type, limit int64, fs []top.Field) topTestCase {
	proc := process.New(m)
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	proc.Reg.MergeReceivers[1] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 3),
	}
	return topTestCase{
		ds:    ds,
		types: ts,
		proc:  proc,
		arg: &Argument{
			Fs:    fs,
			Limit: limit,
		},
		cancel: cancel,
	}
}

// create a new block based on the type information, ds[i] == true: in descending order
func newBatch(t *testing.T, ds []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.InitZsOne(int(rows))
	for i := range bat.Vecs {
		flg := ds[i]
		vec := vector.New(ts[i])
		switch vec.Typ.Oid {
		case types.T_int8:
			data, err := mheap.Alloc(proc.Mp, rows*1)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt8Slice(vec.Data)[:rows]
			if flg {
				for i := range vs {
					vs[i] = int8(rows) - int8(i) - 1
				}
			} else {
				for i := range vs {
					vs[i] = int8(i)
				}
			}
			vec.Col = vs
		case types.T_int64:
			data, err := mheap.Alloc(proc.Mp, rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt64Slice(vec.Data)[:rows]
			if flg {
				for i := range vs {
					vs[i] = int64(rows) - int64(i) - 1
				}
			} else {
				for i := range vs {
					vs[i] = int64(i)
				}
			}
			vec.Col = vs
		}
		bat.Vecs[i] = vec
	}
	return bat
}
