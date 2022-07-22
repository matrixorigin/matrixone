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

package dispatch

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type dispatchTestCase struct {
	arg    *Argument
	types  []types.Type
	proc   *process.Process
	cancel context.CancelFunc
}

var (
	tcs []dispatchTestCase
)

func init() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []dispatchTestCase{
		newTestCase(gm, true),
		newTestCase(gm, false),
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
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
	}
}

func TestDispatch(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		bat := newBatch(t, tc.types, tc.proc, Rows)
		tc.proc.Reg.InputBatch = bat
		{
			for _, vec := range bat.Vecs {
				if vec.Or {
					mheap.Free(tc.proc.Mp, vec.Data)
				}
			}
		}
		_, _ = Call(0, tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = &batch.Batch{}
		_, _ = Call(0, tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = nil
		_, _ = Call(0, tc.proc, tc.arg)
		for {
			bat := <-tc.arg.Regs[0].Ch
			if bat == nil {
				break
			}
			if len(bat.Zs) == 0 {
				continue
			}
			bat.Clean(tc.proc.Mp)
		}
		require.Equal(t, mheap.Size(tc.proc.Mp), int64(0))
	}
}

func newTestCase(gm *guest.Mmu, all bool) dispatchTestCase {
	proc := process.New(mheap.New(gm))
	proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
	ctx, cancel := context.WithCancel(context.Background())
	reg := &process.WaitRegister{Ctx: ctx, Ch: make(chan *batch.Batch, 3)}
	return dispatchTestCase{
		proc: proc,
		types: []types.Type{
			{Oid: types.T_int8},
		},
		arg: &Argument{
			All:  all,
			Mmu:  gm,
			Regs: []*process.WaitRegister{reg},
		},
		cancel: cancel,
	}

}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp)
}
