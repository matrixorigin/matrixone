// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
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
	Rows = 10 // default test rows
)

type hashTestCase struct {
	flgs  []bool // flgs[i] == true: nullable
	types []types.Type
	proc  *process.Process
}

var (
	tcs []hashTestCase
)

func init() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []hashTestCase{
		newTestCase(mheap.New(gm), []bool{false}, []types.Type{{Oid: types.T_int8}}),
		newTestCase(mheap.New(gm), []bool{false, true, false, true}, []types.Type{
			{Oid: types.T_int8},
			{Oid: types.T_int16},
			{Oid: types.T_int32},
			{Oid: types.T_int64},
		}),
		newTestCase(mheap.New(gm), []bool{false, true, false, true}, []types.Type{
			{Oid: types.T_int8},
			{Oid: types.T_int16},
			{Oid: types.T_varchar},
			{Oid: types.T_varchar},
		}),
	}
}

func TestHash(t *testing.T) {
	for _, tc := range tcs {
		bat := newBatch(t, tc.flgs, tc.types, tc.proc, Rows)
		vec, err := Hash(bat.Vecs, tc.proc)
		require.NoError(t, err)
		bat.Clean(tc.proc.GetMheap())
		vec.Free(tc.proc.GetMheap())
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp()))
	}
}

func newTestCase(m *mheap.Mheap, flgs []bool, ts []types.Type) hashTestCase {
	return hashTestCase{
		types: ts,
		flgs:  flgs,
		proc:  testutil.NewProcessWithMheap(m),
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(t *testing.T, flgs []bool, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
