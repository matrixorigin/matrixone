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

package times

import (
	"bytes"
	"strconv"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/join"
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
type timesTestCase struct {
	flg            bool // flg indicates if the data is all duplicated
	hasNull        bool // flg indicates if the data  has null
	arg            *Argument
	factAttrs      []string
	dimensionAttrs [][]string

	factTypes      []types.Type
	dimensionTypes [][]types.Type
	proc           *process.Process
}

var (
	tcs []timesTestCase
)

func init() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	tcs = []timesTestCase{
		{
			flg:       false,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"1"},
			dimensionAttrs: [][]string{
				{"1"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"1"},
			dimensionAttrs: [][]string{
				{"1"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"1"},
			dimensionAttrs: [][]string{
				{"1"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"1"},
			dimensionAttrs: [][]string{
				{"1"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
				types.Type{Oid: types.T_int8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_int8}, types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
				types.Type{Oid: types.T_int8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_int8}, types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
				types.Type{Oid: types.T_int8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_int8}, types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
				types.Type{Oid: types.T_int8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_int8}, types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int32},
				types.Type{Oid: types.T_uint8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_uint8}, types.Type{Oid: types.T_int64}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int32},
				types.Type{Oid: types.T_uint8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_uint8}, types.Type{Oid: types.T_int64}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int32},
				types.Type{Oid: types.T_uint8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_uint8}, types.Type{Oid: types.T_int64}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int32},
				types.Type{Oid: types.T_uint8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_uint8}, types.Type{Oid: types.T_int64}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 10},
				types.Type{Oid: types.T_varchar, Width: 10},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 10}, types.Type{Oid: types.T_varchar, Width: 10}},
			},
			arg: &Argument{
				Result: []string{"0", "1"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 10},
				types.Type{Oid: types.T_varchar, Width: 10},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 10}, types.Type{Oid: types.T_varchar, Width: 10}},
			},
			arg: &Argument{
				Result: []string{"0", "1"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 10},
				types.Type{Oid: types.T_varchar, Width: 10},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 10}, types.Type{Oid: types.T_varchar, Width: 10}},
			},
			arg: &Argument{
				Result: []string{"0", "1"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 10},
				types.Type{Oid: types.T_varchar, Width: 10},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 10}, types.Type{Oid: types.T_varchar, Width: 10}},
			},
			arg: &Argument{
				Result: []string{"0", "1"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
				types.Type{Oid: types.T_varchar, Width: 10},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 10}, types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{"0", "1"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
				types.Type{Oid: types.T_varchar, Width: 10},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 10}, types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{"0", "1"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
				types.Type{Oid: types.T_varchar, Width: 10},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 10}, types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{"0", "1"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_int8},
				types.Type{Oid: types.T_varchar, Width: 10},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 10}, types.Type{Oid: types.T_int8}},
			},
			arg: &Argument{
				Result: []string{"0", "1"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 8},
				types.Type{Oid: types.T_varchar, Width: 8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 8}, types.Type{Oid: types.T_varchar, Width: 8}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 8},
				types.Type{Oid: types.T_varchar, Width: 8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 8}, types.Type{Oid: types.T_varchar, Width: 8}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 8},
				types.Type{Oid: types.T_varchar, Width: 8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 8}, types.Type{Oid: types.T_varchar, Width: 8}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 8},
				types.Type{Oid: types.T_varchar, Width: 8},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 8}, types.Type{Oid: types.T_varchar, Width: 8}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1", "2"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 12},
				types.Type{Oid: types.T_varchar, Width: 12},
				types.Type{Oid: types.T_varchar, Width: 12},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 12}, types.Type{Oid: types.T_varchar, Width: 12}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1", "2"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1", "2"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 12},
				types.Type{Oid: types.T_varchar, Width: 12},
				types.Type{Oid: types.T_varchar, Width: 12},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 12}, types.Type{Oid: types.T_varchar, Width: 12}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1", "2"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1", "2"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 12},
				types.Type{Oid: types.T_varchar, Width: 12},
				types.Type{Oid: types.T_varchar, Width: 12},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 12}, types.Type{Oid: types.T_varchar, Width: 12}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1", "2"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1", "2"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar, Width: 12},
				types.Type{Oid: types.T_varchar, Width: 12},
				types.Type{Oid: types.T_varchar, Width: 12},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar, Width: 12}, types.Type{Oid: types.T_varchar, Width: 12}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1", "2"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1", "2"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar},
				types.Type{Oid: types.T_varchar},
				types.Type{Oid: types.T_varchar},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar}, types.Type{Oid: types.T_varchar}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1", "2"},
				},
			},
		},
		{
			flg:       false,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1", "2"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar},
				types.Type{Oid: types.T_varchar},
				types.Type{Oid: types.T_varchar},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar}, types.Type{Oid: types.T_varchar}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1", "2"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   false,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1", "2"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar},
				types.Type{Oid: types.T_varchar},
				types.Type{Oid: types.T_varchar},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar}, types.Type{Oid: types.T_varchar}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1", "2"},
				},
			},
		},
		{
			flg:       true,
			hasNull:   true,
			proc:      process.New(mheap.New(gm)),
			factAttrs: []string{"0", "1", "2"},
			dimensionAttrs: [][]string{
				{"1", "2"},
			},

			factTypes: []types.Type{
				types.Type{Oid: types.T_varchar},
				types.Type{Oid: types.T_varchar},
				types.Type{Oid: types.T_varchar},
			},
			dimensionTypes: [][]types.Type{
				{types.Type{Oid: types.T_varchar}, types.Type{Oid: types.T_varchar}},
			},
			arg: &Argument{
				Result: []string{"0", "1", "2"},
				Vars: [][]string{
					{"1", "2"},
				},
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
		for i, dmAttrs := range tc.dimensionAttrs {
			tc.arg.Bats = append(tc.arg.Bats, newBatch(t, tc.flg, tc.hasNull, tc.dimensionTypes[i], dmAttrs, tc.proc))
		}
		constructViews(tc.arg.Bats, tc.arg.Vars)
		Prepare(tc.proc, tc.arg)
	}
}

func TestTimes(t *testing.T) {
	for _, tc := range tcs {
		for i, dmAttrs := range tc.dimensionAttrs {
			tc.arg.Bats = append(tc.arg.Bats, newBatch(t, tc.flg, tc.hasNull, tc.dimensionTypes[i], dmAttrs, tc.proc))
		}
		constructViews(tc.arg.Bats, tc.arg.Vars)
		Prepare(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = newBatch(t, tc.flg, tc.hasNull, tc.factTypes, tc.factAttrs, tc.proc)
		Call(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = &batch.Batch{}
		Call(tc.proc, tc.arg)
		tc.proc.Reg.InputBatch = nil
		Call(tc.proc, tc.arg)
		require.Equal(t, len(tc.proc.Reg.InputBatch.Attrs), len(tc.arg.Result))
	}
}

// create a new block based on the attribute information, flg indicates if the data is all duplicated
func newBatch(t *testing.T, flg, isNull bool, ts []types.Type, attrs []string, proc *process.Process) *batch.Batch {
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
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = int8(i)
				}
			}
			vec.Col = vs
		case types.T_int16:
			data, err := mheap.Alloc(proc.Mp, Rows*2)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt16Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = int16(i)
				}
			}
			vec.Col = vs
		case types.T_int32:
			data, err := mheap.Alloc(proc.Mp, Rows*4)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt32Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = int32(i)
				}
			}
			vec.Col = vs
		case types.T_int64:
			data, err := mheap.Alloc(proc.Mp, Rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt64Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = int64(i)
				}
			}
			vec.Col = vs
		case types.T_uint8:
			data, err := mheap.Alloc(proc.Mp, Rows)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeUint8Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = uint8(i)
				}
			}
			vec.Col = vs
		case types.T_uint16:
			data, err := mheap.Alloc(proc.Mp, Rows*2)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeUint16Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = uint16(i)
				}
			}
			vec.Col = vs
		case types.T_uint32:
			data, err := mheap.Alloc(proc.Mp, Rows*4)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeUint32Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = uint32(i)
				}
			}
			vec.Col = vs
		case types.T_uint64:
			data, err := mheap.Alloc(proc.Mp, Rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeUint64Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = uint64(i)
				}
			}
			vec.Col = vs
		case types.T_float32:
			data, err := mheap.Alloc(proc.Mp, Rows*4)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeFloat32Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = float32(i)
				}
			}
			vec.Col = vs
		case types.T_float64:
			data, err := mheap.Alloc(proc.Mp, Rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeFloat64Slice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = float64(i)
				}
			}
			vec.Col = vs
		case types.T_date:
			data, err := mheap.Alloc(proc.Mp, Rows*4)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeDateSlice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = types.Date(i)
				}
			}
			vec.Col = vs
		case types.T_datetime:
			data, err := mheap.Alloc(proc.Mp, Rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeDatetimeSlice(vec.Data)[:Rows]
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = 0
				} else {
					vs[i] = types.Datetime(i)
				}
			}
			vec.Col = vs
		case types.T_char, types.T_varchar:
			size := 0
			vs := make([][]byte, Rows)
			if isNull {
				nulls.Add(vec.Nsp, 0)
			}
			for i := range vs {
				if flg {
					vs[i] = []byte("0")
				} else {
					vs[i] = []byte(strconv.Itoa(i))
				}
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

func constructViews(bats []*batch.Batch, vars [][]string) {
	for i := range vars {
		constructView(bats[i], vars[i])
	}
}

func constructView(bat *batch.Batch, vars []string) {
	var rows uint64

	batch.Reorder(bat, vars)
	if len(vars) == 1 {
		constructViewWithOneVar(bat, vars[0])
		return
	}
	ht := &join.HashTable{
		StrHashMap: &hashtable.StringHashMap{},
	}
	ht.StrHashMap.Init()
	keys := make([][]byte, UnitLimit)
	values := make([]uint64, UnitLimit)
	strHashStates := make([][3]uint64, UnitLimit)
	vecs := make([]*vector.Vector, len(vars))
	{ // fill vectors
		for i := range vars {
			vecs[i] = batch.GetVector(bat, vars[i])
		}
	}
	count := int64(len(bat.Zs))
	zValues := make([]int64, UnitLimit)
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(zValues[:n], OneInt64s[:n])
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vecs[j].Col.([]int8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
						}
					}
				}
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
						}
					}
				}
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
						}
					}
				}
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
						}
					}
				}
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_date:
				vs := vecs[j].Col.([]types.Date)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
						}
					}
				}
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_datetime:
				vs := vecs[j].Col.([]types.Datetime)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				if !nulls.Any(vecs[j].Nsp) {
					for k := int64(0); k < n; k++ {
						keys[k] = append(keys[k], vs.Get(i+k)...)
					}
				} else {
					for k := int64(0); k < n; k++ {
						if vecs[j].Nsp.Np.Contains(uint64(i + k)) {
							zValues[k] = 0
						} else {
							keys[k] = append(keys[k], vs.Get(i+k)...)
						}
					}
				}
			}
		}
		for k := int64(0); k < n; k++ {
			if l := len(keys[k]); l < 16 {
				keys[k] = append(keys[k], hashtable.StrKeyPadding[l:]...)
			}
		}
		ht.StrHashMap.InsertStringBatchWithRing(zValues, strHashStates, keys[:n], values)
		{
			for k, v := range values[:n] {
				keys[k] = keys[k][:0]
				if zValues[k] == 0 {
					continue
				}
				if v > rows {
					ht.Sels = append(ht.Sels, make([]int64, 0, 8))
				}
				ai := int64(v) - 1
				ht.Sels[ai] = append(ht.Sels[ai], i+int64(k))
			}
		}
	}
	bat.Ht = ht
}

func constructViewWithOneVar(bat *batch.Batch, fvar string) {
	var rows uint64

	vec := batch.GetVector(bat, fvar)
	switch vec.Typ.Oid {
	case types.T_int8:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]int8)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_uint8:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]uint8)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_int16:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]int16)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_uint16:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]uint16)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_int32:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]int32)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_uint32:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]uint32)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_int64:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]int64)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_uint64:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]uint64)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_float32:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]float32)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_date:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]types.Date)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_float64:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]float64)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_datetime:
		ht := &join.HashTable{
			IntHashMap: &hashtable.Int64HashMap{},
		}
		ht.IntHashMap.Init()
		vs := vec.Col.([]types.Datetime)
		keys := make([]uint64, UnitLimit)
		hashes := make([]uint64, UnitLimit)
		values := make([]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatch(n, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = uint64(vs[int(i)+k])
					}
				}
				hashes[0] = 0
				ht.IntHashMap.InsertBatchWithRing(n, zValues, hashes, unsafe.Pointer(&keys[0]), values)
				for k, v := range values[:n] {
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	case types.T_char, types.T_varchar:
		ht := &join.HashTable{
			StrHashMap: &hashtable.StringHashMap{},
		}
		ht.StrHashMap.Init()
		vs := vec.Col.(*types.Bytes)
		keys := make([][]byte, UnitLimit)
		values := make([]uint64, UnitLimit)
		strHashStates := make([][3]uint64, UnitLimit)
		count := len(bat.Zs)
		if !nulls.Any(vec.Nsp) {
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						keys[k] = append(keys[k], vs.Get(int64(i+k))...)
					}
				}
				for k := 0; k < n; k++ {
					if l := len(keys[k]); l < 16 {
						keys[k] = append(keys[k], hashtable.StrKeyPadding[l:]...)
					}
				}
				ht.StrHashMap.InsertStringBatch(strHashStates, keys[:n], values)
				for k, v := range values[:n] {
					keys[k] = keys[k][:0]
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		} else {
			zValues := make([]int64, UnitLimit)
			for i := 0; i < count; i += UnitLimit {
				n := count - i
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					copy(zValues[:n], OneInt64s[:n])
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(i + k)) {
							zValues[i] = 0
						}
						keys[k] = append(keys[k], vs.Get(int64(i+k))...)
					}
				}
				for k := 0; k < n; k++ {
					if l := len(keys[k]); l < 16 {
						keys[k] = append(keys[k], hashtable.StrKeyPadding[l:]...)
					}
				}
				ht.StrHashMap.InsertStringBatchWithRing(zValues, strHashStates, keys[:n], values)
				for k, v := range values[:n] {
					keys[k] = keys[k][:0]
					if zValues[k] == 0 {
						continue
					}
					if v > rows {
						ht.Sels = append(ht.Sels, make([]int64, 0, 8))
					}
					ai := int64(v) - 1
					ht.Sels[ai] = append(ht.Sels[ai], int64(i+k))
				}
			}
		}
		bat.Ht = ht
	}
}
