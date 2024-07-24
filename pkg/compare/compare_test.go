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

package compare

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 100
)

type testCase struct {
	desc bool
	proc *process.Process
	vecs []*vector.Vector
}

var (
	tcs []testCase
)

func init() {
	mp := mpool.MustNewZero()
	tcs = []testCase{
		newTestCase(true, mp, types.New(types.T_bool, 0, 0)),
		newTestCase(false, mp, types.New(types.T_bool, 0, 0)),

		newTestCase(true, mp, types.New(types.T_bit, 0, 0)),
		newTestCase(false, mp, types.New(types.T_bit, 0, 0)),

		newTestCase(true, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(false, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(true, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(false, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(true, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(false, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(true, mp, types.New(types.T_int64, 0, 0)),
		newTestCase(false, mp, types.New(types.T_int64, 0, 0)),

		newTestCase(true, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(false, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(true, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(false, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(true, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(false, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(true, mp, types.New(types.T_uint64, 0, 0)),
		newTestCase(false, mp, types.New(types.T_uint64, 0, 0)),

		newTestCase(true, mp, types.New(types.T_float32, 0, 0)),
		newTestCase(false, mp, types.New(types.T_float32, 0, 0)),

		newTestCase(true, mp, types.New(types.T_float64, 0, 0)),
		newTestCase(false, mp, types.New(types.T_float64, 0, 0)),

		newTestCase(true, mp, types.New(types.T_date, 0, 0)),
		newTestCase(false, mp, types.New(types.T_date, 0, 0)),

		newTestCase(true, mp, types.New(types.T_time, 0, 0)),
		newTestCase(false, mp, types.New(types.T_time, 0, 0)),

		newTestCase(true, mp, types.New(types.T_datetime, 0, 0)),
		newTestCase(false, mp, types.New(types.T_datetime, 0, 0)),

		newTestCase(true, mp, types.New(types.T_timestamp, 0, 0)),
		newTestCase(false, mp, types.New(types.T_timestamp, 0, 0)),

		newTestCase(true, mp, types.New(types.T_decimal64, 0, 0)),
		newTestCase(false, mp, types.New(types.T_decimal64, 0, 0)),

		newTestCase(true, mp, types.New(types.T_decimal128, 0, 0)),
		newTestCase(false, mp, types.New(types.T_decimal128, 0, 0)),

		newTestCase(true, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),
		newTestCase(false, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),

		newTestCase(true, mp, types.New(types.T_blob, 0, 0)),
		newTestCase(false, mp, types.New(types.T_blob, 0, 0)),

		newTestCase(true, mp, types.New(types.T_text, 0, 0)),
		newTestCase(false, mp, types.New(types.T_text, 0, 0)),
		newTestCase(true, mp, types.New(types.T_datalink, 0, 0)),
		newTestCase(false, mp, types.New(types.T_datalink, 0, 0)),

		newTestCase(true, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),
		newTestCase(false, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),

		newTestCase(true, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),
		newTestCase(false, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),
	}
}

func TestCompare(t *testing.T) {
	for _, tc := range tcs {
		nb0 := tc.proc.Mp().CurrNB()
		c := New(*tc.vecs[0].GetType(), tc.desc, false)
		c.Set(0, tc.vecs[0])
		c.Set(1, tc.vecs[1])
		err := c.Copy(0, 1, 0, 0, tc.proc)
		require.NoError(t, err)
		c.Compare(0, 1, 0, 0)
		nb1 := tc.proc.Mp().CurrNB()
		require.Equal(t, nb0, nb1)
		// XXX MPOOL
		// tv.vecs[0].Free modifies tc.proc.Mp()
		tc.vecs[0].Free(tc.proc.Mp())
		tc.vecs[1].Free(tc.proc.Mp())
	}
}

func newTestCase(desc bool, m *mpool.MPool, typ types.Type) testCase {
	vecs := make([]*vector.Vector, 2)
	vecs[0] = testutil.NewVector(Rows, typ, m, true, nil)
	vecs[1] = testutil.NewVector(Rows, typ, m, true, nil)
	return testCase{
		desc: desc,
		vecs: vecs,
		proc: testutil.NewProcessWithMPool("", m),
	}
}
