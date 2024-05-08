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

package batch

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type batchTestCase struct {
	bat   *Batch
	types []types.Type
}

var (
	tcs []batchTestCase
)

func init() {
	tcs = []batchTestCase{
		newTestCase([]types.Type{types.T_int8.ToType()}),
	}
}

func TestBatchMarshalAndUnmarshal(t *testing.T) {
	for _, tc := range tcs {
		data, err := tc.bat.MarshalBinary()
		require.NoError(t, err)

		rbat := new(Batch)
		err = rbat.UnmarshalBinary(data)
		require.NoError(t, err)
		for i, vec := range rbat.Vecs {
			require.Equal(t, vector.MustFixedCol[int8](tc.bat.Vecs[i]), vector.MustFixedCol[int8](vec))
		}
	}
}

func TestBatch(t *testing.T) {
	for _, tc := range tcs {
		data, err := types.Encode(tc.bat)
		require.NoError(t, err)
		rbat := new(Batch)
		err = types.Decode(data, rbat)
		require.NoError(t, err)
		for i, vec := range rbat.Vecs {
			require.Equal(t, vector.MustFixedCol[int8](tc.bat.Vecs[i]), vector.MustFixedCol[int8](vec))
		}
	}
}

func TestBatchShrink(t *testing.T) {
	bat := newBatch([]types.Type{types.T_int8.ToType()}, 4)
	bat.Shrink([]int64{0}, true)
	require.Equal(t, 3, bat.rowCount)
	bat.Shrink([]int64{0, 2}, false)
	require.Equal(t, 2, bat.rowCount)
}

func TestBatch_ReplaceVector(t *testing.T) {
	v1, v2, v3 := &vector.Vector{}, &vector.Vector{}, &vector.Vector{}
	bat := &Batch{
		Vecs: []*vector.Vector{
			v1,
			v1,
			v1,
			v2,
			v2,
		},
	}
	bat.ReplaceVector(bat.Vecs[0], v3)
	require.Equal(t, v3, bat.Vecs[0])
	require.Equal(t, v3, bat.Vecs[1])
	require.Equal(t, v3, bat.Vecs[2])
	require.Equal(t, v2, bat.Vecs[3])
}

func newTestCase(ts []types.Type) batchTestCase {
	return batchTestCase{
		types: ts,
		bat:   newBatch(ts, Rows),
	}
}

// create a new block based on the type information, flgs[i] == ture: has null
func newBatch(ts []types.Type, rows int) *Batch {
	mp := mpool.MustNewZero()
	bat := NewWithSize(len(ts))
	bat.SetRowCount(rows)
	for i, typ := range ts {
		switch typ.Oid {
		case types.T_int8:
			vec := vector.NewVec(typ)
			err := vec.PreExtend(rows, mp)
			if err != nil {
				panic(err)
			}
			vec.SetLength(rows)
			vs := vector.MustFixedCol[int8](vec)
			for j := range vs {
				vs[j] = int8(j)
			}
			bat.Vecs[i] = vec
		}
	}
	return bat
}
