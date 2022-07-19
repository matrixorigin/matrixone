// Copyright 2022 Matrix Origin
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

package avg

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

func TestSum(t *testing.T) {
	testTyp := types.New(types.T_int8, 0, 0, 0)
	retTyp := types.New(types.T_float64, 0, 0, 0)
	s1 := NewAvg[int8, float64](testTyp)
	s2 := NewAvg[int8, float64](testTyp)
	s3 := NewAvg[int8, float64](testTyp)
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs1 := []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	vs2 := []int8{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	vec1 := testutil.NewVector(Rows, testTyp, m, false, vs1)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	expected1 := []float64{4.5}
	expected2 := []float64{14.5}
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(s1, true, testTyp, retTyp, s1.Grows, s1.Eval, s1.Merge, s1.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec1})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, expected1, vector.GetColumn[float64](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(s2, true, testTyp, retTyp, s2.Grows, s2.Eval, s2.Merge, s2.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec1})
		}
		agg1 := agg.NewUnaryAgg(s3, true, testTyp, retTyp, s3.Grows, s3.Eval, s3.Merge, s3.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []float64{9.5}, vector.GetColumn[float64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, expected2, vector.GetColumn[float64](v))
			v.Free(m)
		}
	}
	vec1.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

//TODO: add decimal test
