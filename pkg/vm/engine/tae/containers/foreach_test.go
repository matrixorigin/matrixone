// Copyright 2026 Matrix Origin
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

package containers

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// collectRows iterates vec[start, start+length) with an untyped op, checks
// each value against src at row+bias, and returns the visited rows.
func collectRows(
	t *testing.T, vec, src Vector, bias, start, length int, sels *nulls.Nulls,
) []int {
	rows := []int{}
	op := func(v any, isNull bool, row int) error {
		rows = append(rows, row)
		require.Equal(t, src.IsNull(uint64(row+bias)), isNull, "row %d", row)
		require.Equal(t, src.GetValue(row+bias), v, "row %d", row)
		return nil
	}
	require.NoError(t, ForeachVectorWindow(vec, start, length, nil, op, sels))
	return rows
}

// Row selections and windows on flat vectors of every mock type: the op sees
// exactly the selected rows inside the window, with their own values.
func TestForeachVectorWindowSelections(t *testing.T) {
	mp := mpool.MustNewZero()
	sels, err := nulls.NewWithSize(1, mp)
	require.NoError(t, err)
	defer sels.Free()
	sels.Add(2, 6)

	cases := []struct {
		name          string
		start, length int
		sels          *nulls.Nulls
		want          []int
	}{
		{"all", 0, 10, nil, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{"all selected", 0, 10, sels, []int{2, 6}},
		{"window", 2, 6, nil, []int{2, 3, 4, 5, 6, 7}},
		{"window holding both selected", 2, 6, sels, []int{2, 6}},
		{"window past the first selected", 3, 6, sels, []int{6}},
		{"window between selected", 3, 3, sels, []int{}},
		{"window starting at the last selected", 6, 4, sels, []int{6}},
		{"empty window", 5, 0, sels, []int{}},
	}
	for _, typ := range types.MockColTypes() {
		for _, nullable := range []bool{false, true} {
			vec := MockVector(typ, 10, false, nil)
			if nullable {
				UpdateValue(vec, 6, nil, true, vec.MPool())
			}
			for _, c := range cases {
				got := collectRows(t, vec, vec, 0, c.start, c.length, c.sels)
				require.Equal(t, c.want, got, "%s nullable=%v %s", typ.String(), nullable, c.name)
			}
			ReleaseVector(vec)
		}
	}
}

// On a window view, rows and selections are relative to the view.
func TestForeachVectorWindowOnView(t *testing.T) {
	mp := mpool.MustNewZero()
	sels, err := nulls.NewWithSize(1, mp)
	require.NoError(t, err)
	defer sels.Free()
	sels.Add(2, 6)

	for _, typ := range types.MockColTypes() {
		vec := MockVector(typ, 10, false, nil)
		UpdateValue(vec, 4, nil, true, vec.MPool())
		for _, bias := range []int{0, 1} {
			view, err := vec.Window(bias, bias+8)
			require.NoError(t, err)
			require.Equal(t, 8, view.Length())
			name := typ.String()
			require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, collectRows(t, view, vec, bias, 0, 8, nil), name)
			require.Equal(t, []int{2, 3, 4}, collectRows(t, view, vec, bias, 2, 3, nil), name)
			require.Equal(t, []int{2, 6}, collectRows(t, view, vec, bias, 0, 8, sels), name)
			require.Equal(t, []int{6}, collectRows(t, view, vec, bias, 3, 4, sels), name)
			require.Equal(t, []int{}, collectRows(t, view, vec, bias, 3, 3, sels), name)
		}
		ReleaseVector(vec)
	}
}

// The typed op form visits the same selected rows as the untyped one.
func TestForeachVectorWindowTypedSelections(t *testing.T) {
	mp := mpool.MustNewZero()
	sels, err := nulls.NewWithSize(1, mp)
	require.NoError(t, err)
	defer sels.Free()
	sels.Add(2, 6)

	ints := MockVector(types.T_int64.ToType(), 10, false, nil)
	defer ReleaseVector(ints)
	var intRows []int
	require.NoError(t, ForeachVectorWindow(ints, 1, 8, func(v int64, _ bool, row int) error {
		intRows = append(intRows, row)
		require.Equal(t, ints.GetValue(row), v)
		return nil
	}, nil, sels))
	require.Equal(t, []int{2, 6}, intRows)

	strs := MockVector(types.T_varchar.ToType(), 10, false, nil)
	defer ReleaseVector(strs)
	var strRows []int
	require.NoError(t, ForeachVectorWindow(strs, 3, 7, func(v []byte, _ bool, row int) error {
		strRows = append(strRows, row)
		require.Equal(t, strs.GetValue(row), v)
		return nil
	}, nil, sels))
	require.Equal(t, []int{6}, strRows)
}
