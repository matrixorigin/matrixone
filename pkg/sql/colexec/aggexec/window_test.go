// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestValueWindowExec_BasicOperations(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	// Test LAG function
	t.Run("LAG_basic", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		require.NotNil(t, exec)

		// Test GroupGrow
		err = exec.GroupGrow(3)
		require.NoError(t, err)

		// Test PreAllocateGroups
		err = exec.PreAllocateGroups(2)
		require.NoError(t, err)

		// Test GetOptResult
		result := exec.GetOptResult()
		require.Nil(t, result)

		// Test Size
		size := exec.Size()
		require.GreaterOrEqual(t, size, int64(0))

		// Test Free
		exec.Free()
	})

	// Test LEAD function
	t.Run("LEAD_basic", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLead, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		require.NotNil(t, exec)
		exec.Free()
	})

	// Test FIRST_VALUE function
	t.Run("FIRST_VALUE_basic", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfFirstValue, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		require.NotNil(t, exec)
		exec.Free()
	})

	// Test LAST_VALUE function
	t.Run("LAST_VALUE_basic", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLastValue, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		require.NotNil(t, exec)
		exec.Free()
	})

	// Test NTH_VALUE function
	t.Run("NTH_VALUE_basic", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfNthValue, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		require.NotNil(t, exec)
		exec.Free()
	})

	// Test with distinct (should fail)
	t.Run("distinct_not_supported", func(t *testing.T) {
		_, err := makeValueWindowExec(mp, WinIdOfLag, true, []types.Type{types.T_int64.ToType()})
		require.Error(t, err)
	})
}

func TestValueWindowExec_FillAndFlush(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("LAG_fill_and_flush_int64", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		// Create test vector with int64 values
		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{100, 200, 300}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		// Grow groups
		err = exec.GroupGrow(3)
		require.NoError(t, err)

		// Fill values for each row
		// Row 0: frame contains [100, 200, 300], current row is 0
		for k := 0; k < 3; k++ {
			err = exec.Fill(0, k, []*vector.Vector{vec})
			require.NoError(t, err)
		}
		// Row 1: frame contains [100, 200, 300], current row is 1
		for k := 0; k < 3; k++ {
			err = exec.Fill(1, k, []*vector.Vector{vec})
			require.NoError(t, err)
		}
		// Row 2: frame contains [100, 200, 300], current row is 2
		for k := 0; k < 3; k++ {
			err = exec.Fill(2, k, []*vector.Vector{vec})
			require.NoError(t, err)
		}

		// Flush and check results
		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		// LAG should return: [null, 100, 200]
		resultVec := results[0]
		require.Equal(t, 3, resultVec.Length())

		// First row should be null (no previous row)
		require.True(t, resultVec.IsNull(0))

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("LEAD_fill_and_flush_int64", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLead, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{100, 200, 300}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		for j := 0; j < 3; j++ {
			for k := 0; k < 3; k++ {
				err = exec.Fill(j, k, []*vector.Vector{vec})
				require.NoError(t, err)
			}
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 3, resultVec.Length())

		// Last row should be null (no next row)
		require.True(t, resultVec.IsNull(2))

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("FIRST_VALUE_fill_and_flush", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfFirstValue, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{100, 200, 300}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		for j := 0; j < 3; j++ {
			for k := 0; k < 3; k++ {
				err = exec.Fill(j, k, []*vector.Vector{vec})
				require.NoError(t, err)
			}
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 3, resultVec.Length())

		// All rows should have first value = 100
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		for i := 0; i < 3; i++ {
			require.Equal(t, int64(100), col[i])
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("LAST_VALUE_fill_and_flush", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLastValue, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{100, 200, 300}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		for j := 0; j < 3; j++ {
			for k := 0; k < 3; k++ {
				err = exec.Fill(j, k, []*vector.Vector{vec})
				require.NoError(t, err)
			}
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 3, resultVec.Length())

		// All rows should have last value = 300
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		for i := 0; i < 3; i++ {
			require.Equal(t, int64(300), col[i])
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("NTH_VALUE_fill_and_flush", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfNthValue, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{100, 200, 300}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		for j := 0; j < 3; j++ {
			for k := 0; k < 3; k++ {
				err = exec.Fill(j, k, []*vector.Vector{vec})
				require.NoError(t, err)
			}
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 3, resultVec.Length())

		resultVec.Free(mp)
		exec.Free()
	})
}

func TestValueWindowExec_VarlenTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("LAG_varchar", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_varchar.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_varchar.ToType())
		err = vector.AppendStringList(vec, []string{"aaa", "bbb", "ccc"}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		for j := 0; j < 3; j++ {
			for k := 0; k < 3; k++ {
				err = exec.Fill(j, k, []*vector.Vector{vec})
				require.NoError(t, err)
			}
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 3, resultVec.Length())

		// First row should be null
		require.True(t, resultVec.IsNull(0))

		// Second row should be "aaa"
		require.Equal(t, "aaa", string(resultVec.GetBytesAt(1)))

		// Third row should be "bbb"
		require.Equal(t, "bbb", string(resultVec.GetBytesAt(2)))

		resultVec.Free(mp)
		exec.Free()
	})
}

func TestValueWindowExec_NullValues(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("LAG_with_nulls", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{100, 0, 300}, []bool{false, true, false}, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		for j := 0; j < 3; j++ {
			for k := 0; k < 3; k++ {
				err = exec.Fill(j, k, []*vector.Vector{vec})
				require.NoError(t, err)
			}
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 3, resultVec.Length())

		// Row 0: LAG should be null (no previous)
		require.True(t, resultVec.IsNull(0))

		// Row 1: LAG should be 100 (previous row value)
		require.False(t, resultVec.IsNull(1))

		// Row 2: LAG should be null (previous row was null)
		require.True(t, resultVec.IsNull(2))

		resultVec.Free(mp)
		exec.Free()
	})
}

func TestValueWindowExec_EmptyFrame(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("LAG_empty_frame", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		// Grow but don't fill
		err = exec.GroupGrow(1)
		require.NoError(t, err)

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 1, resultVec.Length())
		require.True(t, resultVec.IsNull(0))

		resultVec.Free(mp)
		exec.Free()
	})
}

func TestValueWindowExec_ErrorMethods(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	// Test marshal (should return error)
	_, err = exec.(*valueWindowExec).marshal()
	require.Error(t, err)

	// Test unmarshal (should return error)
	err = exec.(*valueWindowExec).unmarshal(mp, nil, nil, nil)
	require.Error(t, err)

	// Test BulkFill (should return error)
	err = exec.BulkFill(0, nil)
	require.Error(t, err)

	// Test BatchFill (should return error)
	err = exec.BatchFill(0, nil, nil)
	require.Error(t, err)

	// Test Merge (should return error)
	err = exec.Merge(nil, 0, 0)
	require.Error(t, err)

	// Test BatchMerge (should return error)
	err = exec.BatchMerge(nil, 0, nil)
	require.Error(t, err)

	// Test SetExtraInformation (should not return error)
	err = exec.SetExtraInformation(nil, 0)
	require.NoError(t, err)

	exec.Free()
}

func TestValueWindowExec_EmptyVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	err = exec.GroupGrow(1)
	require.NoError(t, err)

	// Fill with empty vectors
	err = exec.Fill(0, 0, []*vector.Vector{})
	require.NoError(t, err)

	exec.Free()
}
