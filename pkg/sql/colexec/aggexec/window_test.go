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
	"bytes"
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

// TestValueWindowExec_FillWithoutGroupGrow tests Fill when frameValues needs to be extended
func TestValueWindowExec_FillWithoutGroupGrow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{100, 200}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	// Don't call GroupGrow, let Fill extend frameValues automatically
	// This covers the branch: for len(exec.frameValues) <= groupIndex
	err = exec.Fill(0, 0, []*vector.Vector{vec})
	require.NoError(t, err)

	err = exec.Fill(1, 0, []*vector.Vector{vec})
	require.NoError(t, err)

	exec.Free()
}

// TestValueWindowExec_SaveIntermediateResult tests the error methods
func TestValueWindowExec_SaveIntermediateResult(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	vExec := exec.(*valueWindowExec)

	// Test SaveIntermediateResult (should return error)
	err = vExec.SaveIntermediateResult(0, nil, nil)
	require.Error(t, err)

	// Test SaveIntermediateResultOfChunk (should return error)
	err = vExec.SaveIntermediateResultOfChunk(0, nil)
	require.Error(t, err)

	// Test UnmarshalFromReader (should return error)
	err = vExec.UnmarshalFromReader(nil, mp)
	require.Error(t, err)

	exec.Free()
}

// TestValueWindowExec_SizeWithData tests Size method with actual data
func TestValueWindowExec_SizeWithData(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{100, 200, 300}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(2)
	require.NoError(t, err)

	// Fill some data
	for k := 0; k < 3; k++ {
		err = exec.Fill(0, k, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	// Size should be > 0 now
	size := exec.Size()
	require.Greater(t, size, int64(0))

	exec.Free()
}

// TestValueWindowExec_FreeWithResultVec tests Free when resultVec is set
func TestValueWindowExec_FreeWithResultVec(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
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

	// Flush to create resultVec
	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)

	// Set resultVec manually to test Free branch
	vExec := exec.(*valueWindowExec)
	vExec.resultVec = results[0]

	// Free should clean up resultVec
	exec.Free()
}

// TestValueWindowExec_VariousTypes tests different data types for appendValueToVector
func TestValueWindowExec_VariousTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	testCases := []struct {
		name   string
		typ    types.Type
		values interface{}
		nulls  []bool
	}{
		{"bool", types.T_bool.ToType(), []bool{true, false, true}, nil},
		{"int8", types.T_int8.ToType(), []int8{1, 2, 3}, nil},
		{"int16", types.T_int16.ToType(), []int16{100, 200, 300}, nil},
		{"int32", types.T_int32.ToType(), []int32{1000, 2000, 3000}, nil},
		{"uint8", types.T_uint8.ToType(), []uint8{1, 2, 3}, nil},
		{"uint16", types.T_uint16.ToType(), []uint16{100, 200, 300}, nil},
		{"uint32", types.T_uint32.ToType(), []uint32{1000, 2000, 3000}, nil},
		{"uint64", types.T_uint64.ToType(), []uint64{10000, 20000, 30000}, nil},
		{"float32", types.T_float32.ToType(), []float32{1.1, 2.2, 3.3}, nil},
		{"float64", types.T_float64.ToType(), []float64{1.11, 2.22, 3.33}, nil},
		{"date", types.T_date.ToType(), []types.Date{1, 2, 3}, nil},
		{"datetime", types.T_datetime.ToType(), []types.Datetime{1000, 2000, 3000}, nil},
		{"decimal64", types.T_decimal64.ToType(), []types.Decimal64{100, 200, 300}, nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{tc.typ})
			require.NoError(t, err)

			vec := vector.NewVec(tc.typ)
			switch v := tc.values.(type) {
			case []bool:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []int8:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []int16:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []int32:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []uint8:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []uint16:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []uint32:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []uint64:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []float32:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []float64:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []types.Date:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []types.Datetime:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			case []types.Decimal64:
				err = vector.AppendFixedList(vec, v, tc.nulls, mp)
			}
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
}

// TestValueWindowExec_LEADVariousTypes tests LEAD with various types
func TestValueWindowExec_LEADVariousTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	// Test with varchar for LEAD
	t.Run("LEAD_varchar", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLead, false, []types.Type{types.T_varchar.ToType()})
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

		// Last row should be null
		require.True(t, resultVec.IsNull(2))

		resultVec.Free(mp)
		exec.Free()
	})
}

// TestValueWindowExec_FIRST_LAST_VALUE_WithNulls tests FIRST_VALUE and LAST_VALUE with null values
func TestValueWindowExec_FIRST_LAST_VALUE_WithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("FIRST_VALUE_with_null_first", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfFirstValue, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		// First value is null
		err = vector.AppendFixedList(vec, []int64{0, 200, 300}, []bool{true, false, false}, mp)
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

		// All rows should have null as first value
		for i := 0; i < 3; i++ {
			require.True(t, resultVec.IsNull(uint64(i)))
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("LAST_VALUE_with_null_last", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLastValue, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		// Last value is null
		err = vector.AppendFixedList(vec, []int64{100, 200, 0}, []bool{false, false, true}, mp)
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

		// All rows should have null as last value
		for i := 0; i < 3; i++ {
			require.True(t, resultVec.IsNull(uint64(i)))
		}

		resultVec.Free(mp)
		exec.Free()
	})
}

// TestValueWindowExec_EmptyFrameAllFunctions tests empty frame for all window functions
func TestValueWindowExec_EmptyFrameAllFunctions(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	funcs := []struct {
		name string
		id   int64
	}{
		{"LAG", WinIdOfLag},
		{"LEAD", WinIdOfLead},
		{"FIRST_VALUE", WinIdOfFirstValue},
		{"LAST_VALUE", WinIdOfLastValue},
		{"NTH_VALUE", WinIdOfNthValue},
	}

	for _, f := range funcs {
		t.Run(f.name+"_empty_frame", func(t *testing.T) {
			exec, err := makeValueWindowExec(mp, f.id, false, []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)

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
}

// TestValueWindowExec_MoreTypes tests additional data types for appendValueToVector
func TestValueWindowExec_MoreTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	// Test bit type
	t.Run("bit", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_bit.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_bit.ToType())
		err = vector.AppendFixedList(vec, []uint64{1, 2, 3}, nil, mp)
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
		results[0].Free(mp)
		exec.Free()
	})

	// Test time type
	t.Run("time", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_time.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_time.ToType())
		err = vector.AppendFixedList(vec, []types.Time{1000, 2000, 3000}, nil, mp)
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
		results[0].Free(mp)
		exec.Free()
	})

	// Test timestamp type
	t.Run("timestamp", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_timestamp.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_timestamp.ToType())
		err = vector.AppendFixedList(vec, []types.Timestamp{1000, 2000, 3000}, nil, mp)
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
		results[0].Free(mp)
		exec.Free()
	})

	// Test decimal128 type
	t.Run("decimal128", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_decimal128.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_decimal128.ToType())
		err = vector.AppendFixedList(vec, []types.Decimal128{
			{B0_63: 100, B64_127: 0},
			{B0_63: 200, B64_127: 0},
			{B0_63: 300, B64_127: 0},
		}, nil, mp)
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
		results[0].Free(mp)
		exec.Free()
	})

	// Test uuid type
	t.Run("uuid", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_uuid.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_uuid.ToType())
		err = vector.AppendFixedList(vec, []types.Uuid{
			{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
			{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18},
		}, nil, mp)
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
		results[0].Free(mp)
		exec.Free()
	})

	// Test enum type
	t.Run("enum", func(t *testing.T) {
		exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_enum.ToType()})
		require.NoError(t, err)

		vec := vector.NewVec(types.T_enum.ToType())
		err = vector.AppendFixedList(vec, []types.Enum{1, 2, 3}, nil, mp)
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
		results[0].Free(mp)
		exec.Free()
	})
}

// TestValueWindowExec_InvalidAggID tests Flush with invalid aggID
func TestValueWindowExec_InvalidAggID(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	// Create a valueWindowExec with invalid aggID
	info := singleAggInfo{
		aggID:     -999, // Invalid ID
		distinct:  false,
		argType:   types.T_int64.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: true,
	}
	exec := &valueWindowExec{
		singleAggInfo:      info,
		mp:                 mp,
		frameValues:        make([][]*valueEntry, 0),
		currentRowPosition: make([]int, 0),
	}

	err := exec.GroupGrow(1)
	require.NoError(t, err)

	// Flush should return error for invalid aggID
	_, err = exec.Flush()
	require.Error(t, err)

	exec.Free()
}

// TestValueWindowExec_RowidType tests Rowid type
func TestValueWindowExec_RowidType(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_Rowid.ToType()})
	require.NoError(t, err)

	vec := vector.NewVec(types.T_Rowid.ToType())
	err = vector.AppendFixedList(vec, []types.Rowid{
		{1, 2, 3, 4, 5, 6},
		{2, 3, 4, 5, 6, 7},
		{3, 4, 5, 6, 7, 8},
	}, nil, mp)
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
	results[0].Free(mp)
	exec.Free()
}

// TestValueWindowExec_BlockidType tests Blockid type
func TestValueWindowExec_BlockidType(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_Blockid.ToType()})
	require.NoError(t, err)

	vec := vector.NewVec(types.T_Blockid.ToType())
	err = vector.AppendFixedList(vec, []types.Blockid{
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21},
		{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
	}, nil, mp)
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
	results[0].Free(mp)
	exec.Free()
}

// TestNtileWindowExec tests NTILE window function
func TestNtileWindowExec(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("NTILE_basic_10_rows_3_buckets", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		// Simulate window operator: one partition with 10 rows
		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		err = exec.GroupGrow(10)
		require.NoError(t, err)

		// Fill with groupIndex=0, row=0..10 (indices into os array)
		for o := 0; o <= 10; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 10, resultVec.Length())

		// Expected: 1,1,1,1,2,2,2,3,3,3 (4+3+3 distribution)
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		expected := []int64{1, 1, 1, 1, 2, 2, 2, 3, 3, 3}
		for i := 0; i < 10; i++ {
			require.Equal(t, expected[i], col[i], "row %d", i)
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("NTILE_9_rows_3_buckets", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{3, 3, 3, 3, 3, 3, 3, 3, 3}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		err = exec.GroupGrow(9)
		require.NoError(t, err)

		for o := 0; o <= 9; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 9, resultVec.Length())

		// Expected: 1,1,1,2,2,2,3,3,3 (3+3+3 distribution)
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		expected := []int64{1, 1, 1, 2, 2, 2, 3, 3, 3}
		for i := 0; i < 9; i++ {
			require.Equal(t, expected[i], col[i], "row %d", i)
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("NTILE_5_rows_3_buckets", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{3, 3, 3, 3, 3}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		err = exec.GroupGrow(5)
		require.NoError(t, err)

		for o := 0; o <= 5; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 5, resultVec.Length())

		// Expected: 1,1,2,2,3 (2+2+1 distribution)
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		expected := []int64{1, 1, 2, 2, 3}
		for i := 0; i < 5; i++ {
			require.Equal(t, expected[i], col[i], "row %d", i)
		}

		resultVec.Free(mp)
		exec.Free()
	})
}

// TestValueWindowExec_TSType tests TS type
func TestValueWindowExec_TSType(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_TS.ToType()})
	require.NoError(t, err)

	vec := vector.NewVec(types.T_TS.ToType())
	err = vector.AppendFixedList(vec, []types.TS{
		types.BuildTS(1, 1),
		types.BuildTS(2, 2),
		types.BuildTS(3, 3),
	}, nil, mp)
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
	results[0].Free(mp)
	exec.Free()
}

// TestNtileExec_ParameterValidation tests parameter validation for makeNtileExec
func TestNtileExec_ParameterValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("distinct_not_supported", func(t *testing.T) {
		_, err := makeNtileExec(mp, WinIdOfNtile, true, []types.Type{types.T_int64.ToType()})
		require.Error(t, err)
		require.Contains(t, err.Error(), "distinct")
	})

	t.Run("wrong_param_count", func(t *testing.T) {
		_, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "exactly one argument")
	})

	t.Run("valid_creation", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		require.NotNil(t, exec)
		exec.Free()
	})
}

// TestNtileExec_IntegerTypes tests Fill with various integer types
func TestNtileExec_IntegerTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	testCases := []struct {
		name     string
		typ      types.Type
		values   interface{}
		expected int64
	}{
		{"int8", types.T_int8.ToType(), []int8{5}, 5},
		{"int16", types.T_int16.ToType(), []int16{5}, 5},
		{"int32", types.T_int32.ToType(), []int32{5}, 5},
		{"int64", types.T_int64.ToType(), []int64{5}, 5},
		{"uint8", types.T_uint8.ToType(), []uint8{5}, 5},
		{"uint16", types.T_uint16.ToType(), []uint16{5}, 5},
		{"uint32", types.T_uint32.ToType(), []uint32{5}, 5},
		{"uint64", types.T_uint64.ToType(), []uint64{5}, 5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{tc.typ})
			require.NoError(t, err)

			osVec := vector.NewVec(types.T_int64.ToType())
			err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6}, nil, mp)
			require.NoError(t, err)
			defer osVec.Free(mp)

			bucketVec := vector.NewVec(tc.typ)
			switch v := tc.values.(type) {
			case []int8:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []int16:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []int32:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []int64:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []uint8:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []uint16:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []uint32:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []uint64:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			}
			require.NoError(t, err)
			defer bucketVec.Free(mp)

			err = exec.GroupGrow(5)
			require.NoError(t, err)

			for o := 0; o <= 5; o++ {
				err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
				require.NoError(t, err)
			}

			ntileExec := exec.(*ntileWindowExec)
			require.Equal(t, tc.expected, ntileExec.bucketCounts[0])

			exec.Free()
		})
	}
}

// TestNtileExec_BoundaryConditions tests boundary conditions
func TestNtileExec_BoundaryConditions(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("negative_bucket_count", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{-1}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		err = exec.Fill(0, 0, []*vector.Vector{osVec, bucketVec})
		require.Error(t, err)
		require.Contains(t, err.Error(), "positive")

		exec.Free()
	})

	t.Run("zero_bucket_count", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{0}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		err = exec.Fill(0, 0, []*vector.Vector{osVec, bucketVec})
		require.Error(t, err)

		exec.Free()
	})

	t.Run("null_bucket_defaults_to_1", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{0}, []bool{true}, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		for o := 0; o <= 3; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		ntileExec := exec.(*ntileWindowExec)
		require.Equal(t, int64(1), ntileExec.bucketCounts[0])

		exec.Free()
	})

	t.Run("empty_vectors", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		err = exec.Fill(0, 0, []*vector.Vector{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires vectors")

		exec.Free()
	})

	t.Run("only_os_vector", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		err = exec.GroupGrow(2)
		require.NoError(t, err)

		for o := 0; o <= 2; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec})
			require.NoError(t, err)
		}

		ntileExec := exec.(*ntileWindowExec)
		require.Equal(t, int64(1), ntileExec.bucketCounts[0])

		exec.Free()
	})
}

// TestNtileExec_AlgorithmEdgeCases tests flushNtile algorithm edge cases
func TestNtileExec_AlgorithmEdgeCases(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("1_row_1_bucket", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{1}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		for o := 0; o <= 1; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 1, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		require.Equal(t, int64(1), col[0])

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("1_row_multiple_buckets", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{5}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		for o := 0; o <= 1; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 1, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		require.Equal(t, int64(1), col[0])

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("multiple_rows_1_bucket", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{1}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(4)
		require.NoError(t, err)

		for o := 0; o <= 4; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 4, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		for i := 0; i < 4; i++ {
			require.Equal(t, int64(1), col[i])
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("even_distribution", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{3}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(6)
		require.NoError(t, err)

		for o := 0; o <= 6; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 6, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		expected := []int64{1, 1, 2, 2, 3, 3}
		for i := 0; i < 6; i++ {
			require.Equal(t, expected[i], col[i], "row %d", i)
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("empty_group", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.GroupGrow(2)
		require.NoError(t, err)

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 2, resultVec.Length())

		resultVec.Free(mp)
		exec.Free()
	})
}

// TestNtileExec_MultiplePartitions tests multiple groups with different configurations
func TestNtileExec_MultiplePartitions(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("different_bucket_counts", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucket2Vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucket2Vec, []int64{2}, nil, mp)
		require.NoError(t, err)
		defer bucket2Vec.Free(mp)

		bucket3Vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucket3Vec, []int64{3}, nil, mp)
		require.NoError(t, err)
		defer bucket3Vec.Free(mp)

		err = exec.GroupGrow(6)
		require.NoError(t, err)

		// Group 0: 3 rows, 2 buckets
		for o := 0; o <= 3; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucket2Vec})
			require.NoError(t, err)
		}

		// Group 1: 3 rows, 3 buckets
		for o := 0; o <= 3; o++ {
			err = exec.Fill(1, o, []*vector.Vector{osVec, bucket3Vec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 6, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)

		// Group 0: 1,1,2
		require.Equal(t, int64(1), col[0])
		require.Equal(t, int64(1), col[1])
		require.Equal(t, int64(2), col[2])

		// Group 1: 1,2,3
		require.Equal(t, int64(1), col[3])
		require.Equal(t, int64(2), col[4])
		require.Equal(t, int64(3), col[5])

		resultVec.Free(mp)
		exec.Free()
	})
}

// TestNtileExec_Serialization tests marshal/unmarshal
func TestNtileExec_Serialization(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("marshal_unmarshal_roundtrip", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{2}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(2)
		require.NoError(t, err)

		for o := 0; o <= 4; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		data, err := exec.(*ntileWindowExec).marshal()
		require.NoError(t, err)
		require.NotNil(t, data)

		// Create exec2 with same group count to match unmarshal logic
		exec2, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec2.GroupGrow(2)
		require.NoError(t, err)

		var encoded EncodedAgg
		err = encoded.Unmarshal(data)
		require.NoError(t, err)

		err = exec2.(*ntileWindowExec).unmarshal(mp, encoded.Result, encoded.Empties, encoded.Groups)
		require.NoError(t, err)

		// Verify bucketCounts were restored
		ntileExec2 := exec2.(*ntileWindowExec)
		require.Equal(t, int64(2), ntileExec2.bucketCounts[0])

		exec.Free()
		exec2.Free()
	})

	t.Run("marshal_with_empty_groups", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.GroupGrow(2)
		require.NoError(t, err)

		data, err := exec.(*ntileWindowExec).marshal()
		require.NoError(t, err)
		require.NotNil(t, data)

		exec.Free()
	})
}

// TestNtileExec_MemoryManagement tests memory-related methods
func TestNtileExec_MemoryManagement(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("size_calculation", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		initialSize := exec.Size()
		require.GreaterOrEqual(t, initialSize, int64(0))

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{2}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(2)
		require.NoError(t, err)

		for o := 0; o <= 3; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		sizeWithData := exec.Size()
		require.Greater(t, sizeWithData, initialSize)

		exec.Free()
	})

	t.Run("group_grow", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.GroupGrow(5)
		require.NoError(t, err)

		ntileExec := exec.(*ntileWindowExec)
		require.Len(t, ntileExec.groups, 5)
		require.Len(t, ntileExec.bucketCounts, 5)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		require.Len(t, ntileExec.groups, 8)
		require.Len(t, ntileExec.bucketCounts, 8)

		exec.Free()
	})

	t.Run("pre_allocate_groups", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.PreAllocateGroups(10)
		require.NoError(t, err)

		exec.Free()
	})

	t.Run("get_opt_result", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		result := exec.GetOptResult()
		require.NotNil(t, result)

		exec.Free()
	})
}

// TestNtileExec_ErrorMethods tests methods that should panic or return errors
func TestNtileExec_ErrorMethods(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	ntileExec := exec.(*ntileWindowExec)

	t.Run("bulk_fill_panics", func(t *testing.T) {
		require.Panics(t, func() {
			_ = ntileExec.BulkFill(0, nil)
		})
	})

	t.Run("set_extra_information_panics", func(t *testing.T) {
		require.Panics(t, func() {
			_ = ntileExec.SetExtraInformation(nil, 0)
		})
	})

	t.Run("batch_fill_returns_nil", func(t *testing.T) {
		err := ntileExec.BatchFill(0, nil, nil)
		require.NoError(t, err)
	})

	t.Run("merge_returns_nil", func(t *testing.T) {
		err := ntileExec.Merge(nil, 0, 0)
		require.NoError(t, err)
	})

	t.Run("batch_merge_returns_nil", func(t *testing.T) {
		err := ntileExec.BatchMerge(nil, 0, nil)
		require.NoError(t, err)
	})

	exec.Free()
}

// TestNtileExec_InvalidBucketType tests Fill with invalid bucket type
func TestNtileExec_InvalidBucketType(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)

	osVec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer osVec.Free(mp)

	bucketVec := vector.NewVec(types.T_varchar.ToType())
	err = vector.AppendStringList(bucketVec, []string{"invalid"}, nil, mp)
	require.NoError(t, err)
	defer bucketVec.Free(mp)

	err = exec.GroupGrow(1)
	require.NoError(t, err)

	err = exec.Fill(0, 0, []*vector.Vector{osVec, bucketVec})
	require.Error(t, err)
	require.Contains(t, err.Error(), "integer type")

	exec.Free()
}

// TestCumeDistWindowExec_BasicOperations tests basic operations of cumeDistWindowExec
func TestCumeDistWindowExec_BasicOperations(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)
	require.NotNil(t, exec)

	// Test PreAllocateGroups
	err = exec.PreAllocateGroups(5)
	require.NoError(t, err)

	// Test GetOptResult
	result := exec.GetOptResult()
	require.NotNil(t, result)

	// Test Size
	size := exec.Size()
	require.GreaterOrEqual(t, size, int64(0))

	exec.Free()
}

// TestCumeDistWindowExec_FillAndFlush tests Fill and Flush operations
func TestCumeDistWindowExec_FillAndFlush(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2, 3}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(4)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)

	resultVec := results[0]
	require.Equal(t, 4, resultVec.Length())

	resultVec.Free(mp)
	exec.Free()
}

// TestCumeDistWindowExec_Merge tests Merge operation
func TestCumeDistWindowExec_Merge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec1, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec1.GroupGrow(2)
	require.NoError(t, err)
	err = exec2.GroupGrow(2)
	require.NoError(t, err)

	err = exec1.Fill(0, 0, []*vector.Vector{vec})
	require.NoError(t, err)
	err = exec2.Fill(0, 0, []*vector.Vector{vec})
	require.NoError(t, err)

	err = exec1.Merge(exec2, 0, 0)
	require.NoError(t, err)

	exec1.Free()
	exec2.Free()
}

// TestCumeDistWindowExec_BatchMerge tests BatchMerge operation
func TestCumeDistWindowExec_BatchMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec1, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec1.GroupGrow(3)
	require.NoError(t, err)
	err = exec2.GroupGrow(3)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = exec1.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec2.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	groups := []uint64{1, 2, 3}
	err = exec1.BatchMerge(exec2, 0, groups)
	require.NoError(t, err)

	exec1.Free()
	exec2.Free()
}

// TestCumeDistWindowExec_BatchMergeWithNotMatched tests BatchMerge with GroupNotMatched
func TestCumeDistWindowExec_BatchMergeWithNotMatched(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec1, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec1.GroupGrow(3)
	require.NoError(t, err)
	err = exec2.GroupGrow(3)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = exec1.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec2.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	groups := []uint64{GroupNotMatched, 2, GroupNotMatched}
	err = exec1.BatchMerge(exec2, 0, groups)
	require.NoError(t, err)

	exec1.Free()
	exec2.Free()
}

// TestCumeDistWindowExec_PanicMethods tests methods that should panic
func TestCumeDistWindowExec_PanicMethods(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	cExec := exec.(*cumeDistWindowExec)

	// Test BulkFill (should panic)
	require.Panics(t, func() {
		_ = cExec.BulkFill(0, nil)
	})

	// Test BatchFill (should panic)
	require.Panics(t, func() {
		_ = cExec.BatchFill(0, nil, nil)
	})

	// Test SetExtraInformation (should panic)
	require.Panics(t, func() {
		_ = cExec.SetExtraInformation(nil, 0)
	})

	exec.Free()
}

// TestCumeDistWindowExec_MarshalUnmarshal tests marshal and unmarshal operations
func TestCumeDistWindowExec_MarshalUnmarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(3)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)

	// Test marshal
	data, err := cExec.marshal()
	require.NoError(t, err)
	require.NotNil(t, data)

	// Test unmarshal
	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)
	cExec2 := exec2.(*cumeDistWindowExec)

	err = cExec2.unmarshal(mp, nil, nil, nil)
	require.NoError(t, err)

	exec.Free()
	exec2.Free()
}

// TestCumeDistWindowExec_SaveIntermediateResult tests SaveIntermediateResult
func TestCumeDistWindowExec_SaveIntermediateResult(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(2)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)

	// Test SaveIntermediateResult
	var buf bytes.Buffer
	flags := [][]uint8{{1, 1}}
	err = cExec.SaveIntermediateResult(2, flags, &buf)
	require.NoError(t, err)

	exec.Free()
}

// TestCumeDistWindowExec_SaveIntermediateResultOfChunk tests SaveIntermediateResultOfChunk
func TestCumeDistWindowExec_SaveIntermediateResultOfChunk(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(2)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)

	// Test SaveIntermediateResultOfChunk
	var buf bytes.Buffer
	err = cExec.SaveIntermediateResultOfChunk(0, &buf)
	require.NoError(t, err)

	exec.Free()
}

// TestCumeDistWindowExec_UnmarshalFromReader tests UnmarshalFromReader
func TestCumeDistWindowExec_UnmarshalFromReader(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(2)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)

	// Save to buffer
	var buf bytes.Buffer
	flags := [][]uint8{{1, 1}}
	err = cExec.SaveIntermediateResult(2, flags, &buf)
	require.NoError(t, err)

	// Create new exec and unmarshal
	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)
	cExec2 := exec2.(*cumeDistWindowExec)

	err = cExec2.UnmarshalFromReader(&buf, mp)
	require.NoError(t, err)

	exec.Free()
	exec2.Free()
}

// TestCumeDistWindowExec_SizeWithData tests Size with actual data
func TestCumeDistWindowExec_SizeWithData(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2, 3, 4}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(5)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)
	size := cExec.Size()
	require.Greater(t, size, int64(0))

	exec.Free()
}
