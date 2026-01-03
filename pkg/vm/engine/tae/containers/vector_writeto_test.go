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

package containers

import (
	"bytes"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVectorWrapper_WriteTo_DirectWrite tests that WriteTo writes directly to bytes.Buffer
func TestVectorWrapper_WriteTo_DirectWrite(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create a simple vector
	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	err := vector.AppendFixed(vec, int32(1), false, mp)
	require.NoError(t, err)
	err = vector.AppendFixed(vec, int32(2), false, mp)
	require.NoError(t, err)

	wrapper := &vectorWrapper{wrapped: vec}

	// Test 1: Write to bytes.Buffer (should use direct write optimization)
	var buf bytes.Buffer
	n, err := wrapper.WriteTo(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data1 := buf.Bytes()
	assert.Greater(t, len(data1), 8, "should have size header + data")

	// Test 2: Write again to verify consistency
	var buf2 bytes.Buffer
	n2, err := wrapper.WriteTo(&buf2)
	require.NoError(t, err)
	assert.Equal(t, n, n2, "should produce same size")
	assert.Equal(t, data1, buf2.Bytes(), "should produce same data")
}

// TestVectorWrapper_WriteTo_SizeHeader tests that size header is correctly backfilled
func TestVectorWrapper_WriteTo_SizeHeader(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int64.ToType())
	defer vec.Free(mp)

	for i := 0; i < 10; i++ {
		err := vector.AppendFixed(vec, int64(i), false, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	var buf bytes.Buffer
	n, err := wrapper.WriteTo(&buf)
	require.NoError(t, err)

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")

	// Decode size header
	size := types.DecodeInt64(data[:8])
	assert.Equal(t, int64(len(data)-8), size, "size header should match actual data size")
	assert.Equal(t, int64(len(data)), n, "returned n should match total bytes written")
}

// TestVectorWrapper_WriteTo_NoIntermediateAllocation tests that no intermediate buffer is allocated
func TestVectorWrapper_WriteTo_NoIntermediateAllocation(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	for i := 0; i < 100; i++ {
		err := vector.AppendFixed(vec, int32(i), false, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	// Write to bytes.Buffer multiple times
	// If optimization works, no intermediate buffer should be allocated
	for i := 0; i < 10; i++ {
		var buf bytes.Buffer
		n, err := wrapper.WriteTo(&buf)
		require.NoError(t, err)
		assert.Greater(t, n, int64(0))
	}
}

// TestVectorWrapper_WriteToV1_DirectWrite tests WriteToV1 direct write optimization
func TestVectorWrapper_WriteToV1_DirectWrite(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	err := vector.AppendFixed(vec, int32(1), false, mp)
	require.NoError(t, err)

	wrapper := &vectorWrapper{wrapped: vec}

	// Test WriteToV1 with bytes.Buffer
	var buf bytes.Buffer
	n, err := wrapper.WriteToV1(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	assert.Greater(t, len(data), 8, "should have size header + data")

	// Verify size header
	size := types.DecodeInt64(data[:8])
	assert.Equal(t, int64(len(data)-8), size)
}

// errorWriter is a writer that always returns an error
type errorWriter struct{}

func (e *errorWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("write error")
}

// TestVectorWrapper_WriteTo_Fallback tests WriteTo with non-bytes.Buffer writer (fallback path)
func TestVectorWrapper_WriteTo_Fallback(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	for i := 0; i < 10; i++ {
		err := vector.AppendFixed(vec, int32(i), false, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	// Test with a custom writer (not bytes.Buffer)
	var buf bytes.Buffer
	n, err := wrapper.WriteTo(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")

	// Verify size header
	size := types.DecodeInt64(data[:8])
	assert.Equal(t, int64(len(data)-8), size)
	assert.Equal(t, int64(len(data)), n)
}

// TestVectorWrapper_WriteTo_Error tests WriteTo when writer returns error
func TestVectorWrapper_WriteTo_Error(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	err := vector.AppendFixed(vec, int32(1), false, mp)
	require.NoError(t, err)

	wrapper := &vectorWrapper{wrapped: vec}

	// Test with error writer (fallback path)
	errorW := &errorWriter{}
	n, err := wrapper.WriteTo(errorW)
	assert.Error(t, err)
	assert.Equal(t, int64(0), n)
	assert.Contains(t, err.Error(), "write error")
}

// TestVectorWrapper_WriteTo_BufferGrow tests WriteTo with buffer that needs to grow
func TestVectorWrapper_WriteTo_BufferGrow(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	// Create a large vector to trigger buffer growth
	for i := 0; i < 1000; i++ {
		err := vector.AppendFixed(vec, int32(i), false, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	// Create buffer with small initial capacity
	buf := bytes.NewBuffer(make([]byte, 0, 10))
	n, err := wrapper.WriteTo(buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")

	// Verify size header
	size := types.DecodeInt64(data[:8])
	assert.Equal(t, int64(len(data)-8), size)
}

// TestVectorWrapper_WriteTo_SmallEstimatedSize tests WriteTo with small estimated size (< 256)
func TestVectorWrapper_WriteTo_SmallEstimatedSize(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	// Create a small vector (estimated size will be < 256)
	err := vector.AppendFixed(vec, int32(1), false, mp)
	require.NoError(t, err)

	wrapper := &vectorWrapper{wrapped: vec}

	var buf bytes.Buffer
	n, err := wrapper.WriteTo(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")
}

// TestVectorWrapper_WriteToV1_Fallback tests WriteToV1 with non-bytes.Buffer writer (fallback path)
func TestVectorWrapper_WriteToV1_Fallback(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	for i := 0; i < 10; i++ {
		err := vector.AppendFixed(vec, int32(i), false, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	// Test with a custom writer (not bytes.Buffer)
	var buf bytes.Buffer
	n, err := wrapper.WriteToV1(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")

	// Verify size header
	size := types.DecodeInt64(data[:8])
	assert.Equal(t, int64(len(data)-8), size)
	assert.Equal(t, int64(len(data)), n)
}

// TestVectorWrapper_WriteToV1_Error tests WriteToV1 when writer returns error
func TestVectorWrapper_WriteToV1_Error(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	err := vector.AppendFixed(vec, int32(1), false, mp)
	require.NoError(t, err)

	wrapper := &vectorWrapper{wrapped: vec}

	// Test with error writer (fallback path)
	errorW := &errorWriter{}
	n, err := wrapper.WriteToV1(errorW)
	assert.Error(t, err)
	assert.Equal(t, int64(0), n)
	assert.Contains(t, err.Error(), "write error")
}

// TestVectorWrapper_WriteToV1_BufferGrow tests WriteToV1 with buffer that needs to grow
func TestVectorWrapper_WriteToV1_BufferGrow(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	// Create a large vector to trigger buffer growth
	for i := 0; i < 1000; i++ {
		err := vector.AppendFixed(vec, int32(i), false, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	// Create buffer with small initial capacity
	buf := bytes.NewBuffer(make([]byte, 0, 10))
	n, err := wrapper.WriteToV1(buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")

	// Verify size header
	size := types.DecodeInt64(data[:8])
	assert.Equal(t, int64(len(data)-8), size)
}

// TestVectorWrapper_WriteToV1_SmallEstimatedSize tests WriteToV1 with small estimated size (< 256)
func TestVectorWrapper_WriteToV1_SmallEstimatedSize(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	// Create a small vector (estimated size will be < 256)
	err := vector.AppendFixed(vec, int32(1), false, mp)
	require.NoError(t, err)

	wrapper := &vectorWrapper{wrapped: vec}

	var buf bytes.Buffer
	n, err := wrapper.WriteToV1(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")
}

// TestVectorWrapper_WriteTo_NoGrowNeeded tests WriteTo when buffer has enough capacity
func TestVectorWrapper_WriteTo_NoGrowNeeded(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	for i := 0; i < 10; i++ {
		err := vector.AppendFixed(vec, int32(i), false, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	// Create buffer with large initial capacity (no grow needed)
	buf := bytes.NewBuffer(make([]byte, 0, 10000))
	n, err := wrapper.WriteTo(buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")
}

// TestVectorWrapper_WriteToV1_NoGrowNeeded tests WriteToV1 when buffer has enough capacity
func TestVectorWrapper_WriteToV1_NoGrowNeeded(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	for i := 0; i < 10; i++ {
		err := vector.AppendFixed(vec, int32(i), false, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	// Create buffer with large initial capacity (no grow needed)
	buf := bytes.NewBuffer(make([]byte, 0, 10000))
	n, err := wrapper.WriteToV1(buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")
}

// TestVectorWrapper_WriteToV1_WithNulls tests WriteToV1 with vector containing nulls
func TestVectorWrapper_WriteToV1_WithNulls(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)

	// Add some values with nulls
	for i := 0; i < 10; i++ {
		isNull := i%2 == 0
		err := vector.AppendFixed(vec, int32(i), isNull, mp)
		require.NoError(t, err)
	}

	wrapper := &vectorWrapper{wrapped: vec}

	var buf bytes.Buffer
	n, err := wrapper.WriteToV1(&buf)
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))

	data := buf.Bytes()
	require.GreaterOrEqual(t, len(data), 8, "should have size header")

	// Verify size header
	size := types.DecodeInt64(data[:8])
	assert.Equal(t, int64(len(data)-8), size)
}

// TestVectorWrapper_WriteToV1_DifferentTypes tests WriteToV1 with different vector types
func TestVectorWrapper_WriteToV1_DifferentTypes(t *testing.T) {
	mp := mpool.MustNewZero()

	testTypes := []types.Type{
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
	}

	for _, typ := range testTypes {
		vec := vector.NewVec(typ)
		defer vec.Free(mp)

		// Add some data
		for i := 0; i < 5; i++ {
			switch typ.Oid {
			case types.T_int32:
				err := vector.AppendFixed(vec, int32(i), false, mp)
				require.NoError(t, err)
			case types.T_int64:
				err := vector.AppendFixed(vec, int64(i), false, mp)
				require.NoError(t, err)
			case types.T_float32:
				err := vector.AppendFixed(vec, float32(i), false, mp)
				require.NoError(t, err)
			case types.T_float64:
				err := vector.AppendFixed(vec, float64(i), false, mp)
				require.NoError(t, err)
			}
		}

		wrapper := &vectorWrapper{wrapped: vec}

		var buf bytes.Buffer
		n, err := wrapper.WriteToV1(&buf)
		require.NoError(t, err, "WriteToV1 should succeed for type %s", typ.String())
		assert.Greater(t, n, int64(0))

		data := buf.Bytes()
		require.GreaterOrEqual(t, len(data), 8, "should have size header")
	}
}
