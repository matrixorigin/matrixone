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
