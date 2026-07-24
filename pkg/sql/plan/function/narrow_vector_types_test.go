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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// narrowVectorOids is the set of vector types this branch added. Every generic
// type gate that accepts vecf32/vecf64 must accept these too — the bar is parity
// with vecf32, not new capability.
var narrowVectorOids = []types.T{
	types.T_array_bf16,
	types.T_array_float16,
	types.T_array_int8,
	types.T_array_uint8,
}

// jsonConstructorSupportsType gates json_array/json_object BEFORE the value
// conversion runs, so a narrow vector column was rejected at type-check with
// "invalid argument function json_array" even once the conversion handled it.
// Both halves are needed; this asserts the gate half.
func TestJsonConstructorSupportsNarrowVectorTypes(t *testing.T) {
	require.True(t, jsonConstructorSupportsType(types.T_array_float32), "vecf32 is the parity baseline")
	require.True(t, jsonConstructorSupportsType(types.T_array_float64), "vecf64 is the parity baseline")
	for _, oid := range narrowVectorOids {
		require.True(t, jsonConstructorSupportsType(oid), "json constructor must accept %s", oid.String())
	}
	// A type that genuinely is not supported must still be rejected, so the test
	// fails if the gate is ever widened to "accept everything".
	require.False(t, jsonConstructorSupportsType(types.T_tuple))
}

// least()/greatest() resolve through this gate; narrow vectors failed with
// "invalid argument function least, bad value [VECINT8]" while vecf32 worked.
func TestLeastGreatestSupportsNarrowVectorTypes(t *testing.T) {
	require.True(t, leastGreatestExecutorSupportsOid(types.T_array_float32))
	require.True(t, leastGreatestExecutorSupportsOid(types.T_array_float64))
	for _, oid := range narrowVectorOids {
		require.True(t, leastGreatestExecutorSupportsOid(oid), "least/greatest must accept %s", oid.String())
	}
	require.False(t, leastGreatestExecutorSupportsOid(types.T_tuple))
}
