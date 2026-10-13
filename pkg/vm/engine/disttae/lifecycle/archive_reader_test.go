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

package lifecycle

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParquetIntegerConversionsPreserveRangeAndRejectCorruption(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
		want  int64
	}{
		{name: "int", value: int(-7), want: -7},
		{name: "int32", value: int32(-32), want: -32},
		{name: "int64", value: int64(-64), want: -64},
		{name: "uint32", value: uint32(32), want: 32},
		{name: "uint64 max int64", value: uint64(math.MaxInt64), want: math.MaxInt64},
	} {
		t.Run("signed/"+test.name, func(t *testing.T) {
			actual, err := parquetInt64(test.value)
			require.NoError(t, err)
			require.Equal(t, test.want, actual)
		})
	}
	for _, value := range []any{uint64(math.MaxInt64) + 1, "not-an-integer"} {
		_, err := parquetInt64(value)
		require.Error(t, err)
	}

	for _, test := range []struct {
		name  string
		value any
		want  uint64
	}{
		{name: "int", value: int(7), want: 7},
		{name: "int32", value: int32(32), want: 32},
		{name: "int64", value: int64(64), want: 64},
		{name: "uint32", value: uint32(32), want: 32},
		{name: "uint64", value: uint64(math.MaxUint64), want: math.MaxUint64},
	} {
		t.Run("unsigned/"+test.name, func(t *testing.T) {
			actual, err := parquetUint64(test.value)
			require.NoError(t, err)
			require.Equal(t, test.want, actual)
		})
	}
	for _, value := range []any{int(-1), int32(-1), int64(-1), "not-an-integer"} {
		_, err := parquetUint64(value)
		require.Error(t, err)
	}
}
