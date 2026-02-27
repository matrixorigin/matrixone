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

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
)

func TestParquetValueToGo(t *testing.T) {
	tests := []struct {
		name     string
		value    parquet.Value
		expected any
	}{
		{"null", parquet.NullValue(), nil},
		{"bool_true", parquet.BooleanValue(true), true},
		{"bool_false", parquet.BooleanValue(false), false},
		{"int32", parquet.Int32Value(42), int64(42)},
		{"int64", parquet.Int64Value(123456789), int64(123456789)},
		{"float", parquet.FloatValue(3.14), float64(float32(3.14))},
		{"double", parquet.DoubleValue(3.14159), 3.14159},
		{"string", parquet.ByteArrayValue([]byte("hello")), "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parquetValueToGo(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStringifyMapKey(t *testing.T) {
	tests := []struct {
		name     string
		value    parquet.Value
		expected string
	}{
		{"string_key", parquet.ByteArrayValue([]byte("key1")), "key1"},
		{"int_key", parquet.Int32Value(123), "123"},
		{"null_key", parquet.NullValue(), "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stringifyMapKey(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsNestedTargetTypeSupported(t *testing.T) {
	tests := []struct {
		typ      types.T
		expected bool
	}{
		{types.T_json, true},
		{types.T_text, true},
		{types.T_varchar, true},
		{types.T_char, true},
		{types.T_int32, false},
		{types.T_float64, false},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			result := isNestedTargetTypeSupported(tt.typ)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsNestedColumnNull(t *testing.T) {
	t.Run("empty_values", func(t *testing.T) {
		result := isNestedColumnNull([]parquet.Value{}, nil)
		assert.True(t, result)
	})

	t.Run("non_null_values", func(t *testing.T) {
		values := []parquet.Value{parquet.Int32Value(1)}
		result := isNestedColumnNull(values, nil)
		assert.False(t, result)
	})
}
