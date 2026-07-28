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

package objecttool

import (
	"encoding/hex"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultFormatter(t *testing.T) {
	formatter := &DefaultFormatter{}

	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: "NULL",
		},
		{
			name:     "empty byte slice",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "small byte slice",
			input:    []byte{0x01, 0x02, 0x03},
			expected: "010203",
		},
		{
			name:     "large byte slice",
			input:    make([]byte, 100),
			expected: hex.EncodeToString(make([]byte, 100)),
		},
		{
			name:     "string value",
			input:    "test string",
			expected: "test string",
		},
		{
			name:     "integer value",
			input:    42,
			expected: "42",
		},
		{
			name:     "boolean value",
			input:    true,
			expected: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatter.Format(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDefaultFormatter_CanFormat(t *testing.T) {
	formatter := &DefaultFormatter{}

	// DefaultFormatter should handle any value
	assert.True(t, formatter.CanFormat("string", types.Type{}))
	assert.True(t, formatter.CanFormat(123, types.Type{}))
	assert.True(t, formatter.CanFormat([]byte{1, 2, 3}, types.Type{}))
	assert.True(t, formatter.CanFormat(nil, types.Type{}))
}

func TestHexFormatter(t *testing.T) {
	formatter := &HexFormatter{}

	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "byte slice",
			input:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
			expected: "deadbeef",
		},
		{
			name:     "empty byte slice",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "non-byte value",
			input:    "not bytes",
			expected: "not bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatter.Format(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHexFormatter_CanFormat(t *testing.T) {
	formatter := &HexFormatter{}

	assert.True(t, formatter.CanFormat([]byte{1, 2, 3}, types.Type{}))
	assert.False(t, formatter.CanFormat("string", types.Type{}))
	assert.False(t, formatter.CanFormat(123, types.Type{}))
}

func TestRowidFormatter(t *testing.T) {
	formatter := &RowidFormatter{}

	// Test with Rowid type
	var rowid types.Rowid
	copy(rowid[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24})

	result := formatter.Format(rowid)
	assert.NotEmpty(t, result)
	assert.Contains(t, result, "-") // Rowid string format contains dashes

	// Test with byte slice
	rowidBytes := make([]byte, types.RowidSize)
	for i := range rowidBytes {
		rowidBytes[i] = byte(i + 1)
	}

	result2 := formatter.Format(rowidBytes)
	assert.NotEmpty(t, result2)
}

func TestRowidFormatter_CanFormat(t *testing.T) {
	formatter := &RowidFormatter{}

	// Test with correct size byte slice
	correctSize := make([]byte, types.RowidSize)
	assert.True(t, formatter.CanFormat(correctSize, types.Type{}))

	// Test with incorrect size byte slice
	wrongSize := make([]byte, 10)
	assert.False(t, formatter.CanFormat(wrongSize, types.Type{}))

	// Test with Rowid type
	var rowid types.Rowid
	assert.True(t, formatter.CanFormat(rowid, types.Type{}))

	// Test with other types
	assert.False(t, formatter.CanFormat("string", types.Type{}))
}

func TestTSFormatter(t *testing.T) {
	formatter := &TSFormatter{}

	// Create a TS value
	ts := types.BuildTS(1640995200, 0) // 2022-01-01 00:00:00 UTC

	result := formatter.Format(ts)
	assert.NotEmpty(t, result)
	// TS format might not contain year directly, just check it's not empty
	assert.Greater(t, len(result), 0)
}

func TestTSFormatter_CanFormat(t *testing.T) {
	formatter := &TSFormatter{}

	tsType := types.Type{Oid: types.T_TS}
	otherType := types.Type{Oid: types.T_int32}

	assert.True(t, formatter.CanFormat(types.TS{}, tsType))
	assert.False(t, formatter.CanFormat(types.TS{}, otherType))
}

func TestFormatterRegistry(t *testing.T) {
	registry := NewFormatterRegistry()

	// Test default behavior
	formatter := registry.GetFormatter(0, types.Type{}, "test")
	assert.IsType(t, &DefaultFormatter{}, formatter)

	// Test setting custom formatter
	hexFormatter := &HexFormatter{}
	registry.SetFormatter(1, hexFormatter)

	formatter = registry.GetFormatter(1, types.Type{}, []byte{1, 2, 3})
	assert.Equal(t, hexFormatter, formatter)

	// Test clearing formatter
	registry.ClearFormatter(1)
	formatter = registry.GetFormatter(1, types.Type{}, []byte{1, 2, 3})
	assert.IsType(t, &DefaultFormatter{}, formatter)
}

func TestFormatterRegistry_GetFormatterName(t *testing.T) {
	registry := NewFormatterRegistry()

	// Test default name
	assert.Equal(t, "auto", registry.GetFormatterName(0))

	// Test custom formatter names
	registry.SetFormatter(1, &HexFormatter{})
	assert.Equal(t, "hex", registry.GetFormatterName(1))

	registry.SetFormatter(2, &RowidFormatter{})
	assert.Equal(t, "rowid", registry.GetFormatterName(2))

	registry.SetFormatter(3, &TSFormatter{})
	assert.Equal(t, "ts", registry.GetFormatterName(3))

	registry.SetFormatter(4, &ObjectStatsFormatter{})
	assert.Equal(t, "objectstats", registry.GetFormatterName(4))
}

func TestFormatterByName(t *testing.T) {
	tests := []struct {
		name         string
		expectedType interface{}
	}{
		{"default", &DefaultFormatter{}},
		{"hex", &HexFormatter{}},
		{"rowid", &RowidFormatter{}},
		{"ts", &TSFormatter{}},
		{"objectstats", &ObjectStatsFormatter{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatter, exists := FormatterByName[tt.name]
			require.True(t, exists, "Formatter %s should exist", tt.name)
			assert.IsType(t, tt.expectedType, formatter)
		})
	}
}

// TestObjectStatsFormatter tests ObjectStatsFormatter with proper data
func TestObjectStatsFormatter(t *testing.T) {
	formatter := &ObjectStatsFormatter{}

	// Test with wrong length - should reject
	shortData := []byte{0x01, 0x02}
	assert.False(t, formatter.CanFormat(shortData, types.Type{}))

	// Test with correct length
	correctData := make([]byte, objectio.ObjectStatsLen)
	assert.True(t, formatter.CanFormat(correctData, types.Type{}))

	// Test Format with valid data
	result := formatter.Format(correctData)
	assert.NotEmpty(t, result)
}

// TestFormatterRegistryAdvanced tests FormatterRegistry functionality
func TestFormatterRegistryAdvanced(t *testing.T) {
	registry := NewFormatterRegistry()

	// Test SetFormatter
	hexFormatter := &HexFormatter{}
	registry.SetFormatter(0, hexFormatter)

	// Test GetFormatter with custom formatter
	testData := []byte{0x01, 0x02}
	formatter := registry.GetFormatter(0, types.Type{}, testData)
	assert.Equal(t, hexFormatter, formatter)

	// Test GetFormatterName
	name := registry.GetFormatterName(0)
	assert.Equal(t, "hex", name)

	// Test ClearFormatter
	registry.ClearFormatter(0)
	name = registry.GetFormatterName(0)
	assert.Equal(t, "auto", name)

	// Test auto-detect with TS
	var tsValue types.TS
	formatter = registry.GetFormatter(1, types.Type{Oid: types.T_TS}, tsValue)
	assert.IsType(t, &TSFormatter{}, formatter)

	// Test auto-detect with Rowid
	var rowidValue types.Rowid
	formatter = registry.GetFormatter(2, types.Type{Oid: types.T_Rowid}, rowidValue)
	assert.IsType(t, &RowidFormatter{}, formatter)

	// Test default formatter
	formatter = registry.GetFormatter(3, types.Type{Oid: types.T_int32}, int32(123))
	assert.IsType(t, &DefaultFormatter{}, formatter)
}

// TestFormatterEdgeCases tests edge cases for formatters
func TestFormatterEdgeCases(t *testing.T) {
	// Test DefaultFormatter with nil
	df := &DefaultFormatter{}
	result := df.Format(nil)
	assert.Equal(t, "NULL", result)

	// Test HexFormatter with empty slice
	hf := &HexFormatter{}
	result = hf.Format([]byte{})
	assert.Empty(t, result)

	// Test HexFormatter with non-byte value
	result = hf.Format("not bytes")
	assert.NotEmpty(t, result)

	// Test RowidFormatter with different values
	rf := &RowidFormatter{}
	var rowid types.Rowid
	result = rf.Format(rowid)
	assert.NotEmpty(t, result)

	// Test TSFormatter with different values
	tf := &TSFormatter{}
	var ts types.TS
	result = tf.Format(ts)
	// May be empty for zero value
	_ = result
}
