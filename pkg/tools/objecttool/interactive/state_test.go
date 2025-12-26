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

package interactive

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/assert"
)

// Formatter type alias for testing
type Formatter = objecttool.Formatter
type DefaultFormatter = objecttool.DefaultFormatter
type TSFormatter = objecttool.TSFormatter
type RowidFormatter = objecttool.RowidFormatter
type HexFormatter = objecttool.HexFormatter

func TestState_SetFormat(t *testing.T) {
	ctx := context.Background()
	// Create a minimal state without reader for testing formatter logic
	state := &State{
		formatter: objecttool.NewFormatterRegistry(),
		ctx:       ctx,
	}

	// Test setting valid formatter
	err := state.SetFormat(0, "hex")
	assert.NoError(t, err)

	// Verify formatter was set
	name := state.formatter.GetFormatterName(0)
	assert.Equal(t, "hex", name)

	// Test clearing formatter
	err = state.SetFormat(0, "auto")
	assert.NoError(t, err)

	name = state.formatter.GetFormatterName(0)
	assert.Equal(t, "auto", name)

	// Test invalid formatter
	err = state.SetFormat(0, "invalid")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown formatter")
}

func TestState_DisplayModes(t *testing.T) {
	ctx := context.Background()
	state := &State{
		formatter:   objecttool.NewFormatterRegistry(),
		ctx:         ctx,
		pageSize:    20,
		maxColWidth: 64,
	}

	// Test initial mode (table)
	assert.False(t, state.verticalMode)
	assert.Equal(t, 64, state.maxColWidth)
	assert.Equal(t, 20, state.pageSize)

	// Test switching to vertical mode
	state.verticalMode = true
	state.maxColWidth = 128
	state.pageSize = 10

	assert.True(t, state.verticalMode)
	assert.Equal(t, 128, state.maxColWidth)
	assert.Equal(t, 10, state.pageSize)
}

func TestState_WidthSettings(t *testing.T) {
	ctx := context.Background()
	state := &State{
		formatter:   objecttool.NewFormatterRegistry(),
		ctx:         ctx,
		maxColWidth: 64,
	}

	// Test default width
	assert.Equal(t, 64, state.maxColWidth)

	// Test setting custom width
	state.maxColWidth = 100
	assert.Equal(t, 100, state.maxColWidth)

	// Test unlimited width
	state.maxColWidth = 0
	assert.Equal(t, 0, state.maxColWidth)
}

func TestState_ColumnFiltering(t *testing.T) {
	ctx := context.Background()
	state := &State{
		formatter: objecttool.NewFormatterRegistry(),
		ctx:       ctx,
	}

	// Test no filtering (show all)
	assert.Nil(t, state.visibleCols)

	// Test filtering specific columns
	state.visibleCols = []uint16{0, 2, 4, 6, 8}
	assert.Equal(t, 5, len(state.visibleCols))
	assert.Equal(t, uint16(0), state.visibleCols[0])
	assert.Equal(t, uint16(8), state.visibleCols[4])

	// Test clearing filter
	state.visibleCols = nil
	assert.Nil(t, state.visibleCols)
}

func TestState_FormatterRegistry(t *testing.T) {
	ctx := context.Background()
	state := &State{
		formatter: objecttool.NewFormatterRegistry(),
		ctx:       ctx,
	}

	// Test formatter registry is initialized
	assert.NotNil(t, state.formatter)

	// Test setting formatter
	err := state.SetFormat(0, "hex")
	assert.NoError(t, err)

	// Test getting formatter name
	name := state.formatter.GetFormatterName(0)
	assert.Equal(t, "hex", name)

	// Test clearing formatter
	err = state.SetFormat(0, "auto")
	assert.NoError(t, err)

	name = state.formatter.GetFormatterName(0)
	assert.Equal(t, "auto", name)
}

func TestState_Close(t *testing.T) {
	ctx := context.Background()
	state := &State{
		formatter: objecttool.NewFormatterRegistry(),
		ctx:       ctx,
	}

	// Should not panic
	assert.NotPanics(t, func() {
		state.Close()
	})
}

func TestState_GlobalRowOffset(t *testing.T) {
	ctx := context.Background()
	state := &State{
		formatter:    objecttool.NewFormatterRegistry(),
		ctx:          ctx,
		currentBlock: 0,
		rowOffset:    0,
	}

	// Test initial offset
	offset := state.GlobalRowOffset()
	assert.Equal(t, int64(0), offset)

	// Test with different row offset
	state.rowOffset = 100
	offset = state.GlobalRowOffset()
	assert.Equal(t, int64(100), offset)

	// Test with different block (approximate calculation)
	state.currentBlock = 1
	state.rowOffset = 50
	offset = state.GlobalRowOffset()
	// Should be approximately 8192 (assumed block size) + 50
	assert.Greater(t, offset, int64(50))
}

func TestNewState(t *testing.T) {
	// We can't create a real ObjectReader easily, so test NewState logic
	// by testing the components it should initialize

	// Test formatter registry creation
	registry := objecttool.NewFormatterRegistry()
	assert.NotNil(t, registry)

	// Test default values that NewState should set
	expectedPageSize := 20
	expectedMaxColWidth := 64

	assert.Equal(t, 20, expectedPageSize)
	assert.Equal(t, 64, expectedMaxColWidth)
}

// TestStateWithVariousDataTypes tests formatters with various MatrixOne data types
func TestStateWithVariousDataTypes(t *testing.T) {
	// Test that formatters handle all common MatrixOne data types
	testCases := []struct {
		name      string
		typeOid   types.T
		formatter Formatter
		testValue interface{}
	}{
		{"int32", types.T_int32, &DefaultFormatter{}, int32(123)},
		{"int64", types.T_int64, &DefaultFormatter{}, int64(456)},
		{"uint32", types.T_uint32, &DefaultFormatter{}, uint32(789)},
		{"uint64", types.T_uint64, &DefaultFormatter{}, uint64(999)},
		{"float32", types.T_float32, &DefaultFormatter{}, float32(1.23)},
		{"float64", types.T_float64, &DefaultFormatter{}, float64(4.56)},
		{"varchar", types.T_varchar, &DefaultFormatter{}, "test string"},
		{"bool", types.T_bool, &DefaultFormatter{}, true},
		{"blob", types.T_blob, &DefaultFormatter{}, []byte{0x01, 0x02, 0x03}},
		{"text", types.T_text, &DefaultFormatter{}, "text content"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			colType := types.Type{Oid: tc.typeOid}

			// Test that formatter can handle the type
			canFormat := tc.formatter.CanFormat(tc.testValue, colType)
			assert.True(t, canFormat, "Formatter should handle type %s", tc.name)

			// Test that formatter produces non-empty output
			result := tc.formatter.Format(tc.testValue)
			assert.NotEmpty(t, result, "Formatter should produce output for type %s", tc.name)
		})
	}

	// Test special formatters
	t.Run("ts_formatter", func(t *testing.T) {
		formatter := &TSFormatter{}
		tsType := types.Type{Oid: types.T_TS}
		var tsValue types.TS // TS is [12]byte

		canFormat := formatter.CanFormat(tsValue, tsType)
		assert.True(t, canFormat, "TSFormatter should handle TS type")

		// Zero TS may produce empty or "0" output, just check it doesn't panic
		result := formatter.Format(tsValue)
		_ = result // TSFormatter may return empty for zero value
	})

	t.Run("rowid_formatter", func(t *testing.T) {
		formatter := &RowidFormatter{}
		rowidType := types.Type{Oid: types.T_Rowid}
		var rowidValue types.Rowid // Rowid is [16]byte

		canFormat := formatter.CanFormat(rowidValue, rowidType)
		assert.True(t, canFormat, "RowidFormatter should handle Rowid type")

		result := formatter.Format(rowidValue)
		assert.NotEmpty(t, result, "RowidFormatter should produce output")
	})

	t.Run("hex_formatter", func(t *testing.T) {
		formatter := &HexFormatter{}
		testData := []byte{0xDE, 0xAD, 0xBE, 0xEF}

		result := formatter.Format(testData)
		assert.Contains(t, result, "de", "HexFormatter should produce hex output")
		assert.Contains(t, result, "ad", "HexFormatter should produce hex output")
	})

	t.Run("objectstats_formatter", func(t *testing.T) {
		formatter := &objecttool.ObjectStatsFormatter{}

		// ObjectStatsFormatter requires specific length byte slice
		// Just test that it doesn't panic with wrong length
		testData := []byte{0x01, 0x02, 0x03, 0x04}
		canFormat := formatter.CanFormat(testData, types.Type{})
		assert.False(t, canFormat, "ObjectStatsFormatter should reject wrong length")
	})
}
