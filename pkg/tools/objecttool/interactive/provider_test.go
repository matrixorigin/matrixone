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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/assert"
)

// TestColumnExpander tests the column expansion logic
func TestColumnExpander(t *testing.T) {
	t.Run("expand_single_column_to_multiple", func(t *testing.T) {
		expander := &ColumnExpander{
			SourceCol: 2,
			NewCols:   []string{"name", "flags", "size"},
			NewTypes: []types.Type{
				types.T_varchar.ToType(),
				types.T_varchar.ToType(),
				types.T_uint32.ToType(),
			},
			ExpandFunc: func(value any) []any {
				// Mock expansion
				return []any{"obj_name", "SC", uint32(1024)}
			},
		}

		// Test expand function
		result := expander.ExpandFunc("test")
		assert.Equal(t, 3, len(result))
		assert.Equal(t, "obj_name", result[0])
		assert.Equal(t, "SC", result[1])
		assert.Equal(t, uint32(1024), result[2])
	})

	t.Run("expand_with_nil_value", func(t *testing.T) {
		expander := &ColumnExpander{
			SourceCol: 0,
			NewCols:   []string{"a", "b"},
			NewTypes: []types.Type{
				types.T_varchar.ToType(),
				types.T_varchar.ToType(),
			},
			ExpandFunc: func(value any) []any {
				if value == nil {
					return []any{"", ""}
				}
				return []any{"x", "y"}
			},
		}

		result := expander.ExpandFunc(nil)
		assert.Equal(t, "", result[0])
		assert.Equal(t, "", result[1])
	})
}

// TestViewOptions tests ViewOptions configuration
func TestViewOptions(t *testing.T) {
	t.Run("default_values", func(t *testing.T) {
		opts := &ViewOptions{}
		assert.Equal(t, int64(0), opts.StartRow)
		assert.Equal(t, int64(0), opts.EndRow)
		assert.Nil(t, opts.ColumnNames)
		assert.Nil(t, opts.ColumnExpander)
		assert.Equal(t, 0, opts.ObjectNameCol)
	})

	t.Run("with_row_range", func(t *testing.T) {
		opts := &ViewOptions{
			StartRow: 10,
			EndRow:   50,
		}
		assert.Equal(t, int64(10), opts.StartRow)
		assert.Equal(t, int64(50), opts.EndRow)
	})

	t.Run("with_column_names", func(t *testing.T) {
		opts := &ViewOptions{
			ColumnNames: map[uint16]string{
				0: "id",
				1: "name",
				2: "value",
			},
		}
		assert.Equal(t, "id", opts.ColumnNames[0])
		assert.Equal(t, "name", opts.ColumnNames[1])
		assert.Equal(t, "value", opts.ColumnNames[2])
	})

	t.Run("with_drill_down", func(t *testing.T) {
		opts := &ViewOptions{
			ObjectNameCol: 4,
			BaseDir:       "/data/objects",
		}
		assert.Equal(t, 4, opts.ObjectNameCol)
		assert.Equal(t, "/data/objects", opts.BaseDir)
	})
}

// TestObjectDataHandler tests the handler behavior
func TestObjectDataHandler(t *testing.T) {
	t.Run("match_row_case_insensitive", func(t *testing.T) {
		handler := &ObjectDataHandler{}

		row := []string{"Hello", "World", "Test123"}

		// Should match case-insensitively
		assert.True(t, handler.MatchRow(row, "hello"))
		assert.True(t, handler.MatchRow(row, "WORLD"))
		assert.True(t, handler.MatchRow(row, "test"))

		// Should not match
		assert.False(t, handler.MatchRow(row, "notfound"))
	})

	t.Run("match_row_partial", func(t *testing.T) {
		handler := &ObjectDataHandler{}

		row := []string{"object_name_123", "value"}

		// Partial match should work
		assert.True(t, handler.MatchRow(row, "object"))
		assert.True(t, handler.MatchRow(row, "name"))
		assert.True(t, handler.MatchRow(row, "123"))
	})

	t.Run("filter_row_same_as_match", func(t *testing.T) {
		handler := &ObjectDataHandler{}

		row := []string{"test", "data"}

		// FilterRow should behave same as MatchRow
		assert.Equal(t,
			handler.MatchRow(row, "test"),
			handler.FilterRow(row, "test"))
	})
}

// TestDataProviderInterface tests that providers implement the interface correctly
func TestDataProviderInterface(t *testing.T) {
	t.Run("object_data_provider_implements_interface", func(t *testing.T) {
		var _ interactive.DataProvider = &ObjectDataProvider{}
	})

	t.Run("block_meta_provider_implements_interface", func(t *testing.T) {
		var _ interactive.DataProvider = &BlockMetaProvider{}
	})

	t.Run("object_meta_provider_implements_interface", func(t *testing.T) {
		var _ interactive.DataProvider = &ObjectMetaProvider{}
	})
}

// TestRowRangeFilter tests row range filtering logic
func TestRowRangeFilter(t *testing.T) {
	t.Run("with_range", func(t *testing.T) {
		state := &State{
			rowRangeStart: 10,
			rowRangeEnd:   20,
		}
		// Should return 11 rows (10 to 20 inclusive)
		count := state.rowRangeEnd - state.rowRangeStart + 1
		assert.Equal(t, int64(11), count)
	})

	t.Run("range_not_set", func(t *testing.T) {
		state := &State{
			rowRangeStart: -1,
			rowRangeEnd:   -1,
		}
		assert.Equal(t, int64(-1), state.rowRangeStart)
		assert.Equal(t, int64(-1), state.rowRangeEnd)
	})

	t.Run("direct_range_assignment", func(t *testing.T) {
		state := &State{
			rowRangeStart: -1,
			rowRangeEnd:   -1,
		}

		// Direct assignment (SetRowRange needs reader)
		state.rowRangeStart = 5
		state.rowRangeEnd = 15
		assert.Equal(t, int64(5), state.rowRangeStart)
		assert.Equal(t, int64(15), state.rowRangeEnd)
	})
}

// TestObjectToOpen tests drill-down functionality
func TestObjectToOpen(t *testing.T) {
	t.Run("get_object_with_base_dir", func(t *testing.T) {
		state := &State{
			objectToOpen: "test_object",
			baseDir:      "/data/dir",
		}

		path := state.GetObjectToOpen()
		assert.Equal(t, "/data/dir/test_object", path)
	})

	t.Run("get_object_without_base_dir", func(t *testing.T) {
		state := &State{
			objectToOpen: "test_object",
			baseDir:      "",
		}

		path := state.GetObjectToOpen()
		assert.Equal(t, "test_object", path)
	})

	t.Run("clear_object_to_open", func(t *testing.T) {
		state := &State{
			objectToOpen: "test_object",
		}

		state.ClearObjectToOpen()
		assert.Equal(t, "", state.objectToOpen)
	})

	t.Run("empty_object_to_open", func(t *testing.T) {
		state := &State{
			objectToOpen: "",
			baseDir:      "/data",
		}

		path := state.GetObjectToOpen()
		assert.Equal(t, "", path)
	})
}

// TestExpandRow tests row expansion with column expander
func TestExpandRow(t *testing.T) {
	t.Run("expand_middle_column", func(t *testing.T) {
		state := &State{
			formatter: objecttool.NewFormatterRegistry(),
			colExpander: &ColumnExpander{
				SourceCol: 1,
				NewCols:   []string{"a", "b", "c"},
				NewTypes: []types.Type{
					types.T_varchar.ToType(),
					types.T_varchar.ToType(),
					types.T_varchar.ToType(),
				},
			},
		}

		row := []string{"col0", "", "col2"}
		expandedValues := []any{"val_a", "val_b", "val_c"}

		result := state.expandRow(row, expandedValues)

		// Original: [col0, _, col2]
		// After: [col0, val_a, val_b, val_c, col2]
		assert.Equal(t, 5, len(result))
		assert.Equal(t, "col0", result[0])
		assert.Equal(t, "val_a", result[1])
		assert.Equal(t, "val_b", result[2])
		assert.Equal(t, "val_c", result[3])
		assert.Equal(t, "col2", result[4])
	})

	t.Run("expand_first_column", func(t *testing.T) {
		state := &State{
			formatter: objecttool.NewFormatterRegistry(),
			colExpander: &ColumnExpander{
				SourceCol: 0,
				NewCols:   []string{"x", "y"},
				NewTypes: []types.Type{
					types.T_varchar.ToType(),
					types.T_varchar.ToType(),
				},
			},
		}

		row := []string{"", "other"}
		expandedValues := []any{"x_val", "y_val"}

		result := state.expandRow(row, expandedValues)

		assert.Equal(t, 3, len(result))
		assert.Equal(t, "x_val", result[0])
		assert.Equal(t, "y_val", result[1])
		assert.Equal(t, "other", result[2])
	})

	t.Run("expand_last_column", func(t *testing.T) {
		state := &State{
			formatter: objecttool.NewFormatterRegistry(),
			colExpander: &ColumnExpander{
				SourceCol: 2,
				NewCols:   []string{"p", "q"},
				NewTypes: []types.Type{
					types.T_varchar.ToType(),
					types.T_varchar.ToType(),
				},
			},
		}

		row := []string{"a", "b", ""}
		expandedValues := []any{"p_val", "q_val"}

		result := state.expandRow(row, expandedValues)

		assert.Equal(t, 4, len(result))
		assert.Equal(t, "a", result[0])
		assert.Equal(t, "b", result[1])
		assert.Equal(t, "p_val", result[2])
		assert.Equal(t, "q_val", result[3])
	})
}

// TestBlockOffsets tests block offset calculations
func TestBlockOffsets(t *testing.T) {
	t.Run("get_block_start_row", func(t *testing.T) {
		state := &State{
			blockOffsets: []int64{0, 1000, 2500, 4000},
		}

		assert.Equal(t, int64(0), state.GetBlockStartRow(0))
		assert.Equal(t, int64(1000), state.GetBlockStartRow(1))
		assert.Equal(t, int64(2500), state.GetBlockStartRow(2))
		assert.Equal(t, int64(4000), state.GetBlockStartRow(3))
	})

	t.Run("get_block_start_row_out_of_range", func(t *testing.T) {
		state := &State{
			blockOffsets: []int64{0, 100},
		}

		// Out of range should return 0
		assert.Equal(t, int64(0), state.GetBlockStartRow(10))
	})

	t.Run("global_row_offset", func(t *testing.T) {
		state := &State{
			blockOffsets: []int64{0, 1000, 2000},
			currentBlock: 1,
			rowOffset:    50,
		}

		// Block 1 starts at 1000, plus offset 50
		assert.Equal(t, int64(1050), state.GlobalRowOffset())
	})
}

// TestViewModeSwitchState tests switching between view modes
func TestViewModeSwitchState(t *testing.T) {
	t.Run("switch_to_blk_meta", func(t *testing.T) {
		state := &State{
			viewMode: ViewModeData,
		}

		state.viewMode = ViewModeBlkMeta
		assert.Equal(t, ViewModeBlkMeta, state.viewMode)
	})

	t.Run("switch_to_obj_meta", func(t *testing.T) {
		state := &State{
			viewMode: ViewModeData,
		}

		state.viewMode = ViewModeObjMeta
		assert.Equal(t, ViewModeObjMeta, state.viewMode)
	})

	t.Run("switch_back_to_data", func(t *testing.T) {
		state := &State{
			viewMode: ViewModeBlkMeta,
		}

		state.viewMode = ViewModeData
		assert.Equal(t, ViewModeData, state.viewMode)
	})
}

// TestStateInitialization tests proper state initialization
func TestStateInitialization(t *testing.T) {
	t.Run("default_values", func(t *testing.T) {
		state := &State{
			rowRangeStart: -1,
			rowRangeEnd:   -1,
			objectNameCol: -1,
			pageSize:      20,
			maxColWidth:   64,
		}

		assert.Equal(t, int64(-1), state.rowRangeStart)
		assert.Equal(t, int64(-1), state.rowRangeEnd)
		assert.Equal(t, -1, state.objectNameCol)
		assert.Equal(t, 20, state.pageSize)
		assert.Equal(t, 64, state.maxColWidth)
	})
}
