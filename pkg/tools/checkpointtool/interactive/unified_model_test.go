package interactive

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/stretchr/testify/assert"
)

func TestUnifiedModel(t *testing.T) {
	// Create empty reader for testing
	reader := &checkpointtool.CheckpointReader{}
	model := NewUnifiedModel(reader)

	// Test initial view
	view := model.View()
	t.Logf("Initial view:\n%s", view)

	if !strings.Contains(view, "Checkpoint List") {
		t.Error("Should show checkpoint list title")
	}

	// Test navigation
	model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})

	// View should still render
	view = model.View()
	if view == "" {
		t.Error("View should not be empty after navigation")
	}
}

func TestUnifiedModelPageStack(t *testing.T) {
	reader := &checkpointtool.CheckpointReader{}
	model := NewUnifiedModel(reader)

	// Initially no pages in stack
	if len(model.pageStack) != 0 {
		t.Errorf("Initial stack should be empty, got %d", len(model.pageStack))
	}

	// After selecting checkpoint, stack should have 1 page
	// (This would require mock data to work properly)
}

// TestExpandObjectStats tests the ObjectStats expansion function
func TestExpandObjectStats(t *testing.T) {
	t.Run("valid_objectstats", func(t *testing.T) {
		// Create a valid ObjectStats
		var stats objectio.ObjectStats
		var objId objectio.ObjectId
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&objId))
		objectio.SetObjectStatsRowCnt(&stats, 100)
		objectio.SetObjectStatsSize(&stats, 2048)
		objectio.SetObjectStatsOriginSize(&stats, 4096)

		data := stats[:]
		result := expandObjectStats(data)

		assert.Equal(t, 5, len(result))
		// object_name should not be empty
		assert.NotEmpty(t, result[0])
		// flags should be "-" (no flags set)
		assert.Equal(t, "-", result[1])
		// rows
		assert.Equal(t, uint32(100), result[2])
		// osize
		assert.Equal(t, "4.0KB", result[3])
		// csize
		assert.Equal(t, "2.0KB", result[4])
	})

	t.Run("objectstats_with_flags", func(t *testing.T) {
		var stats objectio.ObjectStats
		var objId objectio.ObjectId
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&objId))
		objectio.WithSorted()(&stats)
		objectio.WithCNCreated()(&stats)

		data := stats[:]
		result := expandObjectStats(data)

		// flags should contain S and C
		flags := result[1].(string)
		assert.Contains(t, flags, "S")
		assert.Contains(t, flags, "C")
	})

	t.Run("objectstats_appendable", func(t *testing.T) {
		var stats objectio.ObjectStats
		var objId objectio.ObjectId
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&objId))
		objectio.WithAppendable()(&stats)

		data := stats[:]
		result := expandObjectStats(data)

		flags := result[1].(string)
		assert.Contains(t, flags, "A")
	})

	t.Run("invalid_data_wrong_length", func(t *testing.T) {
		data := []byte{1, 2, 3} // Wrong length
		result := expandObjectStats(data)

		// Should return empty values
		assert.Equal(t, 5, len(result))
		assert.Equal(t, "", result[0])
		assert.Equal(t, "", result[1])
	})

	t.Run("invalid_data_wrong_type", func(t *testing.T) {
		result := expandObjectStats("not bytes")

		assert.Equal(t, 5, len(result))
		assert.Equal(t, "", result[0])
	})

	t.Run("nil_value", func(t *testing.T) {
		result := expandObjectStats(nil)

		assert.Equal(t, 5, len(result))
		assert.Equal(t, "", result[0])
	})
}

// TestFormatSize tests the size formatting function
func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes    uint32
		expected string
	}{
		{0, "0B"},
		{100, "100B"},
		{1023, "1023B"},
		{1024, "1.0KB"},
		{1536, "1.5KB"},
		{2048, "2.0KB"},
		{1048576, "1.0MB"},
		{1572864, "1.5MB"},
		{1073741824, "1.0GB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatSize(tt.bytes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCheckpointListProvider tests the checkpoint list provider
func TestCheckpointListProvider(t *testing.T) {
	t.Run("empty_state", func(t *testing.T) {
		state := &State{}
		provider := &CheckpointListProvider{state: state}

		rows := provider.GetRows()
		assert.Empty(t, rows)
	})

	t.Run("get_row_nums_returns_nil", func(t *testing.T) {
		state := &State{}
		provider := &CheckpointListProvider{state: state}

		rowNums := provider.GetRowNums()
		assert.Nil(t, rowNums)
	})
}

// TestCheckpointListHandler tests the handler behavior
func TestCheckpointListHandler(t *testing.T) {
	t.Run("match_row", func(t *testing.T) {
		handler := &checkpointListHandler{}

		row := []string{"G", "123456", "2024/01/01", "2024/01/02", "Finished", "1"}

		assert.True(t, handler.MatchRow(row, "123456"))
		assert.True(t, handler.MatchRow(row, "Finished"))
		assert.True(t, handler.MatchRow(row, "2024"))
		assert.False(t, handler.MatchRow(row, "notfound"))
	})

	t.Run("on_back_returns_nil", func(t *testing.T) {
		handler := &checkpointListHandler{}

		cmd := handler.OnBack()
		// OnBack returns nil for checkpoint list (top level)
		assert.Nil(t, cmd)
	})
}

// TestTableDetailHandler tests table detail handler
func TestTableDetailHandler(t *testing.T) {
	t.Run("match_row", func(t *testing.T) {
		handler := &tableDetailHandler{}

		row := []string{"Data", "object_name_123", "0-10", "100", "1.5KB", "01-01 12:00"}

		assert.True(t, handler.MatchRow(row, "Data"))
		assert.True(t, handler.MatchRow(row, "object"))
		assert.True(t, handler.MatchRow(row, "123"))
		assert.False(t, handler.MatchRow(row, "notfound"))
	})
}

// TestTSFormatting tests timestamp formatting
func TestTSFormatting(t *testing.T) {
	t.Run("format_ts_zero", func(t *testing.T) {
		var ts types.TS
		result := formatTS(ts)
		assert.Equal(t, "0-0", result)
	})

	t.Run("format_ts_short_empty", func(t *testing.T) {
		var ts types.TS
		result := formatTSShort(ts)
		assert.Equal(t, "-", result)
	})
}
