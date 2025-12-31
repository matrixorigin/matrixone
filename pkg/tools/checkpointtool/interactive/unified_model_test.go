package interactive

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
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
