package interactive

import (
	"strings"
	"testing"
)

func TestRenderSimpleTable(t *testing.T) {
	headers := []string{"Col1", "Col2", "Col3"}
	rows := [][]string{
		{"a", "b", "c"},
		{"longer text", "y", "z"},
	}

	output := RenderSimpleTable(headers, rows, 0, 0)

	// Check that output contains borders
	if !strings.Contains(output, "┌") {
		t.Error("Output should contain top border")
	}
	if !strings.Contains(output, "└") {
		t.Error("Output should contain bottom border")
	}

	// Check that headers are present
	if !strings.Contains(output, "Col1") {
		t.Error("Output should contain Col1 header")
	}

	// Check that data is present
	if !strings.Contains(output, "longer text") {
		t.Error("Output should contain 'longer text'")
	}
}

func TestRenderSimpleTable_HorizontalScroll(t *testing.T) {
	headers := []string{"Col1", "Col2", "Col3", "Col4"}
	rows := [][]string{
		{"a", "b", "c", "d"},
		{"e", "f", "g", "h"},
	}

	// Scroll to show Col2, Col3, Col4 (skip Col1)
	output := RenderSimpleTable(headers, rows, 0, 1)

	// Should NOT contain Col1
	if strings.Contains(output, "Col1") {
		t.Error("Output should not contain Col1 after scrolling")
	}

	// Should contain Col2, Col3, Col4
	if !strings.Contains(output, "Col2") {
		t.Error("Output should contain Col2")
	}
	if !strings.Contains(output, "Col3") {
		t.Error("Output should contain Col3")
	}

	// Should contain data from scrolled columns
	if !strings.Contains(output, "f") {
		t.Error("Output should contain 'f' (Col2 data)")
	}
}
