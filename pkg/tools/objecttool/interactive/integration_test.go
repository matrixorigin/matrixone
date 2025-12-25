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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestObjectFile creates an object file with various data types for testing
func createTestObjectFile(t *testing.T, dir string, filename string) string {
	return createTestObjectFileWithRowsAndCols(t, dir, filename, 20, 8)
}

// createTestObjectFileWithRows creates a test object file with specified number of rows
func createTestObjectFileWithRows(t *testing.T, dir string, filename string, rowCount int) string {
	return createTestObjectFileWithRowsAndCols(t, dir, filename, rowCount, 8)
}

// createTestObjectFileWithRowsAndCols creates a test object file with custom rows and columns
func createTestObjectFileWithRowsAndCols(t *testing.T, dir string, filename string, rowCount, colCount int) string {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	require.NoError(t, err)
	defer service.Close(ctx)
	
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterNormal, filename, service)
	require.NoError(t, err)
	
	longText := "This is a very long text field that should test the width truncation functionality. It contains more than 100 characters to ensure proper testing of column width settings and truncation behavior in the interactive viewer."
	
	// Write multiple blocks (each block has up to 8192 rows)
	rowsPerBlock := 8192
	for blockIdx := 0; blockIdx < (rowCount+rowsPerBlock-1)/rowsPerBlock; blockIdx++ {
		startRow := blockIdx * rowsPerBlock
		endRow := startRow + rowsPerBlock
		if endRow > rowCount {
			endRow = rowCount
		}
		blockRows := endRow - startRow
		
		bat := batch.NewWithSize(colCount)
		for i := 0; i < colCount; i++ {
			switch i {
			case 0:
				bat.Vecs[i] = vector.NewVec(types.T_int32.ToType())
			case 1:
				bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
			case 2:
				bat.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
			case 3:
				bat.Vecs[i] = vector.NewVec(types.T_float64.ToType())
			case 4:
				bat.Vecs[i] = vector.NewVec(types.T_bool.ToType())
			case 5:
				bat.Vecs[i] = vector.NewVec(types.T_uint64.ToType())
			case 6:
				bat.Vecs[i] = vector.NewVec(types.T_text.ToType())
			case 7:
				bat.Vecs[i] = vector.NewVec(types.T_blob.ToType())
			default:
				// For extra columns, cycle through basic types
				switch i % 4 {
				case 0:
					bat.Vecs[i] = vector.NewVec(types.T_int32.ToType())
				case 1:
					bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
				case 2:
					bat.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
				case 3:
					bat.Vecs[i] = vector.NewVec(types.T_float64.ToType())
				}
			}
		}
		
		for i := startRow; i < endRow; i++ {
			for colIdx := 0; colIdx < colCount; colIdx++ {
				switch colIdx {
				case 0:
					vector.AppendFixed(bat.Vecs[colIdx], int32(i), false, mp)
				case 1:
					vector.AppendFixed(bat.Vecs[colIdx], int64(i*1000), false, mp)
				case 2:
					if i%3 == 0 {
						vector.AppendBytes(bat.Vecs[colIdx], []byte(longText), false, mp)
					} else if i%3 == 1 {
						vector.AppendBytes(bat.Vecs[colIdx], []byte("medium length text"), false, mp)
					} else {
						vector.AppendBytes(bat.Vecs[colIdx], []byte("short"), false, mp)
					}
				case 3:
					vector.AppendFixed(bat.Vecs[colIdx], float64(i)*1.5, false, mp)
				case 4:
					vector.AppendFixed(bat.Vecs[colIdx], i%2 == 0, false, mp)
				case 5:
					vector.AppendFixed(bat.Vecs[colIdx], uint64(i*100), false, mp)
				case 6:
					vector.AppendBytes(bat.Vecs[colIdx], []byte(longText[:50+i%50]), false, mp)
				case 7:
					vector.AppendBytes(bat.Vecs[colIdx], []byte{byte(i), byte(i + 1), byte(i + 2)}, false, mp)
				default:
					// For extra columns
					switch colIdx % 4 {
					case 0:
						vector.AppendFixed(bat.Vecs[colIdx], int32(i), false, mp)
					case 1:
						vector.AppendFixed(bat.Vecs[colIdx], int64(i*1000), false, mp)
					case 2:
						vector.AppendBytes(bat.Vecs[colIdx], []byte("short"), false, mp)
					case 3:
						vector.AppendFixed(bat.Vecs[colIdx], float64(i)*1.5, false, mp)
					}
				}
			}
		}
		bat.SetRowCount(blockRows)
		
		_, err = writer.Write(bat)
		require.NoError(t, err)
		
		bat.Clean(mp)
	}
	
	_, err = writer.WriteEnd(ctx, objectio.WriteOptions{
		Type: objectio.WriteTS,
		Val:  time.Now(),
	})
	require.NoError(t, err)
	
	return filepath.Join(dir, filename)
}

// TestObjectReaderOpen tests ObjectReader.Open with real file
func TestObjectReaderOpen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_open.obj")
	
	ctx := context.Background()
	
	// Test Open
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()
	
	// Test Info
	info := reader.Info()
	assert.Equal(t, objectPath, info.Path)
	assert.Greater(t, info.BlockCount, uint32(0))
	assert.Greater(t, info.RowCount, uint64(0))
	assert.Greater(t, info.ColCount, uint16(0))
	
	// Test Columns
	cols := reader.Columns()
	assert.NotEmpty(t, cols)
	assert.Equal(t, int(info.ColCount), len(cols))
	
	// Test BlockCount
	blockCount := reader.BlockCount()
	assert.Equal(t, info.BlockCount, blockCount)
	
	// Test ReadBlock for all blocks
	for i := uint32(0); i < blockCount; i++ {
		batch, release, err := reader.ReadBlock(ctx, i)
		require.NoError(t, err)
		require.NotNil(t, batch)
		assert.Greater(t, batch.RowCount(), 0)
		release()
	}
	
	// Test Close
	err = reader.Close()
	require.NoError(t, err)
}

// TestObjectReaderInvalidFile tests error handling
func TestObjectReaderInvalidFile(t *testing.T) {
	ctx := context.Background()
	
	// Test with non-existent file
	_, err := objecttool.Open(ctx, "/tmp/nonexistent.obj")
	assert.Error(t, err)
}


// TestCommandExecution tests command execution with real object file
func TestCommandExecution(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test InfoCommand
	infoCmd := &InfoCommand{}
	output, status, err := infoCmd.Execute(state)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	assert.Empty(t, status)
	assert.Contains(t, output, "Blocks")
	assert.Contains(t, output, "Rows")
	
	// Test SchemaCommand
	schemaCmd := &SchemaCommand{}
	output, status, err = schemaCmd.Execute(state)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	assert.Empty(t, status)
	
	// Test VerticalCommand
	verticalCmd := &VerticalCommand{}
	output, status, err = verticalCmd.Execute(state)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	assert.Empty(t, status)
	
	// Test SetCommand - width
	setCmd := &SetCommand{Option: "width", Value: 100}
	output, status, err = setCmd.Execute(state)
	require.NoError(t, err)
	assert.Equal(t, 100, state.maxColWidth)
	
	// Test SetCommand - unlimited width
	setCmd = &SetCommand{Option: "width", Value: 0}
	output, status, err = setCmd.Execute(state)
	require.NoError(t, err)
	assert.Equal(t, 0, state.maxColWidth)
	
	// Test vertical mode directly
	state.verticalMode = true
	assert.True(t, state.verticalMode)
	state.verticalMode = false
	assert.False(t, state.verticalMode)
	
	// Test ColumnsCommand
	colsCmd := &ColumnsCommand{Columns: []uint16{0, 2, 4}}
	output, status, err = colsCmd.Execute(state)
	require.NoError(t, err)
	assert.Equal(t, 3, len(state.visibleCols))
	
	// Test ColumnsCommand - all
	colsCmd = &ColumnsCommand{ShowAll: true}
	output, status, err = colsCmd.Execute(state)
	require.NoError(t, err)
	assert.Nil(t, state.visibleCols)
	
	// Test FormatCommand
	fmtCmd := &FormatCommand{ColIdx: 7, FormatterName: "hex"}
	output, status, err = fmtCmd.Execute(state)
	require.NoError(t, err)
}

// TestAllCommands tests all command types
func TestAllCommands(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_cmd.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test QuitCommand
	quitCmd := &QuitCommand{}
	_, quit, err := quitCmd.Execute(state)
	require.NoError(t, err)
	assert.True(t, quit)
	
	// Test HelpCommand
	helpCmd := &HelpCommand{}
	output, _, err := helpCmd.Execute(state)
	require.NoError(t, err)
	assert.NotEmpty(t, output)
	assert.Contains(t, output, "Browse Mode")
	
	// Test ScrollCommand - down
	scrollCmd := &ScrollCommand{Down: true, Lines: 1}
	output, _, err = scrollCmd.Execute(state)
	require.NoError(t, err)
	
	// Test ScrollCommand - up
	scrollCmd = &ScrollCommand{Down: false, Lines: 1}
	output, _, err = scrollCmd.Execute(state)
	require.NoError(t, err)
}


// TestViewRendering tests view rendering functions
func TestViewRendering(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Create model
	m := model{
		state:      state,
		cmdInput:   "",
		cmdHistory: []string{},
	}
	
	// Test View rendering - should show table
	view := m.View()
	assert.NotEmpty(t, view)
	assert.Contains(t, view, "Col0") // Column header
	
	// Test with message
	m.message = "Test message"
	view = m.View()
	assert.NotEmpty(t, view)
	
	// Test vertical mode
	state.verticalMode = true
	view = m.View()
	assert.NotEmpty(t, view)
	
	// Test with nil state
	m2 := model{state: nil}
	view = m2.View()
	assert.NotEmpty(t, view)
	assert.Contains(t, view, "No data")
}

// TestStateNavigation tests all navigation functions
func TestStateNavigation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_nav.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Set small page size for testing
	state.pageSize = 5
	
	// Test initial state
	rows, _, err := state.CurrentRows()
	require.NoError(t, err)
	initialCount := len(rows)
	assert.LessOrEqual(t, initialCount, 5)
	
	// Test ScrollDown
	err = state.ScrollDown()
	require.NoError(t, err)
	
	// Test ScrollUp
	err = state.ScrollUp()
	require.NoError(t, err)
	
	// Test multiple ScrollDown
	for i := 0; i < 3; i++ {
		err = state.ScrollDown()
		require.NoError(t, err)
	}
	
	// Test GotoRow
	err = state.GotoRow(0)
	require.NoError(t, err)
	
	err = state.GotoRow(10)
	require.NoError(t, err)
	
	err = state.GotoRow(19) // Last row
	require.NoError(t, err)
	
	// Test GotoBlock
	err = state.GotoBlock(0)
	require.NoError(t, err)
	
	// Test NextPage
	err = state.NextPage()
	require.NoError(t, err)
	
	// Test PrevPage
	err = state.PrevPage()
	require.NoError(t, err)
	
	// Test GlobalRowOffset
	offset := state.GlobalRowOffset()
	assert.GreaterOrEqual(t, int(offset), 0)
	
	// Test ensureBlockLoaded
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	assert.NotEmpty(t, rows)
}

// TestStateDisplayModes tests different display modes
func TestStateDisplayModes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_display.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test horizontal mode (default)
	assert.False(t, state.verticalMode)
	rows, _, err := state.CurrentRows()
	require.NoError(t, err)
	assert.NotEmpty(t, rows)
	
	// Test vertical mode
	state.verticalMode = true
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	assert.NotEmpty(t, rows)
	
	// Test column filtering
	state.visibleCols = []uint16{0, 1, 2}
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	if len(rows) > 0 {
		assert.Equal(t, 3, len(rows[0]))
	}
	
	// Test width settings
	state.maxColWidth = 20
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	assert.NotEmpty(t, rows)
	
	// Test unlimited width
	state.maxColWidth = 0
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	assert.NotEmpty(t, rows)
}




// TestRealObjectFile tests with a real object file containing various data types
func TestRealObjectFile(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	// Create test object file with various data types
	objectPath := createTestObjectFile(t, tmpDir, "test.obj")
	
	// Open the object file
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	// Test ObjectReader methods
	info := reader.Info()
	assert.Greater(t, info.BlockCount, uint32(0))
	assert.Equal(t, uint64(20), info.RowCount)
	assert.Equal(t, uint16(8), info.ColCount)
	assert.NotEmpty(t, info.Path)
	
	// Test Columns
	cols := reader.Columns()
	assert.Equal(t, 8, len(cols))
	assert.Equal(t, types.T_int32, cols[0].Type.Oid)
	assert.Equal(t, types.T_int64, cols[1].Type.Oid)
	assert.Equal(t, types.T_varchar, cols[2].Type.Oid)
	assert.Equal(t, types.T_float64, cols[3].Type.Oid)
	assert.Equal(t, types.T_bool, cols[4].Type.Oid)
	assert.Equal(t, types.T_uint64, cols[5].Type.Oid)
	assert.Equal(t, types.T_text, cols[6].Type.Oid)
	assert.Equal(t, types.T_blob, cols[7].Type.Oid)
	
	// Test BlockCount
	blockCount := reader.BlockCount()
	assert.Greater(t, blockCount, uint32(0))
	
	// Test ReadBlock
	batch, release, err := reader.ReadBlock(ctx, 0)
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Greater(t, batch.RowCount(), 0)
	assert.Equal(t, 8, len(batch.Vecs))
	release()
	
	// Test state operations
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test reading rows
	rows, rowNumbers, err := state.CurrentRows()
	require.NoError(t, err)
	assert.Greater(t, len(rows), 0)
	assert.Equal(t, len(rows), len(rowNumbers))
	
	// Verify data content - should have long text
	if len(rows) > 0 {
		assert.Equal(t, 8, len(rows[0]))
		assert.Greater(t, len(rows[0][2]), 100, "Long varchar should be present in raw data")
	}
	
	// Test width setting
	state.maxColWidth = 50
	assert.Equal(t, 50, state.maxColWidth)
	state.maxColWidth = 0
	assert.Equal(t, 0, state.maxColWidth)
	
	// Test column filtering
	state.visibleCols = []uint16{0, 2, 3}
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	if len(rows) > 0 {
		assert.Equal(t, 3, len(rows[0]), "Should only show 3 columns")
	}
	
	state.visibleCols = nil
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	if len(rows) > 0 {
		assert.Equal(t, 8, len(rows[0]), "Should show all 8 columns")
	}
	
	// Test formatter
	err = state.SetFormat(7, "hex")
	require.NoError(t, err)
	
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	if len(rows) > 0 {
		assert.Contains(t, rows[0][7], "0", "Hex formatter should produce hex output")
	}
	
	// Test pagination
	state.pageSize = 5
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(rows), 5, "Should respect page size")
	
	// Test NextPage
	err = state.NextPage()
	require.NoError(t, err)
	rows2, _, err := state.CurrentRows()
	require.NoError(t, err)
	if len(rows) > 0 && len(rows2) > 0 {
		assert.NotEqual(t, rows[0][0], rows2[0][0], "Different pages should show different rows")
	}
	
	// Test PrevPage
	err = state.PrevPage()
	require.NoError(t, err)
	
	// Test navigation functions
	err = state.ScrollDown()
	require.NoError(t, err)
	
	err = state.ScrollUp()
	require.NoError(t, err)
	
	err = state.GotoRow(10)
	require.NoError(t, err)
	
	err = state.GotoBlock(0)
	require.NoError(t, err)
	
	// Test GlobalRowOffset
	offset := state.GlobalRowOffset()
	assert.GreaterOrEqual(t, int(offset), 0)
}



// TestStateBoundaryConditions tests boundary conditions
func TestStateBoundaryConditions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_boundary.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test ScrollUp at beginning
	err = state.ScrollUp()
	// May return error at boundary
	_ = err
	
	// Test ScrollDown to end
	for i := 0; i < 100; i++ {
		err = state.ScrollDown()
		if err != nil {
			break
		}
	}
	
	// Test ScrollDown at end
	err = state.ScrollDown()
	// May return error at boundary
	_ = err
	
	// Test GotoRow with invalid row
	err = state.GotoRow(1000)
	// May return error for out of range
	_ = err
	
	// Test GotoBlock with invalid block
	err = state.GotoBlock(100)
	assert.Error(t, err)
	
	// Test NextPage at end
	state.pageSize = 5
	for i := 0; i < 10; i++ {
		err = state.NextPage()
		if err != nil {
			break
		}
	}
	
	// Test PrevPage at beginning
	err = state.GotoRow(0)
	require.NoError(t, err)
	err = state.PrevPage()
	// May return error at boundary
	_ = err
	
	// Test with empty visible columns
	state.visibleCols = []uint16{}
	rows, _, err := state.CurrentRows()
	require.NoError(t, err)
	if len(rows) > 0 {
		assert.Equal(t, 0, len(rows[0]))
	}
}


// TestModelView tests model View function
func TestModelView(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_view.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	m := model{
		state:      state,
		cmdInput:   "",
		cmdHistory: []string{},
	}
	
	// Test View with different states
	view := m.View()
	assert.NotEmpty(t, view)
	assert.Contains(t, view, "Col0")
	
	// Test with command mode
	m.cmdMode = true
	m.cmdInput = "test command"
	view = m.View()
	assert.NotEmpty(t, view)
	
	// Test with message
	m.message = "Test message"
	view = m.View()
	assert.NotEmpty(t, view)
	
	// Test with search
	m.searchTerm = "test"
	view = m.View()
	assert.NotEmpty(t, view)
	
	// Test vertical mode
	state.verticalMode = true
	view = m.View()
	assert.NotEmpty(t, view)
}

// TestViewFunctions tests view rendering functions
func TestViewFunctions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_view.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test NewView
	v := NewView()
	assert.NotNil(t, v)
	
	// Test Render
	err = v.Render(state, "")
	assert.NoError(t, err)
	
	// Test with message
	err = v.Render(state, "Test message")
	assert.NoError(t, err)
	
	// Test with vertical mode
	state.verticalMode = true
	err = v.Render(state, "")
	assert.NoError(t, err)
	
	// Test with column filtering
	state.visibleCols = []uint16{0, 1, 2}
	err = v.Render(state, "")
	assert.NoError(t, err)
	
	// Test with width limit
	state.maxColWidth = 20
	err = v.Render(state, "")
	assert.NoError(t, err)
}


// TestStateEdgeCases tests edge cases for state functions
func TestStateEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_edge.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test PrevPage at beginning
	state.pageSize = 5
	err = state.PrevPage()
	// Should handle gracefully
	_ = err
	
	// Test NextPage multiple times
	for i := 0; i < 5; i++ {
		err = state.NextPage()
		if err != nil {
			break
		}
	}
	
	// Test PrevPage from middle
	err = state.PrevPage()
	assert.NoError(t, err)
	
	// Test ScrollDown multiple times
	state.pageSize = 10
	err = state.GotoRow(0)
	require.NoError(t, err)
	
	for i := 0; i < 15; i++ {
		err = state.ScrollDown()
		if err != nil {
			break
		}
	}
	
	// Test ScrollUp multiple times
	for i := 0; i < 15; i++ {
		err = state.ScrollUp()
		if err != nil {
			break
		}
	}
	
	// Test CurrentRows with different page sizes
	state.pageSize = 1
	rows, _, err := state.CurrentRows()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(rows), 1)
	
	state.pageSize = 100
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	assert.Greater(t, len(rows), 0)
	
	// Test with all columns visible
	state.visibleCols = nil
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	if len(rows) > 0 {
		assert.Equal(t, 8, len(rows[0]))
	}
	
	// Test with single column
	state.visibleCols = []uint16{0}
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	if len(rows) > 0 {
		assert.Equal(t, 1, len(rows[0]))
	}
}

// TestCommandEdgeCases tests edge cases for commands
func TestCommandEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_cmd_edge.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test SetCommand with various values
	setCmd := &SetCommand{Option: "width", Value: 1}
	_, _, err = setCmd.Execute(state)
	require.NoError(t, err)
	assert.Equal(t, 1, state.maxColWidth)
	
	setCmd = &SetCommand{Option: "width", Value: 1000}
	_, _, err = setCmd.Execute(state)
	require.NoError(t, err)
	assert.Equal(t, 1000, state.maxColWidth)
	
	// Test SetCommand with unknown option
	setCmd = &SetCommand{Option: "unknown", Value: 1}
	output, _, err := setCmd.Execute(state)
	require.NoError(t, err)
	assert.Contains(t, output, "Unknown")
	
	// Test ScrollCommand with multiple lines
	scrollCmd := &ScrollCommand{Down: true, Lines: 5}
	_, _, err = scrollCmd.Execute(state)
	require.NoError(t, err)
	
	scrollCmd = &ScrollCommand{Down: false, Lines: 3}
	_, _, err = scrollCmd.Execute(state)
	require.NoError(t, err)
	
	// Test ScrollCommand with 0 lines
	scrollCmd = &ScrollCommand{Down: true, Lines: 0}
	_, _, err = scrollCmd.Execute(state)
	require.NoError(t, err)
	
	// Test FormatCommand with different formatters
	fmtCmd := &FormatCommand{ColIdx: 0, FormatterName: "default"}
	_, _, err = fmtCmd.Execute(state)
	require.NoError(t, err)
	
	fmtCmd = &FormatCommand{ColIdx: 1, FormatterName: "hex"}
	_, _, err = fmtCmd.Execute(state)
	require.NoError(t, err)
	
	fmtCmd = &FormatCommand{ColIdx: 2, FormatterName: "rowid"}
	_, _, err = fmtCmd.Execute(state)
	require.NoError(t, err)
	
	// Test ColumnsCommand with various combinations
	colCmd := &ColumnsCommand{Columns: []uint16{}}
	_, _, err = colCmd.Execute(state)
	require.NoError(t, err)
	
	colCmd = &ColumnsCommand{Columns: []uint16{0, 1, 2, 3, 4, 5, 6, 7}}
	_, _, err = colCmd.Execute(state)
	require.NoError(t, err)
}

// TestFormatterNameFunction tests formatterName helper
func TestFormatterNameFunction(t *testing.T) {
	// Test with different formatters
	registry := objecttool.NewFormatterRegistry()
	
	// Set different formatters
	registry.SetFormatter(0, &DefaultFormatter{})
	registry.SetFormatter(1, &HexFormatter{})
	registry.SetFormatter(2, &RowidFormatter{})
	registry.SetFormatter(3, &TSFormatter{})
	registry.SetFormatter(4, &objecttool.ObjectStatsFormatter{})
	
	// Get names
	assert.Equal(t, "default", registry.GetFormatterName(0))
	assert.Equal(t, "hex", registry.GetFormatterName(1))
	assert.Equal(t, "rowid", registry.GetFormatterName(2))
	assert.Equal(t, "ts", registry.GetFormatterName(3))
	assert.Equal(t, "objectstats", registry.GetFormatterName(4))
	
	// Test with unset formatter
	assert.Equal(t, "auto", registry.GetFormatterName(100))
}


// TestReadBlockEdgeCases tests ReadBlock with edge cases
func TestReadBlockEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_readblock.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	blockCount := reader.BlockCount()
	
	// Test reading all blocks
	for i := uint32(0); i < blockCount; i++ {
		batch, release, err := reader.ReadBlock(ctx, i)
		require.NoError(t, err)
		require.NotNil(t, batch)
		assert.Greater(t, batch.RowCount(), 0)
		assert.Equal(t, 8, len(batch.Vecs))
		release()
	}
	
	// Test reading same block multiple times
	for i := 0; i < 3; i++ {
		batch, release, err := reader.ReadBlock(ctx, 0)
		require.NoError(t, err)
		require.NotNil(t, batch)
		release()
	}
	
	// Test invalid block index
	_, _, err = reader.ReadBlock(ctx, blockCount+10)
	assert.Error(t, err)
}


// TestPaginationComprehensive tests pagination thoroughly
func TestPaginationComprehensive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_pagination.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Set page size
	state.pageSize = 3
	
	// Test NextPage through all pages
	pageCount := 0
	for {
		rows, _, err := state.CurrentRows()
		require.NoError(t, err)
		if len(rows) == 0 {
			break
		}
		pageCount++
		
		err = state.NextPage()
		if err != nil {
			break
		}
		
		if pageCount > 20 {
			break
		}
	}
	assert.Greater(t, pageCount, 1)
	
	// Test PrevPage back to beginning
	for i := 0; i < 20; i++ {
		err = state.PrevPage()
		if err != nil {
			break
		}
	}
	
	// Should be at beginning
	offset := state.GlobalRowOffset()
	assert.Equal(t, int64(0), offset)
	
	// Test NextPage then PrevPage
	err = state.NextPage()
	require.NoError(t, err)
	
	err = state.PrevPage()
	require.NoError(t, err)
	
	offset = state.GlobalRowOffset()
	assert.Equal(t, int64(0), offset)
}

// TestScrollComprehensive tests scrolling thoroughly
func TestScrollComprehensive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_scroll.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	state.pageSize = 10
	
	// Test ScrollDown through all rows
	for i := 0; i < 25; i++ {
		err = state.ScrollDown()
		if err != nil {
			break
		}
	}
	
	// Test ScrollUp back to beginning
	for i := 0; i < 25; i++ {
		err = state.ScrollUp()
		if err != nil {
			break
		}
	}
	
	// Should be at beginning
	offset := state.GlobalRowOffset()
	assert.Equal(t, int64(0), offset)
	
	// Test ScrollDown then ScrollUp
	err = state.ScrollDown()
	require.NoError(t, err)
	
	err = state.ScrollUp()
	require.NoError(t, err)
	
	offset = state.GlobalRowOffset()
	assert.Equal(t, int64(0), offset)
	
	// Test ScrollDown multiple times
	for i := 0; i < 5; i++ {
		err = state.ScrollDown()
		require.NoError(t, err)
	}
	
	offset = state.GlobalRowOffset()
	assert.Equal(t, int64(5), offset)
	
	// Test ScrollUp multiple times
	for i := 0; i < 3; i++ {
		err = state.ScrollUp()
		require.NoError(t, err)
	}
	
	offset = state.GlobalRowOffset()
	assert.Equal(t, int64(2), offset)
}

// TestCurrentRowsComprehensive tests CurrentRows with various scenarios
func TestCurrentRowsComprehensive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_currentrows.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test with different page sizes
	pageSizes := []int{1, 3, 5, 10, 20, 100}
	for _, ps := range pageSizes {
		state.pageSize = ps
		rows, rowNumbers, err := state.CurrentRows()
		require.NoError(t, err)
		assert.Equal(t, len(rows), len(rowNumbers))
		if ps < 20 {
			assert.LessOrEqual(t, len(rows), ps)
		}
	}
	
	// Test with different visible columns
	visibleColsTests := [][]uint16{
		nil,
		{0},
		{0, 1},
		{0, 1, 2},
		{0, 2, 4, 6},
		{7},
		{},
	}
	
	for _, cols := range visibleColsTests {
		state.visibleCols = cols
		rows, _, err := state.CurrentRows()
		require.NoError(t, err)
		if len(rows) > 0 {
			if cols == nil {
				assert.Equal(t, 8, len(rows[0]))
			} else {
				assert.Equal(t, len(cols), len(rows[0]))
			}
		}
	}
	
	// Test with different formatters
	err = state.SetFormat(0, "hex")
	require.NoError(t, err)
	rows, _, err := state.CurrentRows()
	require.NoError(t, err)
	assert.NotEmpty(t, rows)
	
	err = state.SetFormat(1, "default")
	require.NoError(t, err)
	rows, _, err = state.CurrentRows()
	require.NoError(t, err)
	assert.NotEmpty(t, rows)
	
	// Test at different positions
	positions := []int64{0, 5, 10, 15, 19}
	for _, pos := range positions {
		err = state.GotoRow(pos)
		require.NoError(t, err)
		rows, _, err = state.CurrentRows()
		require.NoError(t, err)
		assert.NotEmpty(t, rows)
	}
}


// TestCrossBlockNavigation tests navigation across blocks
func TestCrossBlockNavigation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	// Create file with 9000 rows and 4 columns to ensure multiple blocks
	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_crossblock.obj", 9000, 4)
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	blockCount := reader.BlockCount()
	require.GreaterOrEqual(t, blockCount, uint32(2), "Need at least 2 blocks")
	
	// Set small page size
	state.pageSize = 2
	
	// Navigate to second block
	err = state.GotoBlock(1)
	require.NoError(t, err)
	
	// Go back using PrevPage
	err = state.PrevPage()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), state.currentBlock)
	
	// Navigate to second block again
	err = state.GotoBlock(1)
	require.NoError(t, err)
	state.rowOffset = 0
	
	// ScrollUp should go to last row of block 0
	err = state.ScrollUp()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), state.currentBlock)
	
	// Go to last row of block 0
	err = state.GotoBlock(0)
	require.NoError(t, err)
	err = state.ensureBlockLoaded()
	require.NoError(t, err)
	totalRows := state.currentBatch.RowCount()
	state.rowOffset = totalRows - 1
	
	// ScrollDown should go to first row of block 1
	err = state.ScrollDown()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), state.currentBlock)
	assert.Equal(t, 0, state.rowOffset)
	
	// Go to last page of block 0
	err = state.GotoBlock(0)
	require.NoError(t, err)
	err = state.ensureBlockLoaded()
	require.NoError(t, err)
	totalRows = state.currentBatch.RowCount()
	lastPageStart := (totalRows / state.pageSize) * state.pageSize
	if lastPageStart == totalRows && totalRows > 0 {
		lastPageStart -= state.pageSize
	}
	state.rowOffset = lastPageStart
	
	// NextPage should go to first page of block 1
	err = state.NextPage()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), state.currentBlock)
	assert.Equal(t, 0, state.rowOffset)
}

// TestPrevPageEdgeCases tests PrevPage edge cases
func TestPrevPageEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_prevpage.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test PrevPage at beginning
	err = state.PrevPage()
	assert.Error(t, err)
	
	// Test PrevPage within same block
	state.pageSize = 5
	_ = state.NextPage()
	_ = state.NextPage()
	
	err = state.PrevPage()
	assert.NoError(t, err)
}

// TestScrollUpEdgeCases tests ScrollUp edge cases
func TestScrollUpEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	
	objectPath := createTestObjectFile(t, tmpDir, "test_scrollup.obj")
	
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	// Test ScrollUp at beginning
	err = state.ScrollUp()
	assert.Error(t, err)
	
	// Test ScrollUp within same block
	_ = state.ScrollDown()
	_ = state.ScrollDown()
	
	err = state.ScrollUp()
	assert.NoError(t, err)
}
