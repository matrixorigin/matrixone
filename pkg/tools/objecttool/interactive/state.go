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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

// State represents the browsing state
type State struct {
	reader    *objecttool.ObjectReader
	formatter *objecttool.FormatterRegistry
	ctx       context.Context

	// Current data
	currentBlock uint32
	currentBatch *batch.Batch
	batchRelease func()

	// Display state
	rowOffset    int               // Row offset within current block
	pageSize     int               // Rows per page
	visibleCols  []uint16          // Visible columns (nil means all)
	maxColWidth  int               // Maximum column width
	verticalMode bool              // Vertical display mode
	colNames     map[uint16]string // Column rename mapping
}

func NewState(ctx context.Context, reader *objecttool.ObjectReader) *State {
	return &State{
		reader:      reader,
		formatter:   objecttool.NewFormatterRegistry(),
		ctx:         ctx,
		pageSize:    20,
		maxColWidth: 64, // Default 64 characters
	}
}

// CurrentRows returns the current page data (formatted strings)
// Returns: rows, rowNumbers, error
func (s *State) CurrentRows() ([][]string, []string, error) {
	// Ensure current block is loaded
	if err := s.ensureBlockLoaded(); err != nil {
		return nil, nil, err
	}

	if s.currentBatch == nil {
		return nil, nil, nil
	}

	// Calculate current page row range
	totalRows := s.currentBatch.RowCount()
	start := s.rowOffset
	end := start + s.pageSize
	if end > totalRows {
		end = totalRows
	}

	if start >= totalRows {
		return nil, nil, nil
	}

	// Format data
	rows := make([][]string, end-start)
	rowNumbers := make([]string, end-start)
	cols := s.reader.Columns()

	// Determine columns to display
	displayCols := s.visibleCols
	if displayCols == nil {
		// Display all columns
		displayCols = make([]uint16, len(cols))
		for i := range cols {
			displayCols[i] = uint16(i)
		}
	}

	for i := start; i < end; i++ {
		// Generate row number (block-offset)
		rowNumbers[i-start] = fmt.Sprintf("(%d-%d)", s.currentBlock, i)

		row := make([]string, len(displayCols))
		for j, colIdx := range displayCols {
			if int(colIdx) >= len(cols) {
				row[j] = ""
				continue
			}
			col := cols[colIdx]
			vec := s.currentBatch.Vecs[colIdx]

			// Check NULL
			if vec.IsNull(uint64(i)) {
				row[j] = "NULL"
				continue
			}

			// Get value by type
			var value any
			switch col.Type.Oid {
			case types.T_bool:
				value = vector.GetFixedAtNoTypeCheck[bool](vec, i)
			case types.T_int8:
				value = vector.GetFixedAtNoTypeCheck[int8](vec, i)
			case types.T_int16:
				value = vector.GetFixedAtNoTypeCheck[int16](vec, i)
			case types.T_int32:
				value = vector.GetFixedAtNoTypeCheck[int32](vec, i)
			case types.T_int64:
				value = vector.GetFixedAtNoTypeCheck[int64](vec, i)
			case types.T_uint8:
				value = vector.GetFixedAtNoTypeCheck[uint8](vec, i)
			case types.T_uint16:
				value = vector.GetFixedAtNoTypeCheck[uint16](vec, i)
			case types.T_uint32:
				value = vector.GetFixedAtNoTypeCheck[uint32](vec, i)
			case types.T_uint64:
				value = vector.GetFixedAtNoTypeCheck[uint64](vec, i)
			case types.T_float32:
				value = vector.GetFixedAtNoTypeCheck[float32](vec, i)
			case types.T_float64:
				value = vector.GetFixedAtNoTypeCheck[float64](vec, i)
			case types.T_char, types.T_varchar, types.T_text, types.T_blob, types.T_binary, types.T_varbinary:
				value = vec.GetBytesAt(i)
			case types.T_TS:
				value = vector.GetFixedAtNoTypeCheck[types.TS](vec, i)
			case types.T_Rowid:
				value = vector.GetFixedAtNoTypeCheck[types.Rowid](vec, i)
			case types.T_uuid:
				value = vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i)
			default:
				value = vec.GetRawBytesAt(i)
			}

			// Get formatter
			formatter := s.formatter.GetFormatter(col.Idx, col.Type, value)
			formatted := formatter.Format(value)

			// Empty string displays as empty
			if formatted == "" {
				row[j] = ""
			} else {
				// In vertical mode, don't limit length; in table mode, handled by view layer
				row[j] = formatted
			}
		}
		rows[i-start] = row
	}

	return rows, rowNumbers, nil
}

func (s *State) ensureBlockLoaded() error {
	if s.currentBatch != nil {
		return nil
	}

	bat, release, err := s.reader.ReadBlock(s.ctx, s.currentBlock)
	if err != nil {
		return err
	}

	s.currentBatch = bat
	s.batchRelease = release
	return nil
}

// NextPage moves to the next page
func (s *State) NextPage() error {
	if s.currentBatch == nil {
		return s.ensureBlockLoaded()
	}

	totalRows := s.currentBatch.RowCount()
	newOffset := s.rowOffset + s.pageSize

	if newOffset < totalRows {
		// Within the same block
		s.rowOffset = newOffset
		return nil
	}

	// Need to switch to next block
	if s.currentBlock+1 >= s.reader.BlockCount() {
		return fmt.Errorf("already at last page")
	}

	s.releaseCurrentBatch()
	s.currentBlock++
	s.rowOffset = 0
	return s.ensureBlockLoaded()
}

// PrevPage moves to the previous page
func (s *State) PrevPage() error {
	if s.rowOffset >= s.pageSize {
		// Within the same block
		s.rowOffset -= s.pageSize
		return nil
	}

	// Need to switch to previous block
	if s.currentBlock == 0 {
		return fmt.Errorf("already at first page")
	}

	s.releaseCurrentBatch()
	s.currentBlock--

	// Load previous block and position to last page
	if err := s.ensureBlockLoaded(); err != nil {
		return err
	}

	totalRows := s.currentBatch.RowCount()
	lastPageStart := (totalRows / s.pageSize) * s.pageSize
	if lastPageStart == totalRows && totalRows > 0 {
		lastPageStart -= s.pageSize
	}
	s.rowOffset = lastPageStart

	return nil
}

// ScrollDown scrolls down one line
func (s *State) ScrollDown() error {
	if s.currentBatch == nil {
		return s.ensureBlockLoaded()
	}

	totalRows := s.currentBatch.RowCount()
	// If current row is not the last row of current page and not the last row of block, scroll down one line
	if s.rowOffset+1 < totalRows {
		s.rowOffset++
		return nil
	}

	// Already at the last row of current block, try to switch to next block
	if s.currentBlock+1 >= s.reader.BlockCount() {
		return fmt.Errorf("already at last row")
	}

	s.releaseCurrentBatch()
	s.currentBlock++
	s.rowOffset = 0 // Reset to first row of next block
	return s.ensureBlockLoaded()
}

// ScrollUp scrolls up one line
func (s *State) ScrollUp() error {
	if s.rowOffset > 0 {
		s.rowOffset--
		return nil
	}

	// Already at the first row of current block, try to switch to previous block
	if s.currentBlock == 0 {
		return fmt.Errorf("already at first row")
	}

	s.releaseCurrentBatch()
	s.currentBlock--

	if err := s.ensureBlockLoaded(); err != nil {
		return err
	}

	totalRows := s.currentBatch.RowCount()
	s.rowOffset = totalRows - 1 // Jump to last row of previous block
	if s.rowOffset < 0 {
		s.rowOffset = 0
	}

	return nil
}

// GotoRow jumps to specified row (global row number)
func (s *State) GotoRow(globalRow int64) error {
	if globalRow < 0 {
		// -1 means last row
		info := s.reader.Info()
		globalRow = int64(info.RowCount) - 1
	}

	// Find corresponding block
	var currentRow int64
	for blockIdx := uint32(0); blockIdx < s.reader.BlockCount(); blockIdx++ {
		// Temporarily load block to get row count
		bat, release, err := s.reader.ReadBlock(s.ctx, blockIdx)
		if err != nil {
			return err
		}
		blockRows := int64(bat.RowCount())
		release()

		if currentRow+blockRows > globalRow {
			// Found
			s.releaseCurrentBatch()
			s.currentBlock = blockIdx
			s.rowOffset = int(globalRow - currentRow)
			return s.ensureBlockLoaded()
		}

		currentRow += blockRows
	}

	return fmt.Errorf("row %d out of range", globalRow)
}

// GotoBlock jumps to specified block
func (s *State) GotoBlock(blockIdx uint32) error {
	if blockIdx >= s.reader.BlockCount() {
		return fmt.Errorf("block %d out of range [0, %d)", blockIdx, s.reader.BlockCount())
	}

	s.releaseCurrentBatch()
	s.currentBlock = blockIdx
	s.rowOffset = 0
	return s.ensureBlockLoaded()
}

// SetFormat sets column format
func (s *State) SetFormat(colIdx uint16, formatterName string) error {
	if formatterName == "auto" {
		s.formatter.ClearFormatter(colIdx)
		return nil
	}

	formatter, ok := objecttool.FormatterByName[formatterName]
	if !ok {
		return fmt.Errorf("unknown formatter: %s", formatterName)
	}

	s.formatter.SetFormatter(colIdx, formatter)
	return nil
}

// GlobalRowOffset returns current global row offset
func (s *State) GlobalRowOffset() int64 {
	var offset int64
	for i := uint32(0); i < s.currentBlock; i++ {
		// Simplified handling here, should actually cache row count for each block
		// Temporarily return approximate value
		offset += 8192 // Assume 8192 rows per block
	}
	return offset + int64(s.rowOffset)
}

func (s *State) releaseCurrentBatch() {
	if s.batchRelease != nil {
		s.batchRelease()
		s.batchRelease = nil
	}
	s.currentBatch = nil
}

// Close closes the state
func (s *State) Close() {
	s.releaseCurrentBatch()
}
