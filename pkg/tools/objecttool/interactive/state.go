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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

// ViewMode represents the current view mode
type ViewMode int

const (
	ViewModeData ViewMode = iota
	ViewModeBlkMeta
	ViewModeObjMeta
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

	// Row range filter (for checkpoint range viewing)
	rowRangeStart int64 // Start row (0-based, -1 means no filter)
	rowRangeEnd   int64 // End row (inclusive, -1 means no filter)

	// View mode
	viewMode   ViewMode              // Current view mode
	metaRows   [][]string            // Metadata rows (for BlkMeta/ObjMeta mode)
	metaCols   []ColInfo             // Metadata columns schema
	metaOffset int                   // Offset for metadata rows
	blkSummary map[uint32]BlkSummary // Block summary info (for BlkMeta mode)

	// Block offsets cache (for fast search)
	blockOffsets []int64 // Starting row number for each block
}

// ColInfo contains column information
type ColInfo struct {
	Idx  uint16
	Name string
	Type types.Type
}

// BlkSummary contains summary information for a block
type BlkSummary struct {
	BlockID          uint32
	TotalOriginSize  uint32
	TotalCompSize    uint32
	CompressionRatio float64
}

// ObjSummary contains summary information for an object
type ObjSummary struct {
	TotalRows        uint64
	TotalNullCnt     uint64
	TotalOriginSize  uint64
	TotalCompSize    uint64
	CompressionRatio float64
}

func NewState(ctx context.Context, reader *objecttool.ObjectReader) *State {
	s := &State{
		reader:        reader,
		formatter:     objecttool.NewFormatterRegistry(),
		ctx:           ctx,
		pageSize:      20,
		maxColWidth:   64, // Default 64 characters
		rowRangeStart: -1,
		rowRangeEnd:   -1,
	}
	s.initBlockOffsets()
	return s
}

// initBlockOffsets initializes the block offsets cache
func (s *State) initBlockOffsets() {
	blockCount := s.reader.BlockCount()
	s.blockOffsets = make([]int64, blockCount)

	offset := int64(0)
	for i := uint32(0); i < blockCount; i++ {
		s.blockOffsets[i] = offset
		batch, release, err := s.reader.ReadBlock(s.ctx, i)
		if err == nil {
			offset += int64(batch.RowCount())
			release()
		}
	}
}

// GetBlockStartRow returns the starting row number for a block
func (s *State) GetBlockStartRow(blockIdx uint32) int64 {
	if blockIdx >= uint32(len(s.blockOffsets)) {
		return 0
	}
	return s.blockOffsets[blockIdx]
}

// SetRowRange sets the row range filter
func (s *State) SetRowRange(start, end int64) {
	s.rowRangeStart = start
	s.rowRangeEnd = end
}

// FilteredRowCount returns the number of rows after filtering
func (s *State) FilteredRowCount() int64 {
	total := int64(s.reader.Info().RowCount)
	if s.rowRangeStart < 0 && s.rowRangeEnd < 0 {
		return total
	}
	start := s.rowRangeStart
	if start < 0 {
		start = 0
	}
	end := s.rowRangeEnd
	if end < 0 || end >= total {
		end = total - 1
	}
	if end < start {
		return 0
	}
	return end - start + 1
}

// CurrentRows returns the current page data (formatted strings)
// Returns: rows, rowNumbers, error
func (s *State) CurrentRows() ([][]string, []string, error) {
	// Handle metadata modes
	if s.viewMode == ViewModeBlkMeta || s.viewMode == ViewModeObjMeta {
		return s.currentMetaRows()
	}

	// Data mode - original logic
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

	// Apply row range filter if set (convert global row numbers to block offsets)
	if s.rowRangeStart >= 0 || s.rowRangeEnd >= 0 {
		blockStartRow := s.GetBlockStartRow(s.currentBlock)

		// Convert global range to block-relative offsets
		if s.rowRangeStart >= 0 {
			localStart := int(s.rowRangeStart - blockStartRow)
			if localStart > start {
				start = localStart
			}
		}

		if s.rowRangeEnd >= 0 {
			localEnd := int(s.rowRangeEnd-blockStartRow) + 1
			if localEnd < end {
				end = localEnd
			}
		}
	}

	if start >= end || start >= totalRows {
		return nil, nil, nil
	}

	// Format data
	rows := make([][]string, end-start)
	rowNumbers := make([]string, end-start)
	cols := s.reader.Columns()

	// Return all columns data (not filtered by displayCols)
	// Filtering will be done at render time based on hScrollOffset
	allCols := make([]uint16, len(cols))
	for i := range cols {
		allCols[i] = uint16(i)
	}

	for i := start; i < end; i++ {
		// Generate row number (block-offset)
		rowNumbers[i-start] = fmt.Sprintf("(%d-%d)", s.currentBlock, i)

		row := make([]string, len(cols))
		for colIdx := range cols {
			col := cols[colIdx]
			vec := s.currentBatch.Vecs[colIdx]

			// Check NULL
			if vec.IsNull(uint64(i)) {
				row[colIdx] = "NULL"
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
				row[colIdx] = ""
			} else {
				// In vertical mode, don't limit length; in table mode, handled by view layer
				row[colIdx] = formatted
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
	// Metadata mode
	if s.viewMode == ViewModeBlkMeta || s.viewMode == ViewModeObjMeta {
		newOffset := s.metaOffset + s.pageSize
		if newOffset < len(s.metaRows) {
			s.metaOffset = newOffset
			return nil
		}
		return moerr.NewInternalErrorf(s.ctx, "already at last page")
	}

	// Data mode
	if s.currentBatch == nil {
		return s.ensureBlockLoaded()
	}

	totalRows := s.currentBatch.RowCount()

	// Apply row range filter
	maxRow := totalRows
	if s.rowRangeEnd >= 0 && int(s.rowRangeEnd)+1 < maxRow {
		maxRow = int(s.rowRangeEnd) + 1
	}

	newOffset := s.rowOffset + s.pageSize

	if newOffset < maxRow {
		// Within the same block and range
		s.rowOffset = newOffset
		return nil
	}

	// If row range is set, don't switch blocks
	if s.rowRangeStart >= 0 || s.rowRangeEnd >= 0 {
		return moerr.NewInternalErrorf(s.ctx, "already at last page")
	}

	// Need to switch to next block
	if s.currentBlock+1 >= s.reader.BlockCount() {
		return moerr.NewInternalErrorf(s.ctx, "already at last page")
	}

	s.releaseCurrentBatch()
	s.currentBlock++
	s.rowOffset = 0
	return s.ensureBlockLoaded()
}

// PrevPage moves to the previous page
func (s *State) PrevPage() error {
	// Metadata mode
	if s.viewMode == ViewModeBlkMeta || s.viewMode == ViewModeObjMeta {
		if s.metaOffset >= s.pageSize {
			s.metaOffset -= s.pageSize
			return nil
		}
		return moerr.NewInternalErrorf(s.ctx, "already at first page")
	}

	// Data mode
	if s.rowOffset >= s.pageSize {
		// Within the same block
		s.rowOffset -= s.pageSize
		return nil
	}

	// Need to switch to previous block
	if s.currentBlock == 0 {
		return moerr.NewInternalErrorf(s.ctx, "already at first page")
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
	// Metadata mode
	if s.viewMode == ViewModeBlkMeta || s.viewMode == ViewModeObjMeta {
		if s.metaOffset+1 < len(s.metaRows) {
			s.metaOffset++
			return nil
		}
		return moerr.NewInternalErrorf(s.ctx, "already at last row")
	}

	// Data mode
	if s.currentBatch == nil {
		return s.ensureBlockLoaded()
	}

	totalRows := s.currentBatch.RowCount()
	blockStartRow := s.GetBlockStartRow(s.currentBlock)

	// Check if we can scroll down within range filter
	if s.rowRangeEnd >= 0 {
		currentGlobalRow := blockStartRow + int64(s.rowOffset)
		if currentGlobalRow >= s.rowRangeEnd {
			return moerr.NewInternalErrorf(s.ctx, "already at last row of range")
		}
	}

	// If current row is not the last row, scroll down one line
	if s.rowOffset+1 < totalRows {
		s.rowOffset++
		return nil
	}

	// If row range is set, don't switch blocks
	if s.rowRangeStart >= 0 || s.rowRangeEnd >= 0 {
		return moerr.NewInternalErrorf(s.ctx, "already at last row of range")
	}

	// Already at the last row of current block, try to switch to next block
	if s.currentBlock+1 >= s.reader.BlockCount() {
		return moerr.NewInternalErrorf(s.ctx, "already at last row")
	}

	s.releaseCurrentBatch()
	s.currentBlock++
	s.rowOffset = 0 // Reset to first row of next block
	return s.ensureBlockLoaded()
}

// ScrollUp scrolls up one line
func (s *State) ScrollUp() error {
	// Metadata mode
	if s.viewMode == ViewModeBlkMeta || s.viewMode == ViewModeObjMeta {
		if s.metaOffset > 0 {
			s.metaOffset--
			return nil
		}
		return moerr.NewInternalErrorf(s.ctx, "already at first row")
	}

	// Data mode
	blockStartRow := s.GetBlockStartRow(s.currentBlock)

	// Check if we can scroll up within range filter
	if s.rowRangeStart >= 0 {
		currentGlobalRow := blockStartRow + int64(s.rowOffset)
		if currentGlobalRow <= s.rowRangeStart {
			return moerr.NewInternalErrorf(s.ctx, "already at first row of range")
		}
	}

	if s.rowOffset > 0 {
		s.rowOffset--
		return nil
	}

	// If row range is set, don't switch blocks
	if s.rowRangeStart >= 0 || s.rowRangeEnd >= 0 {
		return moerr.NewInternalErrorf(s.ctx, "already at first row of range")
	}

	// Already at the first row of current block, try to switch to previous block
	if s.currentBlock == 0 {
		return moerr.NewInternalErrorf(s.ctx, "already at first row")
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
		if s.rowRangeStart >= 0 || s.rowRangeEnd >= 0 {
			// With range filter: go to last row in range
			if s.rowRangeEnd >= 0 {
				globalRow = s.rowRangeEnd
			} else {
				info := s.reader.Info()
				globalRow = int64(info.RowCount) - 1
			}
		} else {
			// Without filter: go to global last row
			info := s.reader.Info()
			globalRow = int64(info.RowCount) - 1
		}
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

	return moerr.NewInternalErrorf(s.ctx, "row %d out of range", globalRow)
}

// GotoBlock jumps to specified block
func (s *State) GotoBlock(blockIdx uint32) error {
	if blockIdx >= s.reader.BlockCount() {
		return moerr.NewInternalErrorf(s.ctx, "block %d out of range [0, %d)", blockIdx, s.reader.BlockCount())
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
		return moerr.NewInternalErrorf(s.ctx, "unknown formatter: %s", formatterName)
	}

	s.formatter.SetFormatter(colIdx, formatter)
	return nil
}

// GlobalRowOffset returns current global row offset
func (s *State) GlobalRowOffset() int64 {
	return s.GetBlockStartRow(s.currentBlock) + int64(s.rowOffset)
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

// Mode switching methods

func (s *State) SwitchToData() {
	s.viewMode = ViewModeData
	s.metaOffset = 0
}

func (s *State) SwitchToBlkMeta() error {
	s.viewMode = ViewModeBlkMeta
	s.metaCols = s.blkMetaSchema()
	s.metaRows, s.blkSummary = s.buildBlkMetaRows()
	s.metaOffset = 0
	return nil
}

func (s *State) SwitchToObjMeta() error {
	s.viewMode = ViewModeObjMeta
	s.metaCols = s.objMetaSchema()
	s.metaRows = s.buildObjMetaRows()
	s.metaOffset = 0
	return nil
}

// Schema definitions

func (s *State) blkMetaSchema() []ColInfo {
	return []ColInfo{
		{Idx: 0, Name: "ColIdx", Type: types.T_uint16.ToType()},
		{Idx: 1, Name: "Name", Type: types.T_varchar.ToType()},
		{Idx: 2, Name: "OriginSize", Type: types.T_uint32.ToType()},
		{Idx: 3, Name: "CompSize", Type: types.T_uint32.ToType()},
		{Idx: 4, Name: "CompRatio", Type: types.T_float64.ToType()},
		{Idx: 5, Name: "Rows", Type: types.T_uint32.ToType()},
		{Idx: 6, Name: "NullCnt", Type: types.T_uint32.ToType()},
		{Idx: 7, Name: "Min", Type: types.T_varchar.ToType()},
		{Idx: 8, Name: "Max", Type: types.T_varchar.ToType()},
	}
}

func (s *State) objMetaSchema() []ColInfo {
	return []ColInfo{
		{Idx: 0, Name: "Property", Type: types.T_varchar.ToType()},
		{Idx: 1, Name: "Value", Type: types.T_varchar.ToType()},
	}
}

// Data building methods

func (s *State) buildBlkMetaRows() ([][]string, map[uint32]BlkSummary) {
	rows := make([][]string, 0)
	summary := make(map[uint32]BlkSummary)
	info := s.reader.Info()
	meta := s.reader.Meta()

	for blockID := uint32(0); blockID < info.BlockCount; blockID++ {
		blockMeta := meta.GetBlockMeta(blockID)
		var blockTotalOriginSize uint32
		var blockTotalCompSize uint32

		for colIdx := uint16(0); colIdx < info.ColCount; colIdx++ {
			colMeta := blockMeta.ColumnMeta(colIdx)
			colName := s.getColumnName(colIdx)

			originSize := colMeta.Location().OriginSize()
			compSize := colMeta.Location().Length()
			nullCnt := colMeta.NullCnt()
			blockTotalOriginSize += originSize
			blockTotalCompSize += compSize

			// Calculate compression ratio for this column
			colCompRatio := float64(0)
			if originSize > 0 {
				colCompRatio = float64(compSize) / float64(originSize) * 100
			}

			// Format Min/Max as hex if binary
			zm := colMeta.ZoneMap()
			minBuf := zm.GetMinBuf()
			maxBuf := zm.GetMaxBuf()

			// Check if ZoneMap is initialized and buffers are valid
			var minStr, maxStr string
			if len(zm) == 0 || !zm.IsInited() {
				// ZoneMap not initialized
				minStr = "N/A"
				maxStr = "N/A"
			} else {
				// ZoneMap initialized, check if min/max are empty
				if len(minBuf) == 0 {
					minStr = "<empty>"
				} else {
					minStr = fmt.Sprintf("%x", minBuf)
				}
				if len(maxBuf) == 0 {
					maxStr = "<empty>"
				} else {
					maxStr = fmt.Sprintf("%x", maxBuf)
				}
			}

			row := []string{
				fmt.Sprintf("%d", colIdx),
				colName,
				fmt.Sprintf("%s (%d)", formatSize(uint64(originSize)), originSize),
				fmt.Sprintf("%s (%d)", formatSize(uint64(compSize)), compSize),
				fmt.Sprintf("%.2f%%", colCompRatio),
				fmt.Sprintf("%d", blockMeta.GetRows()),
				fmt.Sprintf("%d", nullCnt),
				minStr,
				maxStr,
			}
			rows = append(rows, row)
		}

		// Store summary for this block (not in table)
		compressionRatio := float64(0)
		if blockTotalOriginSize > 0 {
			compressionRatio = float64(blockTotalCompSize) / float64(blockTotalOriginSize) * 100
		}
		summary[blockID] = BlkSummary{
			BlockID:          blockID,
			TotalOriginSize:  blockTotalOriginSize,
			TotalCompSize:    blockTotalCompSize,
			CompressionRatio: compressionRatio,
		}
	}

	return rows, summary
}

func (s *State) buildObjMetaRows() [][]string {
	info := s.reader.Info()
	meta := s.reader.Meta()

	// Calculate totals across all blocks and columns
	var totalOriginSize uint64
	var totalCompSize uint64
	var totalNullCnt uint64

	// Per-column statistics
	colStats := make(map[uint16]struct {
		totalOriginSize uint64
		totalCompSize   uint64
		totalNullCnt    uint64
		minBuf          []byte
		maxBuf          []byte
		hasInitialized  bool
	})

	// Iterate through all blocks and columns to collect data
	for blockID := uint32(0); blockID < info.BlockCount; blockID++ {
		blockMeta := meta.GetBlockMeta(blockID)
		for colIdx := uint16(0); colIdx < info.ColCount; colIdx++ {
			colMeta := blockMeta.ColumnMeta(colIdx)
			originSize := uint64(colMeta.Location().OriginSize())
			compSize := uint64(colMeta.Location().Length())
			nullCnt := uint64(colMeta.NullCnt())

			totalOriginSize += originSize
			totalCompSize += compSize
			totalNullCnt += nullCnt

			// Collect per-column statistics
			stats := colStats[colIdx]
			stats.totalOriginSize += originSize
			stats.totalCompSize += compSize
			stats.totalNullCnt += nullCnt

			// Collect min/max for each column (across all blocks)
			zm := colMeta.ZoneMap()
			minBuf := zm.GetMinBuf()
			maxBuf := zm.GetMaxBuf()

			// Check if ZoneMap is initialized
			hasInitialized := len(zm) > 0 && zm.IsInited()

			if hasInitialized {
				stats.hasInitialized = true
				// Update min: prefer non-empty values
				if len(minBuf) > 0 {
					if len(stats.minBuf) == 0 || string(minBuf) < string(stats.minBuf) {
						stats.minBuf = minBuf
					}
				} else {
					// Current is empty: only update if existing is also empty
					if len(stats.minBuf) == 0 {
						stats.minBuf = minBuf
					}
				}
				// Update max: prefer non-empty values
				if len(maxBuf) > 0 {
					if len(stats.maxBuf) == 0 || string(maxBuf) > string(stats.maxBuf) {
						stats.maxBuf = maxBuf
					}
				} else {
					// Current is empty: only update if existing is also empty
					if len(stats.maxBuf) == 0 {
						stats.maxBuf = maxBuf
					}
				}
			}
			colStats[colIdx] = stats
		}
	}

	// Calculate global compression ratio
	globalCompRatio := float64(0)
	if totalOriginSize > 0 {
		globalCompRatio = float64(totalCompSize) / float64(totalOriginSize) * 100
	}

	rows := [][]string{
		{"Path", info.Path},
		{"Blocks", fmt.Sprintf("%d", info.BlockCount)},
		{"Columns", fmt.Sprintf("%d", info.ColCount)},
		{"Sorted", fmt.Sprintf("%v", info.IsSorted)},
		{"Appendable", fmt.Sprintf("%v", info.IsAppendable)},
	}

	// Add global summary row
	rows = append(rows, []string{
		"TotalRows",
		fmt.Sprintf("%d", info.RowCount),
	})
	rows = append(rows, []string{
		"TotalNullCount",
		fmt.Sprintf("%d", totalNullCnt),
	})
	rows = append(rows, []string{
		"TotalOriginSize",
		fmt.Sprintf("%s (%d)", formatSize(totalOriginSize), totalOriginSize),
	})
	rows = append(rows, []string{
		"TotalCompSize",
		fmt.Sprintf("%s (%d)", formatSize(totalCompSize), totalCompSize),
	})
	rows = append(rows, []string{
		"CompressionRatio",
		fmt.Sprintf("%.2f%%", globalCompRatio),
	})

	// Add per-column statistics
	for colIdx := uint16(0); colIdx < info.ColCount; colIdx++ {
		colName := s.getColumnName(colIdx)
		stats := colStats[colIdx]

		// Calculate column compression ratio
		colCompRatio := float64(0)
		if stats.totalOriginSize > 0 {
			colCompRatio = float64(stats.totalCompSize) / float64(stats.totalOriginSize) * 100
		}

		// Column summary
		rows = append(rows, []string{
			fmt.Sprintf("%s.TotalOriginSize", colName),
			fmt.Sprintf("%s (%d)", formatSize(stats.totalOriginSize), stats.totalOriginSize),
		})
		rows = append(rows, []string{
			fmt.Sprintf("%s.TotalCompSize", colName),
			fmt.Sprintf("%s (%d)", formatSize(stats.totalCompSize), stats.totalCompSize),
		})
		rows = append(rows, []string{
			fmt.Sprintf("%s.CompressionRatio", colName),
			fmt.Sprintf("%.2f%%", colCompRatio),
		})
		rows = append(rows, []string{
			fmt.Sprintf("%s.TotalNullCount", colName),
			fmt.Sprintf("%d", stats.totalNullCnt),
		})

		// Column min/max
		var minStr, maxStr string
		if !stats.hasInitialized {
			minStr = "N/A"
			maxStr = "N/A"
		} else {
			if len(stats.minBuf) == 0 {
				minStr = "<empty>"
			} else {
				minStr = fmt.Sprintf("%x", stats.minBuf)
			}
			if len(stats.maxBuf) == 0 {
				maxStr = "<empty>"
			} else {
				maxStr = fmt.Sprintf("%x", stats.maxBuf)
			}
		}
		rows = append(rows, []string{
			fmt.Sprintf("%s.Min", colName),
			minStr,
		})
		rows = append(rows, []string{
			fmt.Sprintf("%s.Max", colName),
			maxStr,
		})
	}

	return rows
}

func (s *State) getColumnName(colIdx uint16) string {
	if s.colNames != nil {
		if name, exists := s.colNames[colIdx]; exists {
			return name
		}
	}
	return fmt.Sprintf("Col%d", colIdx)
}

func (s *State) currentMetaRows() ([][]string, []string, error) {
	if len(s.metaRows) == 0 {
		return nil, nil, nil
	}

	start := s.metaOffset
	end := start + s.pageSize
	if end > len(s.metaRows) {
		end = len(s.metaRows)
	}

	if start >= len(s.metaRows) {
		return nil, nil, nil
	}

	rows := s.metaRows[start:end]
	rowNumbers := make([]string, len(rows))

	// For blkmeta, use block number from the first column
	// For objmeta, use property name from the first column
	for i := range rows {
		if s.viewMode == ViewModeBlkMeta {
			// Calculate block number from row index
			info := s.reader.Info()
			rowIdx := start + i
			blockNo := rowIdx / int(info.ColCount)
			rowNumbers[i] = fmt.Sprintf("%d", blockNo)
		} else if s.viewMode == ViewModeObjMeta {
			// Use the property name (first column)
			if len(rows[i]) > 0 {
				rowNumbers[i] = rows[i][0]
			} else {
				rowNumbers[i] = fmt.Sprintf("(%d)", start+i)
			}
		} else {
			rowNumbers[i] = fmt.Sprintf("(%d)", start+i)
		}
	}

	return rows, rowNumbers, nil
}

// Columns returns column information based on current mode
func (s *State) Columns() []objecttool.ColInfo {
	if s.viewMode == ViewModeData {
		return s.reader.Columns()
	}

	// Convert internal ColInfo to objecttool.ColInfo
	cols := make([]objecttool.ColInfo, len(s.metaCols))
	for i, col := range s.metaCols {
		cols[i] = objecttool.ColInfo{
			Idx:  col.Idx,
			Type: col.Type,
		}
	}
	return cols
}
