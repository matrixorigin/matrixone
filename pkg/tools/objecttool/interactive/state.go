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

// State 浏览状态
type State struct {
	reader    *objecttool.ObjectReader
	formatter *objecttool.FormatterRegistry
	ctx       context.Context

	// 当前数据
	currentBlock uint32
	currentBatch *batch.Batch
	batchRelease func()

	// 显示状态
	rowOffset    int      // 当前block内的行偏移
	pageSize     int      // 每页显示行数
	visibleCols  []uint16 // 显示的列（nil表示全部）
	maxColWidth  int      // 最大列宽
	verticalMode bool     // 垂直显示模式
	colNames     map[uint16]string // 列重命名映射
}

func NewState(ctx context.Context, reader *objecttool.ObjectReader) *State {
	return &State{
		reader:      reader,
		formatter:   objecttool.NewFormatterRegistry(),
		ctx:         ctx,
		pageSize:    20,
		maxColWidth: 64, // 默认64字符
	}
}

// CurrentRows 获取当前页的数据（格式化后的字符串）
// 返回: rows, rowNumbers, error
func (s *State) CurrentRows() ([][]string, []string, error) {
	// 确保当前block已加载
	if err := s.ensureBlockLoaded(); err != nil {
		return nil, nil, err
	}

	if s.currentBatch == nil {
		return nil, nil, nil
	}

	// 计算当前页的行范围
	totalRows := s.currentBatch.RowCount()
	start := s.rowOffset
	end := start + s.pageSize
	if end > totalRows {
		end = totalRows
	}

	if start >= totalRows {
		return nil, nil, nil
	}

	// 格式化数据
	rows := make([][]string, end-start)
	rowNumbers := make([]string, end-start)
	cols := s.reader.Columns()
	
	// 确定要显示的列
	displayCols := s.visibleCols
	if displayCols == nil {
		// 显示所有列
		displayCols = make([]uint16, len(cols))
		for i := range cols {
			displayCols[i] = uint16(i)
		}
	}

	for i := start; i < end; i++ {
		// 生成行号 (block-offset)
		rowNumbers[i-start] = fmt.Sprintf("(%d-%d)", s.currentBlock, i)
		
		row := make([]string, len(displayCols))
		for j, colIdx := range displayCols {
			if int(colIdx) >= len(cols) {
				row[j] = ""
				continue
			}
			col := cols[colIdx]
			vec := s.currentBatch.Vecs[colIdx]

			// 检查NULL
			if vec.IsNull(uint64(i)) {
				row[j] = "NULL"
				continue
			}

			// 根据类型获取值
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

			// 获取格式化器
			formatter := s.formatter.GetFormatter(col.Idx, col.Type, value)
			formatted := formatter.Format(value)
			
			// 空字符串显示为空
			if formatted == "" {
				row[j] = ""
			} else {
				// 在 vertical 模式下不限制长度，在 table 模式下由 view 层处理
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

// NextPage 下一页
func (s *State) NextPage() error {
	if s.currentBatch == nil {
		return s.ensureBlockLoaded()
	}

	totalRows := s.currentBatch.RowCount()
	newOffset := s.rowOffset + s.pageSize

	if newOffset < totalRows {
		// 同一个block内
		s.rowOffset = newOffset
		return nil
	}

	// 需要切换到下一个block
	if s.currentBlock+1 >= s.reader.BlockCount() {
		return fmt.Errorf("already at last page")
	}

	s.releaseCurrentBatch()
	s.currentBlock++
	s.rowOffset = 0
	return s.ensureBlockLoaded()
}

// PrevPage 上一页
func (s *State) PrevPage() error {
	if s.rowOffset >= s.pageSize {
		// 同一个block内
		s.rowOffset -= s.pageSize
		return nil
	}

	// 需要切换到上一个block
	if s.currentBlock == 0 {
		return fmt.Errorf("already at first page")
	}

	s.releaseCurrentBatch()
	s.currentBlock--

	// 加载上一个block并定位到最后一页
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

// ScrollDown 向下滚动一行
func (s *State) ScrollDown() error {
	if s.currentBatch == nil {
		return s.ensureBlockLoaded()
	}

	totalRows := s.currentBatch.RowCount()
	// 如果当前行不是当前页的最后一行，且不是 block 的最后一行，则向下滚动一行
	if s.rowOffset+1 < totalRows {
		s.rowOffset++
		return nil
	}

	// 已经在当前 block 的最后一行，尝试切换到下一个 block
	if s.currentBlock+1 >= s.reader.BlockCount() {
		return fmt.Errorf("already at last row")
	}

	s.releaseCurrentBatch()
	s.currentBlock++
	s.rowOffset = 0  // 重置为下一个 block 的第一行
	return s.ensureBlockLoaded()
}

// ScrollUp 向上滚动一行
func (s *State) ScrollUp() error {
	if s.rowOffset > 0 {
		s.rowOffset--
		return nil
	}

	// 已经在当前 block 的第一行，尝试切换到上一个 block
	if s.currentBlock == 0 {
		return fmt.Errorf("already at first row")
	}

	s.releaseCurrentBatch()
	s.currentBlock--

	if err := s.ensureBlockLoaded(); err != nil {
		return err
	}

	totalRows := s.currentBatch.RowCount()
	s.rowOffset = totalRows - 1  // 跳到上一个 block 的最后一行
	if s.rowOffset < 0 {
		s.rowOffset = 0
	}

	return nil
}

// GotoRow 跳转到指定行（全局行号）
func (s *State) GotoRow(globalRow int64) error {
	if globalRow < 0 {
		// -1 表示最后一行
		info := s.reader.Info()
		globalRow = int64(info.RowCount) - 1
	}

	// 找到对应的block
	var currentRow int64
	for blockIdx := uint32(0); blockIdx < s.reader.BlockCount(); blockIdx++ {
		// 临时加载block获取行数
		bat, release, err := s.reader.ReadBlock(s.ctx, blockIdx)
		if err != nil {
			return err
		}
		blockRows := int64(bat.RowCount())
		release()

		if currentRow+blockRows > globalRow {
			// 找到了
			s.releaseCurrentBatch()
			s.currentBlock = blockIdx
			s.rowOffset = int(globalRow - currentRow)
			return s.ensureBlockLoaded()
		}

		currentRow += blockRows
	}

	return fmt.Errorf("row %d out of range", globalRow)
}

// GotoBlock 跳转到指定block
func (s *State) GotoBlock(blockIdx uint32) error {
	if blockIdx >= s.reader.BlockCount() {
		return fmt.Errorf("block %d out of range [0, %d)", blockIdx, s.reader.BlockCount())
	}
	
	s.releaseCurrentBatch()
	s.currentBlock = blockIdx
	s.rowOffset = 0
	return s.ensureBlockLoaded()
}

// SetFormat 设置列格式
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

// GlobalRowOffset 返回当前全局行偏移
func (s *State) GlobalRowOffset() int64 {
	var offset int64
	for i := uint32(0); i < s.currentBlock; i++ {
		// 这里简化处理，实际应该缓存每个block的行数
		// 暂时返回近似值
		offset += 8192 // 假设每个block 8192行
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

// Close 关闭状态
func (s *State) Close() {
	s.releaseCurrentBatch()
}
