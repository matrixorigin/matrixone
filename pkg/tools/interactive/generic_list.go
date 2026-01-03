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
	"fmt"
	"strings"
)

// ListOptions configures GenericList behavior (搭积木)
type ListOptions struct {
	// Display
	Headers       []string
	ShowRowNumber bool
	MaxColWidth   int

	// Features (enable/disable)
	EnableCursor bool
	EnableSearch bool
	EnableFilter bool

	// Pagination
	PageSize int
}

// GenericList wraps BaseRenderer with cursor, scroll, search, filter
// This is the "building block" layer on top of BaseRenderer
type GenericList struct {
	opts ListOptions

	// Data
	rows         [][]string
	rowNums      []string   // Custom row numbers (optional)
	rowNumLabel  string     // Row number column label
	filteredRows [][]string // nil means no filter

	// State
	cursor       int
	scrollOffset int
	screenHeight int
	screenWidth  int

	// Horizontal scroll
	hScrollOffset int

	// Search
	searchMatches map[int]bool
	searchQuery   string
	searchHistory *HistoryManager // Persistent search history

	// Filter
	filterActive bool
	filterInfo   string
}

// NewGenericList creates a GenericList with options
func NewGenericList(opts ListOptions) *GenericList {
	if opts.PageSize <= 0 {
		opts.PageSize = 20
	}

	searchHist := NewHistoryManager(100)
	// Try to load search history from file
	searchHist.LoadFromFile("search_history.txt")

	return &GenericList{
		opts:          opts,
		rowNumLabel:   "#", // Default label
		searchMatches: make(map[int]bool),
		searchHistory: searchHist,
	}
}

// SetData sets the data rows
func (v *GenericList) SetData(rows [][]string) {
	v.rows = rows
	v.rowNums = nil
	v.filteredRows = nil
	v.cursor = 0
	v.scrollOffset = 0
}

// SetDataWithRowNums sets data with custom row numbers
func (v *GenericList) SetDataWithRowNums(rows [][]string, rowNums []string) {
	v.rows = rows
	v.rowNums = rowNums
	v.filteredRows = nil
	v.cursor = 0
	v.scrollOffset = 0
}

// SetScreenHeight sets available screen height
func (v *GenericList) SetScreenHeight(h int) {
	v.screenHeight = h
	if v.screenHeight > 0 {
		// Reserve space for header, borders, status
		v.opts.PageSize = v.screenHeight - 6
		if v.opts.PageSize < 5 {
			v.opts.PageSize = 5
		}
	}
}

// SetScreenWidth sets available screen width
func (v *GenericList) SetScreenWidth(w int) {
	v.screenWidth = w
}

// === Cursor ===

func (v *GenericList) GetCursor() int { return v.cursor }

func (v *GenericList) SetCursor(pos int) {
	rows := v.visibleRows()
	if pos < 0 {
		pos = 0
	}
	if pos >= len(rows) {
		pos = len(rows) - 1
	}
	if pos < 0 {
		pos = 0
	}
	v.cursor = pos
	v.adjustScroll()
}

func (v *GenericList) MoveUp()   { v.SetCursor(v.cursor - 1) }
func (v *GenericList) MoveDown() { v.SetCursor(v.cursor + 1) }
func (v *GenericList) PageUp()   { v.SetCursor(v.cursor - v.opts.PageSize) }
func (v *GenericList) PageDown() { v.SetCursor(v.cursor + v.opts.PageSize) }
func (v *GenericList) GoTop()    { v.SetCursor(0) }
func (v *GenericList) GoBottom() { v.SetCursor(len(v.visibleRows()) - 1) }

func (v *GenericList) adjustScroll() {
	if v.cursor < v.scrollOffset {
		v.scrollOffset = v.cursor
	}
	if v.cursor >= v.scrollOffset+v.opts.PageSize {
		v.scrollOffset = v.cursor - v.opts.PageSize + 1
	}
	if v.scrollOffset < 0 {
		v.scrollOffset = 0
	}
}

// === Search ===

func (v *GenericList) Search(query string, matchFn func(row []string, query string) bool) {
	v.searchQuery = query
	v.searchMatches = make(map[int]bool)
	if query == "" || matchFn == nil {
		return
	}
	// Add to history and save to file
	v.searchHistory.Add(query)
	v.searchHistory.SaveToFile("search_history.txt")

	for i, row := range v.visibleRows() {
		if matchFn(row, query) {
			v.searchMatches[i] = true
		}
	}
}

func (v *GenericList) ClearSearch() {
	v.searchQuery = ""
	v.searchMatches = make(map[int]bool)
}

// GetSearchHistory returns search history
func (v *GenericList) GetSearchHistory() []string { return v.searchHistory.GetAll() }

// GetLastSearch returns last search query
func (v *GenericList) GetLastSearch() string {
	history := v.searchHistory.GetAll()
	if len(history) > 0 {
		return history[len(history)-1]
	}
	return ""
}

// HistoryUp moves to previous history entry
func (v *GenericList) HistoryUp() string {
	return v.searchHistory.Up()
}

// HistoryDown moves to next history entry
func (v *GenericList) HistoryDown() string {
	return v.searchHistory.Down()
}

// HistoryReset resets history index
func (v *GenericList) HistoryReset() {
	v.searchHistory.Reset()
}

// === Horizontal Scroll ===

func (v *GenericList) ScrollLeft() {
	if v.hScrollOffset > 0 {
		v.hScrollOffset--
	}
}

func (v *GenericList) ScrollRight() {
	totalCols := len(v.opts.Headers)
	if v.hScrollOffset < totalCols-1 {
		v.hScrollOffset++
	}
}

func (v *GenericList) SetHScroll(offset int) {
	if offset < 0 {
		offset = 0
	}
	totalCols := len(v.opts.Headers)
	if offset >= totalCols {
		offset = totalCols - 1
	}
	v.hScrollOffset = offset
}

func (v *GenericList) GetHScroll() int { return v.hScrollOffset }

func (v *GenericList) NextMatch() {
	for i := v.cursor + 1; i < len(v.visibleRows()); i++ {
		if v.searchMatches[i] {
			v.SetCursor(i)
			return
		}
	}
	// Wrap around
	for i := 0; i < v.cursor; i++ {
		if v.searchMatches[i] {
			v.SetCursor(i)
			return
		}
	}
}

func (v *GenericList) PrevMatch() {
	for i := v.cursor - 1; i >= 0; i-- {
		if v.searchMatches[i] {
			v.SetCursor(i)
			return
		}
	}
	// Wrap around
	for i := len(v.visibleRows()) - 1; i > v.cursor; i-- {
		if v.searchMatches[i] {
			v.SetCursor(i)
			return
		}
	}
}

func (v *GenericList) MatchCount() int { return len(v.searchMatches) }

// === Filter ===

func (v *GenericList) SetFilter(filterFn func(row []string) bool, info string) {
	if filterFn == nil {
		v.filteredRows = nil
		v.filterActive = false
		v.filterInfo = ""
	} else {
		v.filteredRows = make([][]string, 0)
		for _, row := range v.rows {
			if filterFn(row) {
				v.filteredRows = append(v.filteredRows, row)
			}
		}
		v.filterActive = true
		v.filterInfo = info
	}
	v.cursor = 0
	v.scrollOffset = 0
}

func (v *GenericList) ClearFilter() {
	v.SetFilter(nil, "")
}

func (v *GenericList) IsFiltered() bool { return v.filterActive }

// === Render ===

func (v *GenericList) visibleRows() [][]string {
	if v.filteredRows != nil {
		return v.filteredRows
	}
	return v.rows
}

func (v *GenericList) Render() string {
	rows := v.visibleRows()

	r := NewBaseRenderer()
	r.Headers = v.opts.Headers
	r.Rows = rows
	r.RowNums = v.rowNums
	r.RowNumLabel = v.rowNumLabel
	r.ShowBorder = true
	r.ShowRowNumber = v.opts.ShowRowNumber
	r.MaxColWidth = v.opts.MaxColWidth
	r.HScrollOffset = v.hScrollOffset

	// Calculate max visible columns based on screen width
	// Don't limit if screenWidth is not set
	if v.screenWidth > 0 {
		r.MaxVisibleCols = 15 // Default max
	} else {
		r.MaxVisibleCols = 0 // Show all
	}

	// Pagination
	r.StartRow = v.scrollOffset
	r.EndRow = v.scrollOffset + v.opts.PageSize
	r.RowNumOffset = 0

	// Cursor
	if v.opts.EnableCursor {
		r.SelectedRow = v.cursor
		r.RowDecorators = append(r.RowDecorators, CursorDecorator())
	}

	// Search highlights - highlight current match row with color
	if v.opts.EnableSearch && len(v.searchMatches) > 0 {
		// Highlight current cursor position if it's a match
		if v.searchMatches[v.cursor] {
			r.RowDecorators = append(r.RowDecorators, CurrentMatchHighlightDecorator(v.cursor))
		}
	}

	return r.Render()
}

// TotalRows returns total row count
func (v *GenericList) TotalRows() int { return len(v.visibleRows()) }

// GetStatus returns status line info
func (v *GenericList) GetStatus() string {
	rows := v.visibleRows()
	total := len(rows)
	if total == 0 {
		return "No data"
	}

	start := v.scrollOffset + 1
	end := v.scrollOffset + v.opts.PageSize
	if end > total {
		end = total
	}

	var parts []string
	if v.filterActive {
		parts = append(parts, "🔍 "+v.filterInfo)
	}
	if v.searchQuery != "" {
		parts = append(parts, fmt.Sprintf("Search: \"%s\" (%d matches)", v.searchQuery, len(v.searchMatches)))
	}
	parts = append(parts, fmt.Sprintf("[%d-%d of %d]", start, end, total))

	return strings.Join(parts, " │ ")
}
