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

	tea "github.com/charmbracelet/bubbletea"
)

// PageDataProvider defines how to get data for a page
type PageDataProvider interface {
	// GetRows returns all rows for the page
	GetRows() [][]string
	// GetHeaders returns column headers
	GetHeaders() []string
	// GetTitle returns page title
	GetTitle() string
	// GetOverview returns overview text (optional)
	GetOverview() string
	// GetHints returns help hints (optional)
	GetHints() string
}

// PageActionHandler defines how to handle actions on a page
type PageActionHandler interface {
	// HandleSelect handles row selection (Enter key)
	HandleSelect(rowIndex int) tea.Cmd
	// HandleCustomKey handles custom key presses
	HandleCustomKey(key string) tea.Cmd
	// CanFilter returns true if filtering is supported
	CanFilter() bool
	// ApplyFilter applies filter and returns filtered data provider
	ApplyFilter(filter string) PageDataProvider
	// CanSearch returns true if search is supported
	CanSearch() bool
	// Search performs search and returns match indices
	Search(query string) []int
}

// PageConfig defines page behavior
type PageConfig struct {
	EnableRowNumbers       bool
	EnableCursor           bool
	EnableSearch           bool
	EnableFilter           bool
	EnableHorizontalScroll bool
	MaxColWidth            int
	SearchHistoryFile      string
}

// Page is a complete page implementation with all features
type Page struct {
	config   PageConfig
	provider PageDataProvider
	handler  PageActionHandler

	// Internal state
	renderer      *TableRenderer
	cursor        int
	scrollOffset  int
	hScrollOffset int
	pageSize      int

	// Search state
	searchMode    bool
	searchInput   string
	searchHistory *HistoryManager
	searchMatches []int
	matchIndex    int

	// Filter state
	filterMode       bool
	filterInput      string
	filteredProvider PageDataProvider

	// Terminal size
	width  int
	height int
}

// NewPage creates a new page
func NewPage(config PageConfig, provider PageDataProvider, handler PageActionHandler) *Page {
	p := &Page{
		config:   config,
		provider: provider,
		handler:  handler,
		pageSize: 20, // default
	}

	// Initialize search history if enabled
	if config.EnableSearch && config.SearchHistoryFile != "" {
		p.searchHistory = NewHistoryManager(100) // max 100 entries
		// Try to load from file
		p.searchHistory.LoadFromFile(config.SearchHistoryFile)
	}

	p.updateRenderer()
	return p
}

// Update handles all page updates
func (p *Page) Update(msg tea.Msg) (*Page, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		p.width = msg.Width
		p.height = msg.Height
		p.updatePageSize()
		p.updateRenderer()

	case tea.KeyMsg:
		return p, p.handleKeyMsg(msg)
	}

	return p, nil
}

// Init initializes the page
func (p *Page) Init() tea.Cmd {
	return nil
}

// View renders the page
func (p *Page) View() string {
	if p.provider == nil {
		return "No data provider"
	}

	var b strings.Builder
	
	// Title
	b.WriteString(p.provider.GetTitle())
	b.WriteString("\n")
	
	// Overview
	b.WriteString(p.provider.GetOverview())
	b.WriteString("\n")
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")
	
	// Table content
	rows := p.provider.GetRows()
	headers := p.provider.GetHeaders()
	
	// Headers
	for i, header := range headers {
		if i > 0 {
			b.WriteString(" │ ")
		}
		b.WriteString(header)
	}
	b.WriteString("\n")
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")
	
	// Rows with pagination
	start := p.scrollOffset
	end := start + p.pageSize
	if end > len(rows) {
		end = len(rows)
	}
	
	for i := start; i < end; i++ {
		row := rows[i]
		cursor := " "
		if i == p.cursor {
			cursor = ">"
		}
		
		b.WriteString(cursor)
		for j, cell := range row {
			if j > 0 {
				b.WriteString(" │ ")
			}
			// Truncate long cells
			if len(cell) > 30 {
				b.WriteString(cell[:27] + "...")
			} else {
				b.WriteString(cell)
			}
		}
		b.WriteString("\n")
	}
	
	// Hints
	b.WriteString("\n")
	b.WriteString(p.provider.GetHints())
	
	return b.String()
}

// handleKeyMsg handles all key messages
func (p *Page) handleKeyMsg(msg tea.KeyMsg) tea.Cmd {
	key := msg.String()

	// Handle search mode
	if p.searchMode {
		return p.handleSearchKey(key)
	}

	// Handle filter mode
	if p.filterMode {
		return p.handleFilterKey(key)
	}

	// Handle navigation
	switch key {
	case "j", "down":
		p.moveDown()
	case "k", "up":
		p.moveUp()
	case "h", "left":
		if p.config.EnableHorizontalScroll {
			p.scrollLeft()
		}
	case "l", "right":
		if p.config.EnableHorizontalScroll {
			p.scrollRight()
		}
	case "enter":
		if p.handler != nil {
			return p.handler.HandleSelect(p.getCurrentRowIndex())
		}
	case "/":
		if p.config.EnableSearch && p.handler.CanSearch() {
			p.enterSearchMode()
		}
	case "f":
		if p.config.EnableFilter && p.handler.CanFilter() {
			p.enterFilterMode()
		}
	case "n":
		p.nextMatch()
	case "N":
		p.prevMatch()
	case "g":
		p.goToTop()
	case "G":
		p.goToBottom()
	case "ctrl+c", "q":
		return tea.Quit
	default:
		// Let handler process custom keys
		if p.handler != nil {
			return p.handler.HandleCustomKey(key)
		}
	}

	p.updateRenderer()
	return nil
}

// Render renders the complete page
func (p *Page) Render() string {
	var b strings.Builder

	// Get current data provider (filtered or original)
	currentProvider := p.getCurrentProvider()

	// Title
	b.WriteString(currentProvider.GetTitle())
	b.WriteString("\n")

	// Overview
	if overview := currentProvider.GetOverview(); overview != "" {
		b.WriteString(overview)
		b.WriteString("\n")
	}

	// Separator
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")

	// Table
	b.WriteString(p.renderer.Render())

	// Search status
	if len(p.searchMatches) > 0 {
		b.WriteString(fmt.Sprintf("\n🔍 Search: \"%s\" (%d/%d matches)",
			p.getLastSearchQuery(), p.matchIndex+1, len(p.searchMatches)))
	}

	// Filter status
	if p.filteredProvider != nil {
		b.WriteString(fmt.Sprintf(" │ 🔍 Filter: \"%s\"", p.filterInput))
	}

	// Input modes
	if p.searchMode {
		b.WriteString(fmt.Sprintf("\nSearch: %s", p.searchInput))
	} else if p.filterMode {
		b.WriteString(fmt.Sprintf("\nFilter: %s", p.filterInput))
	} else {
		// Hints
		if hints := currentProvider.GetHints(); hints != "" {
			b.WriteString("\n")
			b.WriteString(hints)
		}
	}

	return b.String()
}

// Helper methods
func (p *Page) updateRenderer() {
	currentProvider := p.getCurrentProvider()
	rows := currentProvider.GetRows()

	// Calculate visible rows
	start := p.scrollOffset
	end := start + p.pageSize
	if end > len(rows) {
		end = len(rows)
	}

	visibleRows := make([][]string, 0, end-start)
	if start < len(rows) {
		visibleRows = rows[start:end]
	}

	// Update renderer
	p.renderer = &TableRenderer{
		Headers:         currentProvider.GetHeaders(),
		Rows:            visibleRows,
		MaxColWidth:     p.config.MaxColWidth,
		ShowRowNumber:   p.config.EnableRowNumbers,
		RowNumLabel:     "#",
		RowNumberOffset: start, // Show actual indices
		HScrollOffset:   p.hScrollOffset,
		CursorEnabled:   p.config.EnableCursor,
		CursorPos:       p.cursor - start, // Relative to visible rows
		SearchEnabled:   len(p.searchMatches) > 0,
		SearchMatches:   p.getVisibleSearchMatches(start, end),
	}
}

func (p *Page) getCurrentProvider() PageDataProvider {
	if p.filteredProvider != nil {
		return p.filteredProvider
	}
	return p.provider
}

func (p *Page) getCurrentRowIndex() int {
	return p.cursor
}

func (p *Page) updatePageSize() {
	// Calculate available space for table rows
	reservedLines := 6 // title + overview + separator + hints + search status
	availableHeight := p.height - reservedLines
	if availableHeight < 5 {
		availableHeight = 5
	}
	if availableHeight > 50 {
		availableHeight = 50
	}
	p.pageSize = availableHeight
}

func (p *Page) moveDown() {
	rows := p.getCurrentProvider().GetRows()
	if p.cursor < len(rows)-1 {
		p.cursor++
		// Auto-scroll if needed
		if p.cursor >= p.scrollOffset+p.pageSize {
			p.scrollOffset = p.cursor - p.pageSize + 1
		}
	}
}

func (p *Page) moveUp() {
	if p.cursor > 0 {
		p.cursor--
		// Auto-scroll if needed
		if p.cursor < p.scrollOffset {
			p.scrollOffset = p.cursor
		}
	}
}

func (p *Page) scrollLeft() {
	if p.hScrollOffset > 0 {
		p.hScrollOffset--
	}
}

func (p *Page) scrollRight() {
	headers := p.getCurrentProvider().GetHeaders()
	if p.hScrollOffset < len(headers)-1 {
		p.hScrollOffset++
	}
}

func (p *Page) goToTop() {
	p.cursor = 0
	p.scrollOffset = 0
}

func (p *Page) goToBottom() {
	rows := p.getCurrentProvider().GetRows()
	if len(rows) > 0 {
		p.cursor = len(rows) - 1
		p.scrollOffset = max(0, p.cursor-p.pageSize+1)
	}
}

func (p *Page) enterSearchMode() {
	p.searchMode = true
	p.searchInput = ""
}

func (p *Page) enterFilterMode() {
	p.filterMode = true
	p.filterInput = ""
}

func (p *Page) handleSearchKey(key string) tea.Cmd {
	switch key {
	case "escape":
		p.searchMode = false
		p.searchInput = ""
	case "enter":
		p.performSearch()
		p.searchMode = false
	case "backspace":
		if len(p.searchInput) > 0 {
			p.searchInput = p.searchInput[:len(p.searchInput)-1]
		}
	case "up":
		if p.searchHistory != nil {
			p.searchInput = p.searchHistory.Up()
		}
	case "down":
		if p.searchHistory != nil {
			p.searchInput = p.searchHistory.Down()
		}
	default:
		if len(key) == 1 {
			p.searchInput += key
		}
	}
	return nil
}

func (p *Page) handleFilterKey(key string) tea.Cmd {
	switch key {
	case "escape":
		p.filterMode = false
		p.filterInput = ""
		p.filteredProvider = nil
	case "enter":
		p.applyFilter()
		p.filterMode = false
	case "backspace":
		if len(p.filterInput) > 0 {
			p.filterInput = p.filterInput[:len(p.filterInput)-1]
		}
	default:
		if len(key) == 1 {
			p.filterInput += key
		}
	}
	return nil
}

func (p *Page) performSearch() {
	if p.handler != nil && p.handler.CanSearch() && p.searchInput != "" {
		p.searchMatches = p.handler.Search(p.searchInput)
		p.matchIndex = 0

		// Save to history
		if p.searchHistory != nil {
			p.searchHistory.Add(p.searchInput)
		}

		// Jump to first match
		if len(p.searchMatches) > 0 {
			p.jumpToMatch(0)
		}
	}
}

func (p *Page) applyFilter() {
	if p.handler != nil && p.handler.CanFilter() && p.filterInput != "" {
		p.filteredProvider = p.handler.ApplyFilter(p.filterInput)
		p.cursor = 0
		p.scrollOffset = 0
	}
}

func (p *Page) nextMatch() {
	if len(p.searchMatches) > 0 {
		p.matchIndex = (p.matchIndex + 1) % len(p.searchMatches)
		p.jumpToMatch(p.matchIndex)
	}
}

func (p *Page) prevMatch() {
	if len(p.searchMatches) > 0 {
		p.matchIndex = (p.matchIndex - 1 + len(p.searchMatches)) % len(p.searchMatches)
		p.jumpToMatch(p.matchIndex)
	}
}

func (p *Page) jumpToMatch(matchIndex int) {
	if matchIndex < len(p.searchMatches) {
		p.cursor = p.searchMatches[matchIndex]
		// Center the match in view
		p.scrollOffset = max(0, p.cursor-p.pageSize/2)
	}
}

func (p *Page) getVisibleSearchMatches(start, end int) map[int]bool {
	visible := make(map[int]bool)
	for _, matchIdx := range p.searchMatches {
		if matchIdx >= start && matchIdx < end {
			visible[matchIdx-start] = true
		}
	}
	return visible
}

func (p *Page) getLastSearchQuery() string {
	if p.searchHistory != nil && len(p.searchHistory.GetAll()) > 0 {
		all := p.searchHistory.GetAll()
		return all[len(all)-1]
	}
	return ""
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
