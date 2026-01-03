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
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// PageConfig defines what features a page has (搭积木配置)
type PageConfig struct {
	// Content
	Title   string   // Page title
	Headers []string // Table headers

	// Features (enable/disable)
	ShowRowNumber bool
	RowNumLabel   string // Row number column label (default: "#")
	EnableCursor  bool
	EnableSearch  bool
	EnableFilter  bool
	EnableBack    bool // Show back hint
	EnableHScroll bool // Horizontal scroll

	// Display
	MaxColWidth int

	// Hints (auto-generated if empty)
	CustomHints string
}

// DataProvider provides data for a page (数据源接口)
// 实现这个接口就可以接入任何数据源
type DataProvider interface {
	GetRows() [][]string
	GetRowNums() []string // Optional custom row numbers (nil = auto)
	GetOverview() string  // Dynamic overview line
}

// ActionHandler handles page actions (行为接口)
// 实现这个接口就可以定制页面行为
type ActionHandler interface {
	OnSelect(rowIdx int) tea.Cmd                // Enter key
	OnBack() tea.Cmd                            // ESC/b key
	OnCustomKey(key string) tea.Cmd             // Other keys
	MatchRow(row []string, query string) bool   // For search
	FilterRow(row []string, filter string) bool // For filter
}

// DefaultHandler provides default implementations for common behaviors
// 大多数页面可以嵌入这个，只覆盖需要定制的方法
type DefaultHandler struct {
	OnSelectFunc    func(rowIdx int) tea.Cmd
	OnBackFunc      func() tea.Cmd
	OnCustomKeyFunc func(key string) tea.Cmd
}

func (h *DefaultHandler) OnSelect(rowIdx int) tea.Cmd {
	if h.OnSelectFunc != nil {
		return h.OnSelectFunc(rowIdx)
	}
	return nil
}

func (h *DefaultHandler) OnBack() tea.Cmd {
	if h.OnBackFunc != nil {
		return h.OnBackFunc()
	}
	return nil
}

func (h *DefaultHandler) OnCustomKey(key string) tea.Cmd {
	if h.OnCustomKeyFunc != nil {
		return h.OnCustomKeyFunc(key)
	}
	return nil
}

// MatchRow default: case-insensitive substring match on all columns
func (h *DefaultHandler) MatchRow(row []string, query string) bool {
	query = strings.ToLower(query)
	for _, cell := range row {
		if strings.Contains(strings.ToLower(cell), query) {
			return true
		}
	}
	return false
}

// FilterRow default: same as MatchRow
func (h *DefaultHandler) FilterRow(row []string, filter string) bool {
	return h.MatchRow(row, filter)
}

// GenericPage is the highest level component
// All pages use this: just provide config + data + handler
type GenericPage struct {
	config   PageConfig
	provider DataProvider
	handler  ActionHandler
	list     *GenericList

	// Input mode
	inputMode   string // "", "search", "filter"
	inputBuffer string
	inputCursor int // Cursor position in input buffer

	// Screen size
	width  int
	height int
}

// NewGenericPage creates a page with config
func NewGenericPage(config PageConfig, provider DataProvider, handler ActionHandler) *GenericPage {
	listOpts := ListOptions{
		Headers:       config.Headers,
		ShowRowNumber: config.ShowRowNumber,
		EnableCursor:  config.EnableCursor,
		EnableSearch:  config.EnableSearch,
		EnableFilter:  config.EnableFilter,
		MaxColWidth:   config.MaxColWidth,
		PageSize:      20,
	}

	p := &GenericPage{
		config:   config,
		provider: provider,
		handler:  handler,
		list:     NewGenericList(listOpts),
		width:    120,
		height:   30,
	}

	// Set row number label if provided
	if config.RowNumLabel != "" {
		p.list.rowNumLabel = config.RowNumLabel
	}

	// Load initial data
	p.Refresh()
	return p
}

// Refresh reloads data from provider
func (p *GenericPage) Refresh() {
	if p.provider != nil {
		rows := p.provider.GetRows()
		rowNums := p.provider.GetRowNums()
		if rowNums != nil {
			p.list.SetDataWithRowNums(rows, rowNums)
		} else {
			p.list.SetData(rows)
		}
	}
}

// SetSize sets terminal size
func (p *GenericPage) SetSize(w, h int) {
	p.width = w
	p.height = h
	// Reserve: title(1) + overview(1) + separator(1) + hints(2) + status(1) = 6
	p.list.SetScreenHeight(h - 6)
	p.list.SetScreenWidth(w)
}

// === Bubbletea Interface ===

func (p *GenericPage) Init() tea.Cmd {
	return nil
}

func (p *GenericPage) Update(msg tea.Msg) (*GenericPage, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		p.SetSize(msg.Width, msg.Height)
		return p, nil

	case tea.KeyMsg:
		return p.handleKey(msg)
	}
	return p, nil
}

func (p *GenericPage) handleKey(msg tea.KeyMsg) (*GenericPage, tea.Cmd) {
	key := msg.String()

	// Input mode handling
	if p.inputMode != "" {
		return p.handleInputMode(key)
	}

	// Normal mode
	switch key {
	case "q":
		return p, tea.Quit
	case "j", "down":
		p.list.MoveDown()
	case "k", "up":
		p.list.MoveUp()
	case "h", "left":
		if p.config.EnableHScroll {
			p.list.ScrollLeft()
		}
	case "l", "right":
		if p.config.EnableHScroll {
			p.list.ScrollRight()
		}
	case "g":
		p.list.GoTop()
	case "G":
		p.list.GoBottom()
	case "ctrl+f", "pgdown":
		p.list.PageDown()
	case "ctrl+b", "pgup":
		p.list.PageUp()
	case "enter":
		if p.handler != nil {
			return p, p.handler.OnSelect(p.list.GetCursor())
		}
	case "esc", "b":
		if p.config.EnableBack && p.handler != nil {
			return p, p.handler.OnBack()
		}
	case "/":
		if p.config.EnableSearch {
			p.inputMode = "search"
			p.inputBuffer = ""
			p.inputCursor = 0
			p.list.HistoryReset() // Reset history navigation
		}
	case "f":
		if p.config.EnableFilter {
			p.inputMode = "filter"
			p.inputBuffer = ""
			p.inputCursor = 0
		}
	case "n":
		if p.config.EnableSearch {
			p.list.NextMatch()
		}
	case "N":
		if p.config.EnableSearch {
			p.list.PrevMatch()
		}
	default:
		if p.handler != nil {
			return p, p.handler.OnCustomKey(key)
		}
	}
	return p, nil
}

func (p *GenericPage) handleInputMode(key string) (*GenericPage, tea.Cmd) {
	switch key {
	case "esc":
		p.inputMode = ""
		p.inputBuffer = ""
		p.inputCursor = 0
		p.list.HistoryReset()
	case "enter":
		if p.inputMode == "search" && p.handler != nil {
			p.list.Search(p.inputBuffer, p.handler.MatchRow)
			p.list.NextMatch()
		} else if p.inputMode == "filter" && p.handler != nil {
			if p.inputBuffer == "" {
				p.list.ClearFilter()
			} else {
				p.list.SetFilter(func(row []string) bool {
					return p.handler.FilterRow(row, p.inputBuffer)
				}, "Filter: "+p.inputBuffer)
			}
		}
		p.inputMode = ""
		p.inputCursor = 0
		p.list.HistoryReset()
	case "backspace":
		if p.inputCursor > 0 {
			p.inputBuffer = p.inputBuffer[:p.inputCursor-1] + p.inputBuffer[p.inputCursor:]
			p.inputCursor--
		}
	case "delete":
		if p.inputCursor < len(p.inputBuffer) {
			p.inputBuffer = p.inputBuffer[:p.inputCursor] + p.inputBuffer[p.inputCursor+1:]
		}
	case "left":
		if p.inputCursor > 0 {
			p.inputCursor--
		}
	case "right":
		if p.inputCursor < len(p.inputBuffer) {
			p.inputCursor++
		}
	case "home", "ctrl+a":
		p.inputCursor = 0
	case "end", "ctrl+e":
		p.inputCursor = len(p.inputBuffer)
	case "ctrl+u":
		// Delete from cursor to beginning
		p.inputBuffer = p.inputBuffer[p.inputCursor:]
		p.inputCursor = 0
	case "ctrl+k":
		// Delete from cursor to end
		p.inputBuffer = p.inputBuffer[:p.inputCursor]
	case "ctrl+w":
		// Delete word before cursor
		if p.inputCursor > 0 {
			// Find start of word (skip trailing spaces, then find word boundary)
			pos := p.inputCursor
			for pos > 0 && p.inputBuffer[pos-1] == ' ' {
				pos--
			}
			for pos > 0 && p.inputBuffer[pos-1] != ' ' {
				pos--
			}
			p.inputBuffer = p.inputBuffer[:pos] + p.inputBuffer[p.inputCursor:]
			p.inputCursor = pos
		}
	case "up":
		// Browse search history using HistoryManager
		if p.inputMode == "search" {
			if entry := p.list.HistoryUp(); entry != "" {
				p.inputBuffer = entry
				p.inputCursor = len(p.inputBuffer)
			}
		}
	case "down":
		// Browse search history
		if p.inputMode == "search" {
			entry := p.list.HistoryDown()
			p.inputBuffer = entry
			p.inputCursor = len(p.inputBuffer)
		}
	default:
		// Support paste - handle bracketed paste and filter control chars
		input := key

		// Handle bracketed paste: remove [...]  wrapping
		if strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]") && len(input) > 2 {
			input = input[1 : len(input)-1]
		}

		// Filter out control characters, keep only printable ASCII
		var filtered strings.Builder
		for _, r := range input {
			if r >= 32 && r <= 126 { // Printable ASCII including space
				filtered.WriteRune(r)
			}
		}

		if filtered.Len() > 0 {
			text := filtered.String()
			// Insert at cursor position
			p.inputBuffer = p.inputBuffer[:p.inputCursor] + text + p.inputBuffer[p.inputCursor:]
			p.inputCursor += len(text)
		}
	}
	return p, nil
}

func (p *GenericPage) View() string {
	var b strings.Builder

	// Title
	if p.config.Title != "" {
		b.WriteString(p.config.Title)
		b.WriteString("\n")
	}

	// Overview (from provider, dynamic)
	if p.provider != nil {
		if overview := p.provider.GetOverview(); overview != "" {
			b.WriteString(overview)
			b.WriteString("\n")
		}
	}

	// Separator
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")

	// Table
	b.WriteString(p.list.Render())

	// Status
	b.WriteString("\n")
	b.WriteString(p.list.GetStatus())

	// Input mode or hints
	b.WriteString("\n")
	if p.inputMode != "" {
		prompt := "Search"
		if p.inputMode == "filter" {
			prompt = "Filter"
		}
		// Show input with cursor
		before := p.inputBuffer[:p.inputCursor]
		after := p.inputBuffer[p.inputCursor:]
		b.WriteString(prompt + ": " + before + "█" + after)
	} else {
		b.WriteString(p.getHints())
	}

	return b.String()
}

func (p *GenericPage) getHints() string {
	if p.config.CustomHints != "" {
		return p.config.CustomHints
	}

	var hints []string
	if p.config.EnableCursor {
		hints = append(hints, "[j/k] Navigate", "[Enter] Select")
	}
	if p.config.EnableHScroll {
		hints = append(hints, "[h/l] Scroll")
	}
	if p.config.EnableSearch {
		hints = append(hints, "[/] Search", "[n/N] Next/Prev")
	}
	if p.config.EnableFilter {
		hints = append(hints, "[f] Filter")
	}
	if p.config.EnableBack {
		hints = append(hints, "[b/ESC] Back")
	}
	hints = append(hints, "[q] Quit")

	return strings.Join(hints, "  ")
}

// GetCursor returns current cursor position
func (p *GenericPage) GetCursor() int {
	return p.list.GetCursor()
}

// GetSelectedRow returns the row at cursor
func (p *GenericPage) GetSelectedRow() []string {
	rows := p.list.visibleRows()
	idx := p.list.GetCursor()
	if idx >= 0 && idx < len(rows) {
		return rows[idx]
	}
	return nil
}

// GetVisibleRows returns all visible rows
func (p *GenericPage) GetVisibleRows() [][]string {
	return p.list.visibleRows()
}
