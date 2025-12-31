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
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
)

// === Object Data Provider ===

type ObjectDataProvider struct {
	state *State
}

func (p *ObjectDataProvider) GetRows() [][]string {
	rows, _, _ := p.state.CurrentRows()
	return rows
}

func (p *ObjectDataProvider) GetOverview() string {
	info := p.state.reader.Info()
	var parts []string
	parts = append(parts, fmt.Sprintf("Rows: %d", info.RowCount))
	parts = append(parts, fmt.Sprintf("Blocks: %d", info.BlockCount))
	parts = append(parts, fmt.Sprintf("Cols: %d", len(p.state.reader.Columns())))

	if p.state.viewMode == ViewModeBlkMeta {
		parts = append(parts, "Mode: BlkMeta")
	} else if p.state.viewMode == ViewModeObjMeta {
		parts = append(parts, "Mode: ObjMeta")
	} else {
		parts = append(parts, "Mode: Data")
	}

	return strings.Join(parts, " │ ")
}

// === Object Data Handler ===

type ObjectDataHandler struct {
	state *State
}

func (h *ObjectDataHandler) OnSelect(rowIdx int) tea.Cmd {
	// In object tool, Enter goes to next page
	h.state.NextPage()
	return nil
}

func (h *ObjectDataHandler) OnBack() tea.Cmd {
	h.state.PrevPage()
	return nil
}

func (h *ObjectDataHandler) OnCustomKey(key string) tea.Cmd {
	return nil
}

func (h *ObjectDataHandler) MatchRow(row []string, query string) bool {
	for _, cell := range row {
		if strings.Contains(strings.ToLower(cell), strings.ToLower(query)) {
			return true
		}
	}
	return false
}

func (h *ObjectDataHandler) FilterRow(row []string, filter string) bool {
	return h.MatchRow(row, filter)
}

// === Page Factory for Object Tool ===

func NewObjectDataPage(state *State) *interactive.GenericPage {
	// Build headers from columns
	cols := state.reader.Columns()
	headers := make([]string, len(cols))
	for i, col := range cols {
		if state.colNames != nil {
			if name, ok := state.colNames[col.Idx]; ok {
				headers[i] = name
				continue
			}
		}
		headers[i] = fmt.Sprintf("Col%d", col.Idx)
	}

	config := interactive.PageConfig{
		Title:         "═══ Object Viewer ═══",
		Headers:       headers,
		ShowRowNumber: true,
		EnableCursor:  true,
		EnableSearch:  true,
		EnableHScroll: true,
	}

	provider := &ObjectDataProvider{state: state}
	handler := &ObjectDataHandler{state: state}
	return interactive.NewGenericPage(config, provider, handler)
}

// === Block Meta Provider ===

type BlockMetaProvider struct {
	state *State
}

func (p *BlockMetaProvider) GetRows() [][]string {
	rows, _, _ := p.state.CurrentRows()
	return rows
}

func (p *BlockMetaProvider) GetOverview() string {
	info := p.state.reader.Info()
	return fmt.Sprintf("Block Metadata │ Blocks: %d", info.BlockCount)
}

// === Object Meta Provider ===

type ObjectMetaProvider struct {
	state *State
}

func (p *ObjectMetaProvider) GetRows() [][]string {
	rows, _, _ := p.state.CurrentRows()
	return rows
}

func (p *ObjectMetaProvider) GetOverview() string {
	return "Object Metadata"
}

func NewBlockMetaPage(state *State) *interactive.GenericPage {
	headers := make([]string, len(state.metaCols))
	for i, col := range state.metaCols {
		headers[i] = col.Name
	}

	config := interactive.PageConfig{
		Title:         "═══ Block Metadata ═══",
		Headers:       headers,
		ShowRowNumber: true,
		EnableCursor:  true,
		EnableSearch:  true,
	}

	provider := &BlockMetaProvider{state: state}
	handler := &ObjectDataHandler{state: state}
	return interactive.NewGenericPage(config, provider, handler)
}

func NewObjectMetaPage(state *State) *interactive.GenericPage {
	config := interactive.PageConfig{
		Title:         "═══ Object Metadata ═══",
		Headers:       []string{"Property", "Value"},
		ShowRowNumber: false,
		EnableCursor:  true,
		EnableSearch:  true,
	}

	provider := &ObjectMetaProvider{state: state}
	handler := &ObjectDataHandler{state: state}
	return interactive.NewGenericPage(config, provider, handler)
}
