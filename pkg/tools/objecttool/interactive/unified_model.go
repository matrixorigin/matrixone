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

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

// ObjectUnifiedModel uses GenericPage for object viewing
type ObjectUnifiedModel struct {
	state       *State
	currentPage *interactive.GenericPage

	// Command mode
	cmdMode  bool
	cmdInput string
}

// NewObjectUnifiedModel creates a new unified model for object viewing
func NewObjectUnifiedModel(ctx context.Context, reader *objecttool.ObjectReader, opts *ViewOptions) *ObjectUnifiedModel {
	state := NewState(ctx, reader)

	// Apply options
	if opts != nil {
		if opts.StartRow >= 0 && opts.EndRow >= 0 {
			state.SetRowRange(opts.StartRow, opts.EndRow)
		}
		if opts.ColumnNames != nil {
			state.colNames = opts.ColumnNames
		}
		if opts.ColumnExpander != nil {
			state.colExpander = opts.ColumnExpander
		}
		if opts.ObjectNameCol >= 0 {
			state.objectNameCol = opts.ObjectNameCol
			state.baseDir = opts.BaseDir
		}
		// Note: ColumnFormats are handled by the existing bubbletea.go
	}

	m := &ObjectUnifiedModel{
		state: state,
	}

	// Create initial data page
	m.currentPage = NewObjectDataPage(state)
	return m
}

func (m *ObjectUnifiedModel) Init() tea.Cmd {
	return m.currentPage.Init()
}

func (m *ObjectUnifiedModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.currentPage.SetSize(msg.Width, msg.Height)
		return m, nil

	case tea.KeyMsg:
		// Handle command mode
		if m.cmdMode {
			return m.handleCommandMode(msg)
		}

		key := msg.String()
		switch key {
		case "q", "ctrl+c":
			return m, tea.Quit
		case ":":
			m.cmdMode = true
			m.cmdInput = ""
			return m, nil
		case "m":
			// Switch to block meta view
			m.state.SwitchToBlkMeta()
			m.currentPage = NewBlockMetaPage(m.state)
			m.currentPage.Refresh()
			return m, nil
		case "M":
			// Switch to object meta view
			m.state.SwitchToObjMeta()
			m.currentPage = NewObjectMetaPage(m.state)
			m.currentPage.Refresh()
			return m, nil
		case "d":
			// Switch to data view
			m.state.SwitchToData()
			m.currentPage = NewObjectDataPage(m.state)
			m.currentPage.Refresh()
			return m, nil
		}
	}

	// Delegate to current page
	newPage, cmd := m.currentPage.Update(msg)
	m.currentPage = newPage
	return m, cmd
}

func (m *ObjectUnifiedModel) handleCommandMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()
	switch key {
	case "esc":
		m.cmdMode = false
		m.cmdInput = ""
	case "enter":
		// Execute command
		m.executeCommand(m.cmdInput)
		m.cmdMode = false
		m.cmdInput = ""
	case "backspace":
		if len(m.cmdInput) > 0 {
			m.cmdInput = m.cmdInput[:len(m.cmdInput)-1]
		}
	case "left", "right":
		// Ignore left/right in command mode
	default:
		// Support paste - filter out control sequences
		if key != "[" && key != "]" {
			m.cmdInput += key
		}
	}
	return m, nil
}

func (m *ObjectUnifiedModel) executeCommand(cmd string) {
	// Parse and execute commands like :goto, :col, etc.
	// For now, just basic commands
	switch {
	case cmd == "q":
		// Will be handled by Update
	case len(cmd) > 0 && cmd[0] >= '0' && cmd[0] <= '9':
		// Goto row number
		var row int64
		if _, err := parseNumber(cmd, &row); err == nil {
			m.state.GotoRow(row)
			m.currentPage.Refresh()
		}
	}
}

func parseNumber(s string, v *int64) (string, error) {
	var n int64
	for i, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int64(c-'0')
		} else {
			*v = n
			return s[i:], nil
		}
	}
	*v = n
	return "", nil
}

func (m *ObjectUnifiedModel) View() string {
	view := m.currentPage.View()
	if m.cmdMode {
		view += "\n:" + m.cmdInput + "█"
	}
	return view
}

// GetObjectToOpen returns the object path to open (if any)
func (m *ObjectUnifiedModel) GetObjectToOpen() string {
	return m.state.GetObjectToOpen()
}

// ClearObjectToOpen clears the object to open
func (m *ObjectUnifiedModel) ClearObjectToOpen() {
	m.state.ClearObjectToOpen()
}

// RunUnified runs the object viewer using the unified GenericPage framework
func RunUnified(ctx context.Context, path string, opts *ViewOptions) error {
	reader, err := objecttool.Open(ctx, path)
	if err != nil {
		return err
	}
	defer reader.Close()

	m := NewObjectUnifiedModel(ctx, reader, opts)

	for {
		p := tea.NewProgram(m, tea.WithAltScreen())
		finalModel, err := p.Run()
		if err != nil {
			return err
		}

		um, ok := finalModel.(*ObjectUnifiedModel)
		if !ok {
			return nil
		}

		// Check if we need to open a nested object
		if um.GetObjectToOpen() != "" {
			nestedPath := um.GetObjectToOpen()
			um.ClearObjectToOpen()
			// Open nested object with no special options
			if err := RunUnified(ctx, nestedPath, nil); err != nil {
				return err
			}
			// Continue with current viewer after returning
			continue
		}

		return nil
	}
}
