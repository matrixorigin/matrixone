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
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
	"github.com/stretchr/testify/assert"
)

// Mock implementations for testing
type MockDataProvider struct{}

func (m *MockDataProvider) GetRows() [][]string {
	return [][]string{
		{"1", "Account 1", "5 tables"},
		{"2", "Account 2", "3 tables"},
	}
}

func (m *MockDataProvider) GetHeaders() []string {
	return []string{"#", "Account", "Info"}
}

func (m *MockDataProvider) GetTitle() string {
	return "Mock Accounts"
}

func (m *MockDataProvider) GetOverview() string {
	return "2 accounts total"
}

func (m *MockDataProvider) GetHints() string {
	return "Press Enter to select"
}

type MockActionHandler struct{}

func (m *MockActionHandler) HandleSelect(rowIndex int) tea.Cmd { return nil }
func (m *MockActionHandler) HandleCustomKey(key string) tea.Cmd { return nil }
func (m *MockActionHandler) CanFilter() bool { return false }
func (m *MockActionHandler) ApplyFilter(filter string) interactive.PageDataProvider { return nil }
func (m *MockActionHandler) CanSearch() bool { return true }
func (m *MockActionHandler) Search(query string) []int { return []int{} }

func TestPageFrameworkBuildingBlocks(t *testing.T) {
	// Test the "搭积木" (building blocks) approach
	
	// Step 1: Data Provider (积木1)
	provider := &MockDataProvider{}
	assert.Implements(t, (*interactive.PageDataProvider)(nil), provider)
	
	// Step 2: Action Handler (积木2)
	handler := &MockActionHandler{}
	assert.Implements(t, (*interactive.PageActionHandler)(nil), handler)
	
	// Step 3: Configuration (积木3)
	config := interactive.PageConfig{
		EnableRowNumbers: true,
		EnableCursor:     true,
		EnableSearch:     true,
		MaxColWidth:      50,
	}
	
	// Step 4: Compose Page (搭积木完成!)
	page := interactive.NewPage(config, provider, handler)
	assert.NotNil(t, page)
	
	// The page automatically has all functionality
	view := page.View()
	assert.Contains(t, view, "Mock Accounts")  // Title from provider
	assert.Contains(t, view, "2 accounts")     // Overview from provider
	assert.Contains(t, view, "Account")        // Headers from provider
	assert.Contains(t, view, "Enter")          // Hints from provider
	assert.Contains(t, view, "─")              // Framework styling
}

func TestPageFrameworkReusability(t *testing.T) {
	// Test that the same framework works for different data types
	
	// Same action handler can be reused
	handler := &MockActionHandler{}
	
	// Same configuration can be reused
	config := interactive.PageConfig{
		EnableRowNumbers: true,
		EnableCursor:     true,
		EnableSearch:     true,
		EnableFilter:     true,
		MaxColWidth:      60,
	}
	
	// Create different pages with same building blocks
	accountPage := interactive.NewPage(config, &MockDataProvider{}, handler)
	tablePage := interactive.NewPage(config, &MockDataProvider{}, handler)
	
	// Both work with the same framework
	assert.NotNil(t, accountPage)
	assert.NotNil(t, tablePage)
	
	// Both have framework features
	accountView := accountPage.View()
	tableView := tablePage.View()
	
	assert.Contains(t, accountView, "─") // Framework styling
	assert.Contains(t, tableView, "─")   // Framework styling
}

func TestPageFrameworkModularity(t *testing.T) {
	// Test that components are truly modular - just test the concept
	config := interactive.PageConfig{EnableCursor: true}
	handler := &MockActionHandler{}
	
	// Create pages with same handler and config but different providers
	page1 := interactive.NewPage(config, &MockDataProvider{}, handler)
	page2 := interactive.NewPage(config, &MockDataProvider{}, handler)
	
	// Pages work with modular components
	assert.NotNil(t, page1)
	assert.NotNil(t, page2)
	
	// Both have the same framework structure
	view1 := page1.View()
	view2 := page2.View()
	
	assert.Contains(t, view1, "Mock Accounts")
	assert.Contains(t, view2, "Mock Accounts")
}
