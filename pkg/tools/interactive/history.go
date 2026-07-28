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
	"os"
	"path/filepath"
	"strings"
)

// HistoryManager manages command/search history
type HistoryManager struct {
	history []string
	index   int
	maxSize int
}

// NewHistoryManager creates a new history manager
func NewHistoryManager(maxSize int) *HistoryManager {
	return &HistoryManager{
		history: make([]string, 0),
		index:   0,
		maxSize: maxSize,
	}
}

// Add adds an entry to history
func (h *HistoryManager) Add(entry string) {
	if entry == "" {
		return
	}
	// Don't add duplicates
	if len(h.history) > 0 && h.history[len(h.history)-1] == entry {
		h.index = len(h.history)
		return
	}
	h.history = append(h.history, entry)
	if len(h.history) > h.maxSize {
		h.history = h.history[1:]
	}
	h.index = len(h.history)
}

// Up moves to previous history entry
func (h *HistoryManager) Up() string {
	if len(h.history) == 0 || h.index == 0 {
		return ""
	}
	h.index--
	return h.history[h.index]
}

// Down moves to next history entry
func (h *HistoryManager) Down() string {
	if len(h.history) == 0 {
		return ""
	}
	if h.index >= len(h.history)-1 {
		h.index = len(h.history)
		return ""
	}
	h.index++
	return h.history[h.index]
}

// Reset resets the index to the end
func (h *HistoryManager) Reset() {
	h.index = len(h.history)
}

// GetAll returns all history entries
func (h *HistoryManager) GetAll() []string {
	return h.history
}

// LoadFromFile loads history from a file
func (h *HistoryManager) LoadFromFile(filename string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	histFile := filepath.Join(homeDir, ".mo-tool", filename)
	data, err := os.ReadFile(histFile)
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			h.history = append(h.history, line)
		}
	}
	h.index = len(h.history)
	return nil
}

// SaveToFile saves history to a file
func (h *HistoryManager) SaveToFile(filename string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	moToolDir := filepath.Join(homeDir, ".mo-tool")
	if err := os.MkdirAll(moToolDir, 0755); err != nil {
		return err
	}
	histFile := filepath.Join(moToolDir, filename)
	content := strings.Join(h.history, "\n")
	return os.WriteFile(histFile, []byte(content), 0644)
}
