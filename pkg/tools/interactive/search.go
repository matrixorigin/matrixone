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
	"regexp"
	"strings"
)

// SearchEngine handles search functionality
type SearchEngine struct {
	term         string
	regex        *regexp.Regexp
	useRegex     bool
	results      []bool // Match results for each item
	currentIndex int    // Current match index (-1 if no match)
	matchCount   int
}

// NewSearchEngine creates a new search engine
func NewSearchEngine() *SearchEngine {
	return &SearchEngine{
		currentIndex: -1,
	}
}

// SetSearchTerm sets the search term and performs search
func (s *SearchEngine) SetSearchTerm(term string, items []Item) error {
	s.term = term
	s.useRegex = false
	s.regex = nil
	s.currentIndex = -1
	s.matchCount = 0

	if term == "" {
		s.results = nil
		return nil
	}

	// Try to compile as regex
	if regex, err := regexp.Compile(term); err == nil {
		s.regex = regex
		s.useRegex = true
	}

	// Perform search
	s.results = make([]bool, len(items))
	for i, item := range items {
		if s.matches(item.GetSearchableText()) {
			s.results[i] = true
			s.matchCount++
			if s.currentIndex == -1 {
				s.currentIndex = i
			}
		}
	}

	return nil
}

// matches checks if text matches the search term
func (s *SearchEngine) matches(text string) bool {
	if s.term == "" {
		return false
	}
	if s.useRegex && s.regex != nil {
		return s.regex.MatchString(text)
	}
	return strings.Contains(strings.ToLower(text), strings.ToLower(s.term))
}

// Clear clears the search
func (s *SearchEngine) Clear() {
	s.term = ""
	s.regex = nil
	s.useRegex = false
	s.results = nil
	s.currentIndex = -1
	s.matchCount = 0
}

// IsActive returns whether search is active
func (s *SearchEngine) IsActive() bool {
	return s.term != ""
}

// GetTerm returns the search term
func (s *SearchEngine) GetTerm() string {
	return s.term
}

// GetMatchCount returns the number of matches
func (s *SearchEngine) GetMatchCount() int {
	return s.matchCount
}

// GetCurrentIndex returns the current match index
func (s *SearchEngine) GetCurrentIndex() int {
	return s.currentIndex
}

// IsMatch returns whether the item at index is a match
func (s *SearchEngine) IsMatch(index int) bool {
	if index < 0 || index >= len(s.results) {
		return false
	}
	return s.results[index]
}

// NextMatch moves to the next match
func (s *SearchEngine) NextMatch() int {
	if s.matchCount == 0 {
		return -1
	}
	start := s.currentIndex + 1
	for i := 0; i < len(s.results); i++ {
		idx := (start + i) % len(s.results)
		if s.results[idx] {
			s.currentIndex = idx
			return idx
		}
	}
	return s.currentIndex
}

// PrevMatch moves to the previous match
func (s *SearchEngine) PrevMatch() int {
	if s.matchCount == 0 {
		return -1
	}
	start := s.currentIndex - 1
	if start < 0 {
		start = len(s.results) - 1
	}
	for i := 0; i < len(s.results); i++ {
		idx := (start - i + len(s.results)) % len(s.results)
		if s.results[idx] {
			s.currentIndex = idx
			return idx
		}
	}
	return s.currentIndex
}

// GetStatusText returns search status text for display
func (s *SearchEngine) GetStatusText() string {
	if !s.IsActive() {
		return ""
	}
	if s.matchCount == 0 {
		return fmt.Sprintf("🔍 No matches for: %s", s.term)
	}
	matchNum := 0
	for i := 0; i <= s.currentIndex && i < len(s.results); i++ {
		if s.results[i] {
			matchNum++
		}
	}
	return fmt.Sprintf("🔍 Match %d/%d: %s", matchNum, s.matchCount, s.term)
}
