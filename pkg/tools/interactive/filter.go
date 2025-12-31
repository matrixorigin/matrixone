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

import "fmt"

// FilterFunc is a function that filters items
type FilterFunc func(item interface{}) bool

// Filter manages filtering logic
type Filter struct {
	active      bool
	description string
	filterFunc  FilterFunc
}

// NewFilter creates a new filter
func NewFilter() *Filter {
	return &Filter{
		active: false,
	}
}

// SetFilter sets the filter function and description
func (f *Filter) SetFilter(description string, fn FilterFunc) {
	f.active = true
	f.description = description
	f.filterFunc = fn
}

// Clear clears the filter
func (f *Filter) Clear() {
	f.active = false
	f.description = ""
	f.filterFunc = nil
}

// IsActive returns whether filter is active
func (f *Filter) IsActive() bool {
	return f.active
}

// GetDescription returns filter description
func (f *Filter) GetDescription() string {
	return f.description
}

// Apply applies the filter to an item
func (f *Filter) Apply(item interface{}) bool {
	if !f.active || f.filterFunc == nil {
		return true
	}
	return f.filterFunc(item)
}

// FilterItems filters a slice of items
func (f *Filter) FilterItems(items []interface{}) []interface{} {
	if !f.active {
		return items
	}
	filtered := make([]interface{}, 0)
	for _, item := range items {
		if f.Apply(item) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// GetStatusText returns filter status text for display
func (f *Filter) GetStatusText() string {
	if !f.active {
		return ""
	}
	return fmt.Sprintf("🔍 %s", f.description)
}
