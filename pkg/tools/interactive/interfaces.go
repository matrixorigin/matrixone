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

// Item represents a displayable data item in a table
type Item interface {
	// GetFields returns the field values for table columns
	GetFields() []string
	// GetSearchableText returns text used for searching
	GetSearchableText() string
	// IsSelectable returns whether this item can be selected with cursor
	IsSelectable() bool
}

// DataSource provides data for table view
type DataSource interface {
	// GetItems returns all items
	GetItems() []Item
	// GetFilteredItems returns filtered items (if filter is active)
	GetFilteredItems() []Item
	// GetItemCount returns total item count
	GetItemCount() int
	// HasFilter returns whether filter is active
	HasFilter() bool
	// GetFilterInfo returns filter description for display
	GetFilterInfo() string
}

// TableConfig defines table display configuration
type TableConfig struct {
	Headers      []string // Column headers
	ColumnWidths []int    // Column widths (in characters)
	AllowCursor  bool     // Whether to show cursor for selection
	AllowSearch  bool     // Whether to enable search
	AllowFilter  bool     // Whether to enable filter
}
