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

package objecttool

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Formatter is the column formatter interface
type Formatter interface {
	Format(value any) string
	CanFormat(value any, typ types.Type) bool
}

// FormatterRegistry is the formatter registry
type FormatterRegistry struct {
	byColIdx   map[uint16]Formatter
	autoDetect []Formatter
}

func NewFormatterRegistry() *FormatterRegistry {
	return &FormatterRegistry{
		byColIdx: make(map[uint16]Formatter),
		autoDetect: []Formatter{
			&ObjectStatsFormatter{},
			&RowidFormatter{},
			&TSFormatter{},
		},
	}
}

// SetFormatter sets formatter for specified column
func (r *FormatterRegistry) SetFormatter(colIdx uint16, f Formatter) {
	r.byColIdx[colIdx] = f
}

// ClearFormatter clears formatter for specified column
func (r *FormatterRegistry) ClearFormatter(colIdx uint16) {
	delete(r.byColIdx, colIdx)
}

// GetFormatter gets the formatter for a column
func (r *FormatterRegistry) GetFormatter(colIdx uint16, typ types.Type, sample any) Formatter {
	// 1. User specified
	if f, ok := r.byColIdx[colIdx]; ok {
		return f
	}

	// 2. Auto detect
	if sample != nil {
		for _, f := range r.autoDetect {
			if f.CanFormat(sample, typ) {
				return f
			}
		}
	}

	// 3. Default
	return &DefaultFormatter{}
}

// GetFormatterName gets formatter name (for display)
func (r *FormatterRegistry) GetFormatterName(colIdx uint16) string {
	if f, ok := r.byColIdx[colIdx]; ok {
		return formatterName(f)
	}
	return "auto"
}

func formatterName(f Formatter) string {
	switch f.(type) {
	case *ObjectStatsFormatter:
		return "objectstats"
	case *RowidFormatter:
		return "rowid"
	case *TSFormatter:
		return "ts"
	case *HexFormatter:
		return "hex"
	case *DefaultFormatter:
		return "default"
	default:
		return "unknown"
	}
}
