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
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// DefaultFormatter is the default formatter
type DefaultFormatter struct{}

func (f *DefaultFormatter) CanFormat(value any, typ types.Type) bool {
	return true
}

func (f *DefaultFormatter) Format(value any) string {
	switch v := value.(type) {
	case []byte:
		if len(v) == 0 {
			return ""
		}
		// Don't truncate, display full hex content
		return hex.EncodeToString(v)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ObjectStatsFormatter formats ObjectStats
type ObjectStatsFormatter struct{}

func (f *ObjectStatsFormatter) CanFormat(value any, typ types.Type) bool {
	if v, ok := value.([]byte); ok {
		return len(v) == objectio.ObjectStatsLen
	}
	return false
}

func (f *ObjectStatsFormatter) Format(value any) string {
	v := value.([]byte)
	var stats objectio.ObjectStats
	stats.UnMarshal(v)
	return stats.String()
}

// RowidFormatter formats Rowid
type RowidFormatter struct{}

func (f *RowidFormatter) CanFormat(value any, typ types.Type) bool {
	if v, ok := value.([]byte); ok {
		return len(v) == types.RowidSize
	}
	if _, ok := value.(types.Rowid); ok {
		return true
	}
	return false
}

func (f *RowidFormatter) Format(value any) string {
	switch v := value.(type) {
	case []byte:
		var rowid types.Rowid
		copy(rowid[:], v)
		return rowid.String()
	case types.Rowid:
		return v.String()
	}
	return fmt.Sprintf("%v", value)
}

// TSFormatter formats TS
type TSFormatter struct{}

func (f *TSFormatter) CanFormat(value any, typ types.Type) bool {
	return typ.Oid == types.T_TS
}

func (f *TSFormatter) Format(value any) string {
	if v, ok := value.(types.TS); ok {
		ts := v.ToTimestamp()
		return ts.String()
	}
	return fmt.Sprintf("%v", value)
}

// HexFormatter formats as hexadecimal
type HexFormatter struct{}

func (f *HexFormatter) CanFormat(value any, typ types.Type) bool {
	_, ok := value.([]byte)
	return ok
}

func (f *HexFormatter) Format(value any) string {
	if v, ok := value.([]byte); ok {
		return hex.EncodeToString(v)
	}
	return fmt.Sprintf("%v", value)
}

// FormatterByName maps formatter names to formatters
var FormatterByName = map[string]Formatter{
	"default":     &DefaultFormatter{},
	"objectstats": &ObjectStatsFormatter{},
	"rowid":       &RowidFormatter{},
	"ts":          &TSFormatter{},
	"hex":         &HexFormatter{},
}
