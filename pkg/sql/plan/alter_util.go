// Copyright 2022 Matrix Origin
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

package plan

import (
	"strings"
)

// FindColumn finds column in cols by name.
func FindColumn(cols []*ColDef, name string) *ColDef {
	for _, col := range cols {
		if strings.EqualFold(col.Name, name) {
			return col
		}
	}
	return nil
}

// FindColumnByOriginName finds column in cols by origin name.
func FindColumnByOriginName(cols []*ColDef, originName string) *ColDef {
	for _, col := range cols {
		if col.GetOriginCaseName() == originName {
			return col
		}
	}
	return nil
}

// FindColumn finds column in cols by colId
func FindColumnByColId(cols []*ColDef, colId uint64) *ColDef {
	for _, col := range cols {
		if col.ColId == colId {
			return col
		}
	}
	return nil
}
