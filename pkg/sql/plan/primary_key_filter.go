// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// PrimaryKeyUsesUnicodeCollation is shared by planning and byte-oriented PK
// readers, so a descriptor cannot promise a point probe the reader must disable.
func PrimaryKeyUsesUnicodeCollation(tblDef *plan.TableDef) bool {
	if tblDef == nil || tblDef.Pkey == nil {
		return false
	}
	check := func(name string) bool {
		if name == "" {
			return false
		}
		if tblDef.Name2ColIndex != nil {
			if pos, ok := tblDef.Name2ColIndex[name]; ok && pos >= 0 &&
				int(pos) < len(tblDef.Cols) && tblDef.Cols[pos] != nil {
				return types.IsUnicodeCollation(uint8(tblDef.Cols[pos].Typ.Charset))
			}
		}
		// Reader table definitions assembled from a snapshot or an index path
		// are not required to carry Name2ColIndex. Failing to find the column in
		// that optional map must not re-enable a raw-byte PK probe.
		for _, col := range tblDef.Cols {
			if col != nil && strings.EqualFold(col.Name, name) {
				return types.IsUnicodeCollation(uint8(col.Typ.Charset))
			}
		}
		return false
	}
	if check(tblDef.Pkey.PkeyColName) {
		return true
	}
	for _, name := range tblDef.Pkey.Names {
		if check(name) {
			return true
		}
	}
	if tblDef.Pkey.CompPkeyCol != nil {
		return types.IsUnicodeCollation(uint8(tblDef.Pkey.CompPkeyCol.Typ.Charset))
	}
	return false
}
