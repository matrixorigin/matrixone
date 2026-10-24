//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func init() {
	activeM0WriterSchemaVersion = m0WriterSchemaVersion
}

func m0WriterSchemaVersion(tableDef *plan.TableDef, seqnums []uint16) uint32 {
	if tableDef == nil || !m0WriterHasCompleteAppendPrefix(tableDef, seqnums) {
		if tableDef == nil {
			return 0
		}
		return tableDef.Version
	}
	return uint32(len(seqnums)) - tableDef.Pkey.CompPkeyCol.Seqnum - 1
}

func m0WriterHasCompleteAppendPrefix(tableDef *plan.TableDef, seqnums []uint16) bool {
	if tableDef.IsTemporary || tableDef.Pkey == nil || tableDef.Pkey.CompPkeyCol == nil ||
		tableDef.ClusterBy != nil || len(tableDef.Cols) < 2 ||
		len(seqnums) != len(tableDef.Cols)-1 {
		return false
	}

	pk := tableDef.Pkey.CompPkeyCol
	if !pk.Hidden || !pk.Primary || pk.Name == "" ||
		tableDef.Pkey.PkeyColName != pk.Name ||
		pk.Seqnum >= uint32(len(seqnums)) ||
		!objectio.IsPhysicalAddr(tableDef.Cols[len(tableDef.Cols)-1].Name) {
		return false
	}

	for i, col := range tableDef.Cols {
		if col == nil || col.Seqnum != uint32(i) {
			return false
		}
		if i == len(seqnums) {
			continue
		}
		if seqnums[i] != uint16(i) {
			return false
		}
	}
	anchor := tableDef.Cols[pk.Seqnum]
	return anchor.Name == pk.Name && anchor.Hidden && anchor.Primary
}
