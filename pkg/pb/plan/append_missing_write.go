// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// ValidateAppendMissingWriteTable qualifies a DML projection. INSERT can omit
// the generated rowid and place the hidden compound key last; every ordinary
// physical identity must still be represented exactly once. This exception is
// not used by catalog creation, readers, object proof or batch replay proof.
func ValidateAppendMissingWriteTable(table *TableDef) error {
	if table == nil || table.AppendMissingState == nil {
		return nil
	}
	state := table.AppendMissingState
	if err := ValidateAppendMissingState(state); err != nil {
		return err
	}
	invalid := func() error { return moerr.NewInternalErrorNoCtx("append missing state: incomplete write projection") }
	next := state.BaseWidth + uint32(len(state.Columns))
	if table.Version != uint32(len(state.Columns)) || table.Pkey == nil || table.Pkey.CompPkeyCol == nil ||
		table.Pkey.CompPkeyCol.Seqnum != state.PkSeqnum || uint32(len(table.Cols)) < next || uint32(len(table.Cols)) > next+1 {
		return invalid()
	}
	seen := make([]bool, next)
	rowidSeen := false
	for _, col := range table.Cols {
		if col == nil {
			return invalid()
		}
		if col.Name == "__mo_rowid" {
			if rowidSeen || !col.Hidden || col.Seqnum != next || col.ColId != uint64(next)+1 {
				return invalid()
			}
			rowidSeen = true
			continue
		}
		seq := col.Seqnum
		if seq >= next || seen[seq] || col.ColId != uint64(seq)+1 {
			return invalid()
		}
		seen[seq] = true
		if seq == state.PkSeqnum {
			if col.Name != table.Pkey.CompPkeyCol.Name || !col.Hidden || !col.Primary {
				return invalid()
			}
		}
		if seq >= state.BaseWidth {
			if !BirthFillTypeEqual(state.Columns[seq-state.BaseWidth].Type, col.Typ) || col.Hidden || col.Primary || col.Default == nil || !col.Default.NullAbility {
				return invalid()
			}
		}
	}
	for _, present := range seen {
		if !present {
			return invalid()
		}
	}
	return nil
}
