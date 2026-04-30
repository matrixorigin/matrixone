// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Validates that REPLACE INTO counts both inserted and deleted rows toward
// AffectedRows, while regular UPDATE / DELETE / INSERT are unchanged.
func TestAddAffectRows_PerAction(t *testing.T) {
	cases := []struct {
		name           string
		action         actionType
		isReplace      bool
		tableType      UpdateTableType
		insertRows     uint64
		deleteRows     uint64
		wantAfterInsFn uint64 // after only addInsertAffectRows
		wantAfterDelFn uint64 // after addInsertAffectRows + addDeleteAffectRows
	}{
		{
			name:           "INSERT main table",
			action:         actionInsert,
			tableType:      UpdateMainTable,
			insertRows:     5,
			deleteRows:     0,
			wantAfterInsFn: 5,
			wantAfterDelFn: 5,
		},
		{
			name:           "DELETE main table",
			action:         actionDelete,
			tableType:      UpdateMainTable,
			insertRows:     0,
			deleteRows:     7,
			wantAfterInsFn: 0,
			wantAfterDelFn: 7,
		},
		{
			name:           "UPDATE main table counts insert side only",
			action:         actionUpdate,
			isReplace:      false,
			tableType:      UpdateMainTable,
			insertRows:     3,
			deleteRows:     3,
			wantAfterInsFn: 3,
			wantAfterDelFn: 3,
		},
		{
			name:           "REPLACE main table counts insert + delete",
			action:         actionUpdate,
			isReplace:      true,
			tableType:      UpdateMainTable,
			insertRows:     4,
			deleteRows:     2,
			wantAfterInsFn: 4,
			wantAfterDelFn: 6,
		},
		{
			name:           "REPLACE unique index table is skipped",
			action:         actionUpdate,
			isReplace:      true,
			tableType:      UpdateUniqueIndexTable,
			insertRows:     10,
			deleteRows:     10,
			wantAfterInsFn: 0,
			wantAfterDelFn: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := &MultiUpdate{
				MultiUpdateCtx: []*MultiUpdateCtx{{IsReplace: tc.isReplace}},
			}
			op.addAffectedRowsFunc = op.doAddAffectedRows
			op.ctr.action = tc.action

			op.addInsertAffectRows(tc.tableType, tc.insertRows)
			require.Equal(t, tc.wantAfterInsFn, op.GetAffectedRows(),
				"after addInsertAffectRows")

			op.addDeleteAffectRows(tc.tableType, tc.deleteRows)
			require.Equal(t, tc.wantAfterDelFn, op.GetAffectedRows(),
				"after addDeleteAffectRows")
		})
	}
}

// isReplace must tolerate an empty MultiUpdateCtx slice (defensive — shouldn't
// happen in practice but the operator must not panic on it).
func TestIsReplace_EmptyCtx(t *testing.T) {
	op := &MultiUpdate{}
	require.False(t, op.isReplace())
}
