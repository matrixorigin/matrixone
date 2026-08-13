// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/require"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestValidateSnapshotScope(t *testing.T) {
	newSnapshot := func(level string, objectID uint64) *Snapshot {
		return &Snapshot{ExtraInfo: &planpb.SnapshotExtraInfo{
			Name:  "snapshot",
			Level: level,
			ObjId: objectID,
		}}
	}

	tests := []struct {
		name       string
		snapshot   *Snapshot
		databaseID uint64
		tableID    uint64
		err        string
	}{
		{name: "timestamp snapshot", databaseID: 1, tableID: 2},
		{name: "cluster snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELCLUSTER.String(), 0), databaseID: 1, tableID: 2},
		{name: "account snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELACCOUNT.String(), 1), databaseID: 1, tableID: 2},
		{name: "database snapshot same database", snapshot: newSnapshot(tree.SNAPSHOTLEVELDATABASE.String(), 1), databaseID: 1, tableID: 3},
		{name: "database snapshot other database", snapshot: newSnapshot(tree.SNAPSHOTLEVELDATABASE.String(), 1), databaseID: 2, tableID: 3, err: "internal error: database-level snapshot(snapshot) does not belong to the database(db)"},
		{name: "table snapshot same table", snapshot: newSnapshot(tree.SNAPSHOTLEVELTABLE.String(), 2), databaseID: 1, tableID: 2},
		{name: "table snapshot other table", snapshot: newSnapshot(tree.SNAPSHOTLEVELTABLE.String(), 2), databaseID: 1, tableID: 3, err: "internal error: table-level snapshot(snapshot) does not belong to the table(db-table)"},
		{name: "unknown snapshot level", snapshot: newSnapshot("unknown", 1), databaseID: 1, tableID: 2, err: "internal error: unsupported snapshot level \"unknown\""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSnapshotScope(test.snapshot, "db", "table", test.databaseID, test.tableID)
			if test.err == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.err)
		})
	}
}

func TestValidateSnapshotDatabaseScope(t *testing.T) {
	newSnapshot := func(level string, objectID uint64) *Snapshot {
		return &Snapshot{ExtraInfo: &planpb.SnapshotExtraInfo{
			Name:  "snapshot",
			Level: level,
			ObjId: objectID,
		}}
	}

	tests := []struct {
		name     string
		snapshot *Snapshot
		err      string
	}{
		{name: "timestamp snapshot"},
		{name: "cluster snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELCLUSTER.String(), 0)},
		{name: "account snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELACCOUNT.String(), 1)},
		{name: "database snapshot same database", snapshot: newSnapshot(tree.SNAPSHOTLEVELDATABASE.String(), 1)},
		{name: "database snapshot other database", snapshot: newSnapshot(tree.SNAPSHOTLEVELDATABASE.String(), 2), err: "internal error: database-level snapshot(snapshot) does not belong to the database(db)"},
		{name: "table snapshot", snapshot: newSnapshot(tree.SNAPSHOTLEVELTABLE.String(), 2), err: "internal error: table-level snapshot(snapshot) cannot read database-wide metadata for database(db)"},
		{name: "unknown snapshot level", snapshot: newSnapshot("unknown", 1), err: "internal error: unsupported snapshot level \"unknown\""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSnapshotDatabaseScope(test.snapshot, "db", 1)
			if test.err == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.err)
		})
	}
}
