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

package databranchutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The sys-owned half of the snapshot predicate (D155-A): cluster rows
// ground every account; account-level rows ground only their recorded
// target (obj id first, lazily-resolved name second); database/table rows
// resolve inside the creator's own namespace and are never attributed
// from the sys catalog.
func TestPkMoveSysSnapshotCoversTable(t *testing.T) {
	// Cluster rows cover every account, id- or name-keyed alike.
	require.True(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "cluster"}}, 7, ""))
	require.True(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "cluster", ObjId: ^uint64(0)}}, 7, ""))

	// Account rows cover their recorded target by obj id without any
	// name resolution.
	require.True(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "account", ObjId: 7}}, 7, ""))
	require.False(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "account", ObjId: 8}}, 7, ""))

	// The name fallback matches only when the id missed and the caller
	// resolved the probing account's name.
	require.True(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "account", AccountName: "tenant7"}}, 7, "tenant7"))
	require.False(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "account", AccountName: "tenant7"}}, 7, ""),
		"without a resolved name the id-only match must not cover")
	require.False(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "account", AccountName: "other"}}, 7, "tenant7"))

	// Database/table rows in the SYS catalog are owner-local by
	// construction and never extend a tenant's coverage, even when their
	// names or obj id would match.
	require.False(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "database", DatabaseName: "db1"}}, 7, "tenant7"))
	require.False(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "table", DatabaseName: "db1", TableName: "t800", ObjId: 800}},
		7, "tenant7"))

	// Unknown levels ground nothing (kind='branch' rows never reach the
	// predicate; a stray row must fail dark, not bright).
	require.False(t, PkMoveSysSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "branch", ObjId: 7}}, 7, "tenant7"))
	require.False(t, PkMoveSysSnapshotCoversTable(nil, 7, "tenant7"))
}

// The lazy name-resolution support: a Need fires only for account rows an
// id match cannot explain.
func TestPkMoveSysSnapshotNeedsAccountName(t *testing.T) {
	require.False(t, PkMoveSysSnapshotNeedsAccountName(nil, 7))
	require.False(t, PkMoveSysSnapshotNeedsAccountName(
		[]PkMoveSnapshotRow{{Level: "cluster"}}, 7))
	require.False(t, PkMoveSysSnapshotNeedsAccountName(
		[]PkMoveSnapshotRow{{Level: "account", ObjId: 7}}, 7),
		"an id-hit row needs no name resolution")
	require.False(t, PkMoveSysSnapshotNeedsAccountName(
		[]PkMoveSnapshotRow{{Level: "account", ObjId: 8}}, 7),
		"an id-keyed miss without a name resolves nothing by name")
	require.True(t, PkMoveSysSnapshotNeedsAccountName(
		[]PkMoveSnapshotRow{{Level: "account", ObjId: 0, AccountName: "tenant7"}}, 7))
	require.False(t, PkMoveSysSnapshotNeedsAccountName(
		[]PkMoveSnapshotRow{{Level: "database", AccountName: "x"}}, 7))
}

// The mo_pitr half of the predicate (D152): cluster rows cover every
// account; account rows cover their recorded target; database/table rows
// are scoped to the owning account and the named scope, with table rows
// additionally grounded by the recorded physical obj id.
func TestPkMovePitrCoversTable(t *testing.T) {
	require.True(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "cluster"}}, 7, "", 800, 0, "db1", "t800"))

	// Account rows by recorded id; obj id alone also counts (sys-recorded
	// rows carry both).
	require.True(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "account", AccountId: 7}}, 7, "", 800, 0, "db1", "t800"))
	require.True(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "account", ObjId: 7}}, 7, "", 800, 0, "db1", "t800"))
	require.False(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "account", AccountId: 8, ObjId: 8}}, 7, "", 800, 0, "db1", "t800"))
	require.True(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "account", AccountName: "tenant7"}}, 7, "tenant7", 800, 0, "db1", "t800"))
	require.False(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "account", AccountName: "tenant7"}}, 7, "", 800, 0, "db1", "t800"))

	// Database rows scope to the owning account AND the named database,
	// case-insensitively (the classifier's name rule).
	require.True(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "database", AccountId: 7, DatabaseName: "DB1"}},
		7, "", 800, 0, "db1", "t800"))
	require.False(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "database", AccountId: 7, DatabaseName: "db2"}},
		7, "", 800, 0, "db1", "t800"))
	require.False(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "database", AccountId: 8, DatabaseName: "db1"}},
		7, "", 800, 0, "db1", "t800"),
		"a foreign account's database pitr never grounds this tenant")

	// Table rows: obj id survives a rename; name matching is the
	// case-insensitive fallback, always gated on the owning account.
	require.True(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "table", AccountId: 7, DatabaseName: "old_db", TableName: "old_t", ObjId: 800}},
		7, "", 800, 0, "db1", "t800"))
	require.True(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "table", AccountId: 7, DatabaseName: "DB1", TableName: "T800", ObjId: 42}},
		7, "", 800, 0, "db1", "t800"))
	require.False(t, PkMovePitrCoversTable(
		[]PkMovePitrRow{{Level: "table", AccountId: 8, DatabaseName: "db1", TableName: "t800", ObjId: 800}},
		7, "", 800, 0, "db1", "t800"))
	require.False(t, PkMovePitrCoversTable(nil, 7, "", 800, 0, "db1", "t800"))
}

func TestPkMovePitrNeedsAccountName(t *testing.T) {
	require.False(t, PkMovePitrNeedsAccountName(nil, 7))
	require.False(t, PkMovePitrNeedsAccountName(
		[]PkMovePitrRow{{Level: "account", AccountId: 7}}, 7))
	require.False(t, PkMovePitrNeedsAccountName(
		[]PkMovePitrRow{{Level: "account", ObjId: 7}}, 7))
	require.True(t, PkMovePitrNeedsAccountName(
		[]PkMovePitrRow{{Level: "account", AccountName: "tenant7"}}, 7))
	require.False(t, PkMovePitrNeedsAccountName(
		[]PkMovePitrRow{{Level: "database", AccountName: "tenant7"}}, 7))
}

// The local half keeps its pre-existing rule byte for byte: cluster and
// account rows in the TENANT's own catalog ground everything of that
// tenant; scope rows match by name; table rows additionally by recorded
// physical id.
func TestPkMoveLocalSnapshotCoversTable(t *testing.T) {
	rows := []PkMoveSnapshotRow{
		{Level: "table", DatabaseName: "db1", TableName: "t1", ObjId: 501},
		{Level: "database", DatabaseName: "db2"},
	}
	require.True(t, PkMoveLocalSnapshotCoversTable(rows, 501, 0, "db1", "t1"))
	require.True(t, PkMoveLocalSnapshotCoversTable(rows, 501, 0, "db9", "zz"),
		"recorded obj id survives renames")
	require.True(t, PkMoveLocalSnapshotCoversTable(rows, 42, 0, "db2", "anything"))
	require.False(t, PkMoveLocalSnapshotCoversTable(rows, 9, 0, "db9", "t1"))
	require.True(t, PkMoveLocalSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "cluster"}}, 42, 0, "db9", "zz"))
	require.True(t, PkMoveLocalSnapshotCoversTable(
		[]PkMoveSnapshotRow{{Level: "account"}}, 42, 0, "db9", "zz"))
	require.False(t, PkMoveLocalSnapshotCoversTable(nil, 501, 0, "db1", "t1"))
}

// D158 logical lane: table-level mo_snapshots/mo_pitr obj_id values ARE the
// relation's logical id (authenticate.go:2169 snapshot.go getTableIdFunc
// pitr.go:583), so a nonzero member logical id matches the recorded obj id
// even when the member's current physical id has moved (copy-ALTER, RENAME
// rebuild); the zero marker keeps the legacy physical-compare + name lane.
func TestPkMoveLocalSnapshotCoversTableLogicalLane(t *testing.T) {
	rows := []PkMoveSnapshotRow{
		{Level: "table", DatabaseName: "db1", TableName: "old_t", ObjId: 700},
	}
	// Logical id grounds the recorded obj row although the physical id
	// drifted to 900 and the names no longer match.
	require.True(t, PkMoveLocalSnapshotCoversTable(rows, 900, 700, "db1", "t_new"))
	// A foreign logical id does not ground by physical id in the logical
	// lane; only the pre-existing name rule can still match.
	require.False(t, PkMoveLocalSnapshotCoversTable(rows, 700, 701, "db1", "t_new"))
	// Zero marker: physical compare applies (the legacy lane).
	require.True(t, PkMoveLocalSnapshotCoversTable(rows, 700, 0, "db1", "t_new"))
	require.False(t, PkMoveLocalSnapshotCoversTable(rows, 900, 0, "db1", "t_new"))
}

func TestPkMovePitrCoversTableLogicalLane(t *testing.T) {
	rows := []PkMovePitrRow{
		{Level: "table", AccountId: 7, DatabaseName: "old_db", TableName: "old_t", ObjId: 700},
	}
	require.True(t, PkMovePitrCoversTable(rows, 7, "", 900, 700, "db1", "t_new"))
	require.False(t, PkMovePitrCoversTable(rows, 7, "", 700, 701, "db1", "t_new"))
	require.True(t, PkMovePitrCoversTable(rows, 7, "", 700, 0, "db1", "t_new"))
	require.False(t, PkMovePitrCoversTable(rows, 7, "", 900, 0, "db1", "t_new"),
		"zero logical id never upgrades a physical miss")
}
