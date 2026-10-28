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

import "strings"

// PkMoveSnapshotRow is one mo_catalog.mo_snapshots row reduced to the columns
// the pk-move owner-coverage decision needs. table-level rows record the
// physical table id at snapshot creation in ObjId, which survives a later
// RENAME of the table (renames keep the relation id); account-level rows
// record the owning account's name and id (ObjId); cluster rows record
// ObjId=MaxUint64. Rows with kind='branch' are filtered by the caller's
// query: branch-protect snapshots retire with the branch metadata rows the
// lineage-descendant probe already covers.
type PkMoveSnapshotRow struct {
	Level        string
	AccountName  string
	DatabaseName string
	TableName    string
	ObjId        uint64
}

// PkMoveLocalSnapshotCoversTable reports whether any account-local snapshot
// row grounds a historical DATA BRANCH over (tableID, dbName, tableName):
// cluster- and account-level snapshots freeze the whole account's image,
// database- and table-level ones the named scope. Table-level snapshot rows
// record the table's durable rel_logical_id as obj_id, so the table-level
// match keys on the member's logical identity — a copy ALTER (new physical
// relation, same logical id) keeps its coverage. Members whose logical id
// was never captured (logicalID 0) take the legacy lane exactly: physical
// obj_id compare with the name predicate as fallback. The name predicate
// stays as fallback in both lanes so a snapshot created before a rename
// still grounds the renamed table.
func PkMoveLocalSnapshotCoversTable(rows []PkMoveSnapshotRow, tableID, logicalID uint64, dbName, tableName string) bool {
	for _, row := range rows {
		switch row.Level {
		case "cluster", "account":
			return true
		case "database":
			if row.DatabaseName == dbName {
				return true
			}
		case "table":
			if logicalID != 0 {
				if row.ObjId == logicalID {
					return true
				}
			} else if row.ObjId == tableID {
				return true
			}
			if row.DatabaseName == dbName && row.TableName == tableName {
				return true
			}
		}
	}
	return false
}

// PkMoveSysSnapshotCoversTable evaluates the SYS-catalog half of the
// snapshot predicate. Sys creates cluster snapshots and account-level
// snapshots targeting other tenants in SYS's own catalog, so an
// account-local probe cannot see them. Only cluster rows (which retain
// every tenant's history) and account-level rows whose recorded target is
// the probing tenant extend tenant coverage: database/table-level rows
// resolve inside the creator's own namespace and are owner-local by
// construction, so they are never attributed from the sys catalog.
func PkMoveSysSnapshotCoversTable(rows []PkMoveSnapshotRow, accountID uint32, accountName string) bool {
	for _, row := range rows {
		switch row.Level {
		case "cluster":
			return true
		case "account":
			if row.ObjId == uint64(accountID) {
				return true
			}
			if accountName != "" && row.AccountName == accountName {
				return true
			}
		}
	}
	return false
}

// PkMoveSysSnapshotNeedsAccountName reports whether name matching could add
// coverage the id match missed. Callers resolve the tenant's account name
// lazily only when this returns true, keeping the common path id-only.
func PkMoveSysSnapshotNeedsAccountName(rows []PkMoveSnapshotRow, accountID uint32) bool {
	for _, row := range rows {
		if row.Level == "account" && row.ObjId != uint64(accountID) && row.AccountName != "" {
			return true
		}
	}
	return false
}

// PkMovePitrRow is one mo_catalog.mo_pitr row reduced to the columns the
// coverage decision needs. mo_pitr lives only in the SYS catalog: cluster
// rows carry AccountId=0 and ObjId=MaxUint64; account-level rows record the
// target account's name and id in AccountName/AccountId/ObjId;
// database/table-level rows carry the owning account's id (AccountId) and
// the named database/table (with ObjId being the database or table id).
type PkMovePitrRow struct {
	Level        string
	AccountName  string
	DatabaseName string
	TableName    string
	AccountId    uint64
	ObjId        uint64
}

// PkMovePitrCoversTable reports whether any active (pitr_status = 1) mo_pitr
// row grounds a historical DATA BRANCH over (tableID, dbName, tableName) for
// the probing account: cluster rows cover every account; account rows cover
// their recorded target account; database/table rows cover the owning
// account's named scope. Table-level rows record the durable rel_logical_id
// as obj_id, so the table-level obj match keys on the member's logical
// identity when it is known (logicalID != 0) and on its physical id in the
// legacy lane; the name predicate stays as fallback in both lanes so a pitr
// created before a rename still grounds the renamed table.
func PkMovePitrCoversTable(rows []PkMovePitrRow, accountID uint32, accountName string, tableID, logicalID uint64, dbName, tableName string) bool {
	for _, row := range rows {
		switch row.Level {
		case "cluster":
			return true
		case "account":
			if row.AccountId == uint64(accountID) || row.ObjId == uint64(accountID) {
				return true
			}
			if accountName != "" && row.AccountName == accountName {
				return true
			}
		case "database":
			if row.AccountId == uint64(accountID) && strings.EqualFold(row.DatabaseName, dbName) {
				return true
			}
		case "table":
			if row.AccountId == uint64(accountID) {
				if logicalID != 0 {
					if row.ObjId == logicalID {
						return true
					}
				} else if row.ObjId == tableID {
					return true
				}
				if strings.EqualFold(row.DatabaseName, dbName) && strings.EqualFold(row.TableName, tableName) {
					return true
				}
			}
		}
	}
	return false
}

// PkMovePitrNeedsAccountName reports whether name matching could add
// coverage the id match missed (lazy account-name resolution, as above).
func PkMovePitrNeedsAccountName(rows []PkMovePitrRow, accountID uint32) bool {
	for _, row := range rows {
		if row.Level == "account" && row.AccountId != uint64(accountID) && row.ObjId != uint64(accountID) && row.AccountName != "" {
			return true
		}
	}
	return false
}
