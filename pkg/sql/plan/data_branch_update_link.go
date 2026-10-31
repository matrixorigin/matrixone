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

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	sqlparse "github.com/matrixorigin/matrixone/pkg/sql/parsers"
)

// The DATA BRANCH change stream flattens a pk-moving UPDATE into an
// independent tombstone + insert pair that is indistinguishable from a bare
// DELETE followed by an INSERT. To let data-branch diff recover update
// provenance safely, the UPDATE executor persists one row per moved pk into
// mo_catalog.mo_data_branch_update_link. The link write is driven by a
// synthetic UpdateCtx appended to the MULTI_UPDATE node of the root UPDATE
// plan (bind_update.go). The ctx is a marker plus schema carrier: it holds no
// column references, and the multi_update operator recognizes it by name and
// synthesizes link rows from the owning main-table ctx's vectors.
//
// REPLACE INTO and INSERT ... ON DUPLICATE KEY UPDATE intentionally never get
// this ctx (they are not genuine updates), multi-target UPDATE is excluded at
// the bind site, partitioned/cluster/temporary tables are excluded by
// updatePkMoveLinkEligible, and tenants lacking the catalog table (predating
// v4.0.15) simply produce no link rows, preserving the historical
// delete+insert diff behaviour.

// IsDataBranchUpdateLinkObjRef reports whether ref targets the per-account
// pk-move link table.
func IsDataBranchUpdateLinkObjRef(ref *ObjectRef) bool {
	return ref != nil &&
		ref.SchemaName == catalog.MO_CATALOG &&
		ref.ObjName == catalog.MO_DATA_BRANCH_UPDATE_LINK
}

// IsDataBranchUpdateLinkPlanCtx reports whether ctx is the synthetic pk-move
// link ctx (used by explain and other generic UpdateCtxList walkers to keep
// the internal write invisible).
func IsDataBranchUpdateLinkPlanCtx(updateCtx *UpdateCtx) bool {
	return updateCtx != nil && IsDataBranchUpdateLinkObjRef(updateCtx.ObjRef) &&
		len(updateCtx.InsertCols) == 0 && len(updateCtx.DeleteCols) == 0
}

// buildUpdatePkMoveLinkTableDef returns the plan-side schema of
// mo_catalog.mo_data_branch_update_link. It must stay in sync with
// frontend.MoCatalogDataBranchUpdateLinkDDL (hidden row id column last).
func buildUpdatePkMoveLinkTableDef() *TableDef {
	binaryType := func(width int32) Type {
		return Type{Id: int32(types.T_binary), Width: width}
	}
	cols := make([]*ColDef, 0, 12)
	cols = append(cols,
		&ColDef{
			Name:  catalog.DataBranchUpdateLinkColLinkID,
			Typ:   binaryType(catalog.DataBranchUpdateLinkIDBytes),
			Pkidx: 1,
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColTableID,
			Typ:  Type{Id: int32(types.T_uint64)},
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColOldPKSha,
			Typ:  binaryType(catalog.DataBranchUpdateLinkPKShaBytes),
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColOldRow,
			Typ:  binaryType(24),
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColOldPK,
			Typ:  Type{Id: int32(types.T_varbinary), Width: 65535},
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColNewPKSha,
			Typ:  binaryType(catalog.DataBranchUpdateLinkPKShaBytes),
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColNewPK,
			Typ:  Type{Id: int32(types.T_varbinary), Width: 65535},
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColNewRow,
			Typ:  binaryType(24),
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColWriteTS,
			Typ:  Type{Id: int32(types.T_uint64)},
		},
		&ColDef{
			Name: catalog.DataBranchUpdateLinkColTxnID,
			Typ:  binaryType(catalog.DataBranchUpdateLinkTxnIDBytes),
		},
	)
	// Dense, unique seqnums are load-bearing: a sinker built from this
	// TableDef writes one object column-meta slot per seqnum, and duplicate
	// (zero) seqnums collapse every column into slot 0, corrupting the
	// persisted extent accounting of any object written from this schema.
	for i := range cols {
		cols[i].Seqnum = uint32(i)
	}
	cols = append(cols, MakeRowIdColDef())
	return &TableDef{
		Name: catalog.MO_DATA_BRANCH_UPDATE_LINK,
		Cols: cols,
		Pkey: &PrimaryKeyDef{
			PkeyColName: catalog.DataBranchUpdateLinkColLinkID,
			Names:       []string{catalog.DataBranchUpdateLinkColLinkID},
		},
		TableType: catalog.SystemOrdinaryRel,
	}
}

// updatePkMoveLinkEligible decides whether an UPDATE on this target may
// record pk-move links. Restrictions are fail-closed: any doubt means no link
// rows, and data-branch diff keeps the legacy delete+insert behaviour.
func updatePkMoveLinkEligible(objRef *ObjectRef, tableDef *TableDef) bool {
	if objRef == nil || tableDef == nil {
		return false
	}
	if tableDef.Pkey == nil || len(tableDef.Pkey.Names) == 0 {
		return false
	}
	if tableDef.GetIsTemporary() {
		return false
	}
	if tableDef.TableType != "" && tableDef.TableType != catalog.SystemOrdinaryRel {
		// cluster tables are shared across accounts while the link table is
		// per-account; recording links for them would be ambiguous. Views and
		// other relation kinds are not plain user tables either.
		return false
	}
	if tableDef.Partition != nil && len(tableDef.Partition.PartitionDefs) > 0 {
		// pk moves can also relocate rows across partition maintenance
		// contexts; not covered.
		return false
	}
	if sqlparse.IsSystemDatabase(objRef.SchemaName) {
		return false
	}
	return true
}

// buildUpdatePkMoveLinkPlanCtx builds the synthetic link ctx bound to the
// owning main-table ctx at ownerCtxIdx.
func buildUpdatePkMoveLinkPlanCtx(ownerCtxIdx int32) *UpdateCtx {
	return &UpdateCtx{
		ObjRef: &ObjectRef{
			SchemaName: catalog.MO_CATALOG,
			ObjName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
			// The link write is invisible provenance carried by the owning
			// UPDATE; it must not widen that statement's catalog meta-name
			// locks. Without this skip the second, cross-database target
			// flips LockMeta.doLock into its multi-database branch, and
			// every eligible UPDATE would additionally retain Shared
			// mo_database pins on both the user database and mo_catalog
			// (plus a mo_tables pin on this catalog row), blocking a
			// concurrent DROP DATABASE before it ever queues on the
			// retained mo_tables name row of the UPDATE's real target
			// (main retains only that single-database T pin).
			NotLockMeta: true,
		},
		TableDef:           buildUpdatePkMoveLinkTableDef(),
		IgnoreAffectedRows: true,
		TargetUpdateCtxIdx: ownerCtxIdx,
	}
}
