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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// This file is the one shared decode contract for the pk-move link
// retention ledger (mo_catalog.mo_data_branch_link_retention). Every
// reader - the frontend owner-removal reap scan, the compile-side funnel
// coverage row builders, and any future reader - attempts the wide
// projection first and, on the missing-column signal, retries the legacy
// projection with LogicalID assigned 0. Tolerance lives at the column-set
// level (two pinned projections and one fallback), never at per-row
// nullability: pre-upgrade dev-window catalogs lack the appended columns,
// and a failed projection attempt IS that reader's capability verdict for
// this contact only - nothing is cached across contacts.
//
// row_kind is never projected: real-row readers pin row_kind = 0 in the
// WHERE clause of the wide attempt and drop the predicate entirely on the
// legacy fallback (a catalog old enough to lack row_kind predates the
// fencing protocol, so every readable row is a real identity row by
// construction).

// PkMoveRetentionWideColumns is the current projection, in select order.
var PkMoveRetentionWideColumns = []string{
	catalog.DataBranchLinkRetentionColTableID,
	catalog.DataBranchLinkRetentionColDatabaseName,
	catalog.DataBranchLinkRetentionColTableName,
	catalog.DataBranchLinkRetentionColObjID,
	catalog.DataBranchLinkRetentionColCreatedTS,
	catalog.DataBranchLinkRetentionColLogicalID,
}

// PkMoveRetentionLegacyColumns is the pre-upgrade projection, in select
// order; decoded rows carry LogicalID 0.
var PkMoveRetentionLegacyColumns = []string{
	catalog.DataBranchLinkRetentionColTableID,
	catalog.DataBranchLinkRetentionColDatabaseName,
	catalog.DataBranchLinkRetentionColTableName,
	catalog.DataBranchLinkRetentionColObjID,
	catalog.DataBranchLinkRetentionColCreatedTS,
}

// PkMoveRetentionLedgerRow is one ledger row reduced to the columns the
// coverage and reap decisions need. LogicalID 0 marks a legacy or
// unresolved identity and selects the legacy coverage lane (physical
// obj_id compare plus name fallback).
type PkMoveRetentionLedgerRow struct {
	TableID      uint64
	DatabaseName string
	TableName    string
	ObjID        uint64
	CreatedTS    uint64
	LogicalID    uint64
}

// PkMoveLedgerMissingColumnErr reports whether err is the missing-column
// capability signal (selecting an appended column against an old schema
// errors before any row-level tolerance can apply).
func PkMoveLedgerMissingColumnErr(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrBadFieldError)
}

// DecodePkMoveRetentionRows decodes n rows of one query batch into ledger
// rows. legacy=true decodes the five-column projection and assigns
// LogicalID 0. Both id columns ride the D149 defensive cast: a column
// typed either uint64 or int64 decodes, anything else fails loudly.
func DecodePkMoveRetentionRows(
	n int, cols []*vector.Vector, legacy bool,
) ([]PkMoveRetentionLedgerRow, error) {
	want := len(PkMoveRetentionWideColumns)
	if legacy {
		want = len(PkMoveRetentionLegacyColumns)
	}
	if len(cols) != want {
		return nil, moerr.NewInternalErrorNoCtx("invalid pk-move retention ledger projection")
	}
	rows := make([]PkMoveRetentionLedgerRow, 0, n)
	var err error
	for i := 0; i < n; i++ {
		row := PkMoveRetentionLedgerRow{}
		if row.TableID, err = pkMoveRetentionUint64At(cols[0], i); err != nil {
			return nil, err
		}
		if !cols[1].IsNull(uint64(i)) {
			row.DatabaseName = cols[1].GetStringAt(i)
		}
		if !cols[2].IsNull(uint64(i)) {
			row.TableName = cols[2].GetStringAt(i)
		}
		if row.ObjID, err = pkMoveRetentionUint64At(cols[3], i); err != nil {
			return nil, err
		}
		if row.CreatedTS, err = pkMoveRetentionUint64At(cols[4], i); err != nil {
			return nil, err
		}
		if !legacy {
			if row.LogicalID, err = pkMoveRetentionUint64At(cols[5], i); err != nil {
				return nil, err
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// pkMoveRetentionUint64At defensively casts one id-cell to uint64,
// tolerating the int64 shape pre-upgrade decoders produced for the same
// unsigned catalog columns.
func pkMoveRetentionUint64At(col *vector.Vector, i int) (uint64, error) {
	switch col.GetType().Oid {
	case types.T_uint64:
		return vector.MustFixedColNoTypeCheck[uint64](col)[i], nil
	case types.T_int64:
		return uint64(vector.MustFixedColNoTypeCheck[int64](col)[i]), nil
	default:
		return 0, moerr.NewInternalErrorNoCtx("invalid pk-move retention ledger id column")
	}
}
