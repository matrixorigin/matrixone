// Copyright 2024 Matrix Origin
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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func IsLazyCatalogTableID(tableID uint64) bool {
	return tableID == MO_DATABASE_ID ||
		tableID == MO_TABLES_ID ||
		tableID == MO_COLUMNS_ID
}

func SetLazyCatalogEntryAccountSummary(entry *api.Entry, accountID uint32) {
	if entry == nil {
		return
	}
	entry.LazyCatalogAccountId = accountID
	entry.HasLazyCatalogAccountId = true
}

func LazyCatalogEntryAccountSummary(entry api.Entry) (uint32, bool) {
	if !entry.GetHasLazyCatalogAccountId() {
		return 0, false
	}
	return entry.GetLazyCatalogAccountId(), true
}

func LazyCatalogEntryAccountID(entry api.Entry) (uint32, bool, error) {
	if accountID, ok := LazyCatalogEntryAccountSummary(entry); ok {
		return accountID, true, nil
	}
	if !IsLazyCatalogTableID(entry.GetTableId()) {
		return 0, false, nil
	}

	bat, err := mustLazyCatalogProtoBatch(entry)
	if err != nil {
		return 0, false, err
	}
	if bat.RowCount() == 0 {
		return 0, false, nil
	}

	switch entry.GetEntryType() {
	case api.Entry_Insert, api.Entry_Update:
		return lazyCatalogInsertOrUpdateAccountID(entry, bat)
	case api.Entry_Delete:
		return lazyCatalogDeleteAccountID(bat)
	default:
		return 0, false, nil
	}
}

func mustLazyCatalogProtoBatch(entry api.Entry) (*batch.Batch, error) {
	if entry.Bat == nil {
		return nil, moerr.NewInternalErrorNoCtxf(
			"catalog logtail entry %s missing batch",
			entry.GetEntryType().String(),
		)
	}
	return batch.ProtoBatchToBatch(entry.Bat)
}

func lazyCatalogInsertOrUpdateAccountID(
	entry api.Entry,
	bat *batch.Batch,
) (uint32, bool, error) {
	accountIdx := FindBatchAttrIndex(bat.Attrs, SystemDBAttr_AccID)
	if accountIdx < 0 {
		return 0, false, moerr.NewInternalErrorNoCtxf(
			"catalog logtail entry %s missing account_id column, attrs=%v",
			entry.GetEntryType().String(),
			bat.Attrs,
		)
	}

	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(int32(accountIdx)))
	if len(accounts) == 0 {
		return 0, false, nil
	}
	return accounts[0], true, nil
}

func lazyCatalogDeleteAccountID(bat *batch.Batch) (uint32, bool, error) {
	// Insert/update entries use __mo_cpkey_col; tombstone/delete entries
	// use __mo_%1_pk_val. Both contain the same compound-key bytes.
	cpkeyIdx := FindCatalogDeletePKIndex(bat.Attrs)
	if cpkeyIdx < 0 {
		return 0, false, moerr.NewInternalErrorNoCtxf(
			"catalog delete logtail entry missing cpkey/pk column, attrs=%v",
			bat.Attrs,
		)
	}

	accountID, err := DecodeLazyCatalogAccountFromCPKey(bat.GetVector(int32(cpkeyIdx)).GetBytesAt(0))
	if err != nil {
		return 0, false, err
	}
	return accountID, true, nil
}

// FindCatalogDeletePKIndex returns the batch attribute index that carries
// the compound primary key bytes. Insert/update entries name it
// CPrimaryKeyColName (__mo_cpkey_col); tombstone entries name it
// TombstoneAttr_PK_Attr (__mo_%1_pk_val). Both hold the same encoded tuple.
func FindCatalogDeletePKIndex(attrs []string) int {
	if idx := FindBatchAttrIndex(attrs, CPrimaryKeyColName); idx >= 0 {
		return idx
	}
	return FindBatchAttrIndex(attrs, objectio.TombstoneAttr_PK_Attr)
}

func FindBatchAttrIndex(attrs []string, target string) int {
	for i, attr := range attrs {
		if attr == target {
			return i
		}
	}
	return -1
}

func DecodeLazyCatalogAccountFromCPKey(cpkey []byte) (uint32, error) {
	tuple, err := types.Unpack(cpkey)
	if err != nil {
		return 0, err
	}
	if len(tuple) == 0 {
		return 0, moerr.NewInternalErrorNoCtx("empty catalog cpkey")
	}

	accountID, ok := tuple[0].(uint32)
	if !ok {
		return 0, moerr.NewInternalErrorNoCtxf(
			"unexpected catalog cpkey account type %T",
			tuple[0],
		)
	}
	return accountID, nil
}
