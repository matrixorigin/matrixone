// Copyright 2024 Matrix Origin
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

package service

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
)

var lazyCatalogSubscribeFilterMP = mpool.MustNew("lazy-catalog-subscribe-filter")

// filterLazyCatalogPulledTail filters a pulled logtail for the three lazy
// catalog tables, keeping only rows belonging to allowedAccounts.
//
// When stripObjectMeta is true, object-metadata entries (Entry_DataObject,
// Entry_TombstoneObject) are dropped entirely. This is correct for activation
// responses because partition state already has all object metadata from the
// earlier subscribe + steady-state push; activation only needs the row-level
// catalog entries for the activated account.
func filterLazyCatalogPulledTail(
	tail logtail.TableLogtail,
	allowedAccounts *lazyCatalogAllowedAccounts,
	stripObjectMeta bool,
) (logtail.TableLogtail, func(), error) {
	if allowedAccounts == nil || !isLazyCatalogTableID(tail.Table) {
		return tail, nil, nil
	}
	return filterLazyCatalogSubscribeRowsInTail(tail, allowedAccounts, stripObjectMeta)
}

func filterLazyCatalogSubscribeRowsInTail(
	tail logtail.TableLogtail,
	allowedAccounts *lazyCatalogAllowedAccounts,
	stripObjectMeta bool,
) (logtail.TableLogtail, func(), error) {
	// Subscribe snapshots can still carry mixed-account api.Entry batches for the
	// three catalog tables, so the TN side must copy only the target rows.
	if len(tail.Commands) == 0 {
		return tail, nil, nil
	}

	filtered := tail
	var closeCBs []func()
	// Keep CkpLocation as-is. Checkpoints are not account-filtered, but the
	// CN needs them to populate partition state (object entries). Account-level
	// filtering happens at the catalog-cache replay SQL layer via
	// "account_id IN (...)" predicates. Stripping CkpLocation would leave
	// partition state empty after a checkpoint, breaking replay queries.
	filtered.Commands = make([]api.Entry, 0, len(tail.Commands))
	for i := range tail.Commands {
		entry, keep, closeCB, err := filterLazyCatalogSubscribeEntry(tail.Commands[i], allowedAccounts, stripObjectMeta)
		if err != nil {
			closeCallbacks(closeCB)
			closeCallbacks(closeCBs...)
			return logtail.TableLogtail{}, nil, err
		}
		if keep {
			filtered.Commands = append(filtered.Commands, entry)
		}
		if closeCB != nil {
			closeCBs = append(closeCBs, closeCB)
		}
	}
	return filtered, composeCloseCallback(closeCBs...), nil
}

func filterLazyCatalogPublishRowsInTail(
	tail logtail.TableLogtail,
	allowedAccounts *lazyCatalogAllowedAccounts,
) (logtail.TableLogtail, bool, func(), error) {
	// This helper is only for the three lazy-load catalog tables. Callers are
	// expected to keep normal-table publish/subscribe paths out of here.
	if len(tail.Commands) == 0 {
		return tail, false, nil, nil
	}

	var filteredCommands []api.Entry
	var closeCBs []func()
	for i := range tail.Commands {
		entry := tail.Commands[i]
		rowEntry := entry.EntryType == api.Entry_Insert || entry.EntryType == api.Entry_Update || entry.EntryType == api.Entry_Delete
		accountID, summarized := catalog.LazyCatalogEntryAccountSummary(entry)
		if !rowEntry || summarized && allowedAccounts.contains(accountID) {
			if filteredCommands != nil {
				filteredCommands = append(filteredCommands, entry)
			}
			continue
		}
		if filteredCommands == nil {
			filteredCommands = make([]api.Entry, 0, len(tail.Commands))
			filteredCommands = append(filteredCommands, tail.Commands[:i]...)
		}
		if summarized {
			continue
		}
		entries, closeCB, err := splitLazyCatalogPublishEntry(entry, allowedAccounts)
		if err != nil {
			closeCallbacks(closeCB)
			closeCallbacks(closeCBs...)
			return logtail.TableLogtail{}, false, nil, err
		}
		filteredCommands = append(filteredCommands, entries...)
		if closeCB != nil {
			closeCBs = append(closeCBs, closeCB)
		}
	}
	if filteredCommands == nil {
		return tail, false, nil, nil
	}

	filtered := tail
	filtered.Commands = filteredCommands
	return filtered, true, composeCloseCallback(closeCBs...), nil
}

func filterLazyCatalogSubscribeEntry(
	entry api.Entry,
	allowedAccounts *lazyCatalogAllowedAccounts,
	stripObjectMeta bool,
) (api.Entry, bool, func(), error) {
	switch entry.GetEntryType() {
	case api.Entry_Insert, api.Entry_Update:
		return filterLazyCatalogSubscribeInsertOrUpdateEntry(entry, allowedAccounts)
	case api.Entry_Delete:
		return filterLazyCatalogSubscribeDeleteEntry(entry, allowedAccounts)
	case api.Entry_DataObject, api.Entry_TombstoneObject:
		// Object metadata entries are table-level (not account-specific).
		// For subscribe: keep them so CN can populate partition state.
		// For activation: drop them because partition state already has
		// all objects from the earlier subscribe + steady-state push.
		if stripObjectMeta {
			return api.Entry{}, false, nil, nil
		}
		return entry, true, nil, nil
	default:
		return entry, true, nil, nil
	}
}

// Entries without a summary may contain several accounts (for example restore).
// Emit homogeneous entries so CN per-account delayed replay remains exact.
func splitLazyCatalogPublishEntry(
	entry api.Entry,
	allowedAccounts *lazyCatalogAllowedAccounts,
) ([]api.Entry, func(), error) {
	bat, err := mustProtoBatch(entry)
	if err != nil {
		return nil, nil, err
	}
	if bat.RowCount() == 0 {
		return []api.Entry{entry}, nil, nil
	}
	attrIdx := catalog.FindBatchAttrIndex(bat.Attrs, catalog.SystemDBAttr_AccID)
	if entry.EntryType == api.Entry_Delete {
		attrIdx = catalog.FindCatalogDeletePKIndex(bat.Attrs)
	}
	if attrIdx < 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("catalog entry missing account identity column")
	}
	vec := bat.GetVector(int32(attrIdx))
	var accountIDs []uint32
	if entry.EntryType != api.Entry_Delete {
		accountIDs = vector.MustFixedColWithTypeCheck[uint32](vec)
	}
	var order []uint32
	groups := make(map[uint32][]int64)
	for row := 0; row < bat.RowCount(); row++ {
		var accountID uint32
		if entry.EntryType == api.Entry_Delete {
			accountID, err = catalog.DecodeLazyCatalogAccountFromCPKey(vec.GetBytesAt(row))
			if err != nil {
				return nil, nil, err
			}
		} else {
			accountID = accountIDs[row]
		}
		if !allowedAccounts.contains(accountID) {
			continue
		}
		if _, exists := groups[accountID]; !exists {
			order = append(order, accountID)
		}
		groups[accountID] = append(groups[accountID], int64(row))
	}
	entries := make([]api.Entry, 0, len(order))
	var closeCBs []func()
	for _, accountID := range order {
		filtered, _, closeCB, err := buildFilteredCatalogEntry(entry, bat, groups[accountID])
		if err != nil {
			closeCallbacks(closeCB)
			closeCallbacks(closeCBs...)
			return nil, nil, err
		}
		catalog.SetLazyCatalogEntryAccountSummary(&filtered, accountID)
		entries = append(entries, filtered)
		if closeCB != nil {
			closeCBs = append(closeCBs, closeCB)
		}
	}
	return entries, composeCloseCallback(closeCBs...), nil
}

func filterLazyCatalogSubscribeInsertOrUpdateEntry(
	entry api.Entry,
	allowedAccounts *lazyCatalogAllowedAccounts,
) (api.Entry, bool, func(), error) {
	bat, err := mustProtoBatch(entry)
	if err != nil {
		return api.Entry{}, false, nil, err
	}
	if bat.RowCount() == 0 {
		return entry, true, nil, nil
	}

	accountIdx := catalog.FindBatchAttrIndex(bat.Attrs, catalog.SystemDBAttr_AccID)
	if accountIdx < 0 {
		return api.Entry{}, false, nil, moerr.NewInternalErrorNoCtxf(
			"catalog logtail entry %s missing account_id column, attrs=%v",
			entry.GetEntryType().String(),
			bat.Attrs,
		)
	}

	accounts := vector.MustFixedColWithTypeCheck[uint32](bat.GetVector(int32(accountIdx)))
	selectedRows := make([]int64, 0, len(accounts))
	for row, accountID := range accounts {
		if allowedAccounts.contains(accountID) {
			selectedRows = append(selectedRows, int64(row))
		}
	}
	return buildFilteredCatalogEntry(entry, bat, selectedRows)
}

func filterLazyCatalogSubscribeDeleteEntry(
	entry api.Entry,
	allowedAccounts *lazyCatalogAllowedAccounts,
) (api.Entry, bool, func(), error) {
	bat, err := mustProtoBatch(entry)
	if err != nil {
		return api.Entry{}, false, nil, err
	}
	if bat.RowCount() == 0 {
		return entry, true, nil, nil
	}

	// Insert/update entries use __mo_cpkey_col; tombstone/delete entries
	// use __mo_%1_pk_val. Both contain the same compound-key bytes.
	cpkeyIdx := catalog.FindCatalogDeletePKIndex(bat.Attrs)
	if cpkeyIdx < 0 {
		return api.Entry{}, false, nil, moerr.NewInternalErrorNoCtxf(
			"catalog delete logtail entry missing cpkey/pk column, attrs=%v",
			bat.Attrs,
		)
	}

	selectedRows := make([]int64, 0, bat.RowCount())
	cpkeyVec := bat.GetVector(int32(cpkeyIdx))
	for row := 0; row < bat.RowCount(); row++ {
		accountID, err := catalog.DecodeLazyCatalogAccountFromCPKey(cpkeyVec.GetBytesAt(row))
		if err != nil {
			return api.Entry{}, false, nil, err
		}
		if allowedAccounts.contains(accountID) {
			selectedRows = append(selectedRows, int64(row))
		}
	}
	return buildFilteredCatalogEntry(entry, bat, selectedRows)
}

func buildFilteredCatalogEntry(
	entry api.Entry,
	bat *batch.Batch,
	selectedRows []int64,
) (api.Entry, bool, func(), error) {
	switch {
	case len(selectedRows) == 0:
		return api.Entry{}, false, nil, nil
	case len(selectedRows) == bat.RowCount():
		return entry, true, nil, nil
	}

	copiedBat, closeCB, err := copyProtoBatchRows(bat, selectedRows)
	if err != nil {
		return api.Entry{}, false, nil, err
	}
	filtered := entry
	filtered.Bat = copiedBat
	return filtered, true, closeCB, nil
}

func copyProtoBatchRows(src *batch.Batch, selectedRows []int64) (*api.Batch, func(), error) {
	attrs := append([]string(nil), src.Attrs...)
	typesByAttr := make([]types.Type, len(src.Vecs))
	for i, vec := range src.Vecs {
		typesByAttr[i] = *vec.GetType()
	}

	copied := batch.NewWithSchema(false, attrs, typesByAttr)

	if err := copied.Union(src, selectedRows, lazyCatalogSubscribeFilterMP); err != nil {
		copied.Clean(lazyCatalogSubscribeFilterMP)
		return nil, nil, err
	}

	pbBat, err := batch.BatchToProtoBatch(copied)
	if err != nil {
		copied.Clean(lazyCatalogSubscribeFilterMP)
		return nil, nil, err
	}
	return pbBat, func() {
		copied.Clean(lazyCatalogSubscribeFilterMP)
	}, nil
}

func mustProtoBatch(entry api.Entry) (*batch.Batch, error) {
	if entry.Bat == nil {
		return nil, moerr.NewInternalErrorNoCtxf(
			"catalog logtail entry %s missing batch",
			entry.GetEntryType().String(),
		)
	}
	return batch.ProtoBatchToBatch(entry.Bat)
}
