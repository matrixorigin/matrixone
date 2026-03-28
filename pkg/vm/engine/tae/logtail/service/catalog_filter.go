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

func filterLazyCatalogPulledTail(
	tail logtail.TableLogtail,
	allowedAccounts *lazyCatalogAllowedAccounts,
) (logtail.TableLogtail, func(), error) {
	if allowedAccounts == nil || !isLazyCatalogTableID(tail.Table) {
		return tail, nil, nil
	}
	return filterLazyCatalogSubscribeRowsInTail(tail, allowedAccounts)
}

func filterLazyCatalogSubscribeRowsInTail(
	tail logtail.TableLogtail,
	allowedAccounts *lazyCatalogAllowedAccounts,
) (logtail.TableLogtail, func(), error) {
	// Subscribe snapshots can still carry mixed-account api.Entry batches for the
	// three catalog tables, so the TN side must copy only the target rows.
	if len(tail.Commands) == 0 {
		return tail, nil, nil
	}

	filtered := tail
	var closeCBs []func()
	// Raw checkpoint locations are not account-filtered. Lazy catalog subscribe
	// and activation responses must therefore forward only filtered row data.
	filtered.CkpLocation = ""
	filtered.Commands = make([]api.Entry, 0, len(tail.Commands))
	for i := range tail.Commands {
		entry, keep, closeCB, err := filterLazyCatalogSubscribeEntry(tail.Commands[i], allowedAccounts)
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
) (logtail.TableLogtail, bool, error) {
	// This helper is only for the three lazy-load catalog tables. Callers are
	// expected to keep normal-table publish/subscribe paths out of here.
	if len(tail.Commands) == 0 {
		return tail, false, nil
	}

	var filteredCommands []api.Entry
	for i := range tail.Commands {
		keep, err := filterLazyCatalogPublishEntry(tail.Commands[i], allowedAccounts)
		if err != nil {
			return logtail.TableLogtail{}, false, err
		}
		if filteredCommands == nil {
			if keep {
				continue
			}
			filteredCommands = make([]api.Entry, 0, len(tail.Commands)-1)
			filteredCommands = append(filteredCommands, tail.Commands[:i]...)
			continue
		}
		if keep {
			filteredCommands = append(filteredCommands, tail.Commands[i])
		}
	}
	if filteredCommands == nil {
		return tail, false, nil
	}

	filtered := tail
	filtered.Commands = filteredCommands
	return filtered, true, nil
}

func filterLazyCatalogSubscribeEntry(
	entry api.Entry,
	allowedAccounts *lazyCatalogAllowedAccounts,
) (api.Entry, bool, func(), error) {
	switch entry.GetEntryType() {
	case api.Entry_Insert, api.Entry_Update:
		return filterLazyCatalogSubscribeInsertOrUpdateEntry(entry, allowedAccounts)
	case api.Entry_Delete:
		return filterLazyCatalogSubscribeDeleteEntry(entry, allowedAccounts)
	default:
		// Object/meta entries are not row-level tenant data; keep them untouched.
		return entry, true, nil, nil
	}
}

func filterLazyCatalogPublishEntry(
	entry api.Entry,
	allowedAccounts *lazyCatalogAllowedAccounts,
) (bool, error) {
	switch entry.GetEntryType() {
	case api.Entry_Insert, api.Entry_Update, api.Entry_Delete:
		accountID, ok, err := catalog.LazyCatalogEntryAccountID(entry)
		if err != nil {
			return false, err
		}
		if !ok {
			return true, nil
		}
		return allowedAccounts.contains(accountID), nil
	default:
		// Object/meta entries are not row-level tenant data; keep them untouched.
		return true, nil
	}
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

	accountIdx := findBatchAttrIndex(bat.Attrs, catalog.SystemDBAttr_AccID)
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

	cpkeyIdx := findBatchAttrIndex(bat.Attrs, catalog.CPrimaryKeyColName)
	if cpkeyIdx < 0 {
		return api.Entry{}, false, nil, moerr.NewInternalErrorNoCtxf(
			"catalog delete logtail entry missing cpkey column, attrs=%v",
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

func findBatchAttrIndex(attrs []string, target string) int {
	for i, attr := range attrs {
		if attr == target {
			return i
		}
	}
	return -1
}
