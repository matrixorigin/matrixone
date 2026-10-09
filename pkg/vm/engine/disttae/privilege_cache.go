// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

// PrivilegeCacheVersion covers both grant contents and physical catalog/object
// identity. It holds only small generation identities, never partition trees.
// The zero version is deliberately not a reusable authorization proof.
type PrivilegeCacheVersion struct {
	account uint32
	tables  [7]struct {
		id      uint64
		version logtailreplay.DataVersion
	}
}

// GetPrivilegeCacheVersion uses the same snapshot admission as a fresh catalog
// transaction. It does not consult the caller's possibly older data snapshot.
// No permission decisions are made here: frontend remains their sole owner.
func (e *Engine) GetPrivilegeCacheVersion(
	ctx context.Context, account uint32, minimum timestamp.Timestamp,
) (PrivilegeCacheVersion, timestamp.Timestamp, error) {
	var version PrivilegeCacheVersion
	if !e.pClient.receivedLogTailTime.ready.Load() {
		return version, minimum, nil
	}
	snapshot, err := e.cli.GetLatestSnapshot(ctx, minimum)
	if err != nil {
		return version, minimum, err
	}
	if snapshot.IsEmpty() {
		return version, minimum, nil
	}
	minimum = snapshot.Prev()
	if !e.pClient.receivedLogTailTime.ready.Load() {
		return version, minimum, nil
	}

	cc := e.GetLatestCatalogCache()
	if cc == nil {
		return version, minimum, nil
	}
	version.account = account
	for i, name := range [...]string{
		catalog.MO_DATABASE, catalog.MO_TABLES,
		"mo_user", "mo_role", "mo_user_grant", "mo_role_grant", "mo_role_privs",
	} {
		id := uint64(catalog.MO_DATABASE_ID)
		partitionAccount := uint64(0)
		switch i {
		case 0:
		case 1:
			id = catalog.MO_TABLES_ID
		default:
			table := cc.GetTableByName(account, catalog.MO_CATALOG_ID, name)
			if table == nil || !table.Ts.Less(snapshot) {
				return PrivilegeCacheVersion{}, minimum, nil
			}
			id = table.Id
			partitionAccount = uint64(account)
		}
		var pending bool
		ps, err := e.pClient.toSubscribeTable(ctx, partitionAccount, id, name,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, &pending)
		if err != nil {
			return PrivilegeCacheVersion{}, minimum, err
		}
		if pending || ps == nil || !ps.DataVersion().VisibleAt(types.TimestampToTS(snapshot)) {
			return PrivilegeCacheVersion{}, minimum, nil
		}
		version.tables[i].id = id
		version.tables[i].version = ps.DataVersion()
	}
	if !e.pClient.receivedLogTailTime.ready.Load() || e.GetLatestCatalogCache() != cc {
		return PrivilegeCacheVersion{}, minimum, nil
	}
	return version, minimum, nil
}
