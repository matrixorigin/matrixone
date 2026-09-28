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

package rpc

import (
	"context"
	"testing"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/require"
)

func TestMalformedDatabaseCatalogEntryRollsBackEarlierDDL(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	h := mockTAEHandle(ctx, t, config.WithLongScanAndCKPOpts(nil))
	defer h.HandleClose(ctx)

	const dbName = "catalog_guard_rollback"
	valid, err := makeCreateDatabaseEntries("", AccessInfo{}, dbName, 919191, h.m)
	require.NoError(t, err)

	// A generic SQL UPDATE's delete half carries only rowid and pk. It must
	// not be dispatched as the four-column DROP DATABASE command.
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{catalog2.Row_ID, "pk"}
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.Rowid{}, false, h.m))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("key"), false, h.m))
	bat.SetRowCount(1)
	defer bat.Clean(h.m)
	malformed, err := makePBEntry(DELETE, catalog2.MO_CATALOG_ID, catalog2.MO_DATABASE_ID,
		catalog2.MO_CATALOG, catalog2.MO_DATABASE, "", bat)
	require.NoError(t, err)

	commit := func(entries []*api.Entry) error {
		t.Helper()
		payload, err := (&api.PrecommitWriteCmd{EntryList: entries}).MarshalBinary()
		require.NoError(t, err)
		req := &txnpb.TxnCommitRequest{Payload: []*txnpb.TxnRequest{{
			CNRequest: &txnpb.CNOpRequest{OpCode: uint32(api.OpCode_OpPreCommit), Payload: payload},
		}}}
		meta := mock1PCTxn(h.db)
		_, err = h.HandleCommit(ctx, meta, nil, req)
		return err
	}
	require.ErrorContains(t, commit(append(valid, malformed)), "invalid mo_database entry width")
	check, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	require.NotContains(t, check.DatabaseNames(), dbName)
	require.NoError(t, check.Commit(ctx))

	// Replay must not publish the earlier CREATE from the rejected commit.
	dir := h.db.Dir
	require.NoError(t, h.HandleClose(ctx))
	reopened, err := db.Open(ctx, dir, config.WithLongScanAndCKPOpts(nil))
	require.NoError(t, err)
	h.Handle = &Handle{db: reopened}
	check, err = h.db.StartTxn(nil)
	require.NoError(t, err)
	require.NotContains(t, check.DatabaseNames(), dbName)
	require.NoError(t, check.Commit(ctx))

	// The restarted TN remains usable and accepts the proper DDL tuple.
	require.NoError(t, commit(valid))
	check, err = h.db.StartTxn(nil)
	require.NoError(t, err)
	require.Contains(t, check.DatabaseNames(), dbName)
	require.NoError(t, check.Commit(ctx))
}
