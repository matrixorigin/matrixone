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

package test

import (
	"context"
	"testing"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

// Resolving a branch ancestor's creation commit reads mo_tables history with a
// composite-PK filter. Once that history leaves the CN partition-state window,
// the same filter must still prune unrelated catalog objects in checkpoint replay.
func TestCatalogChangesPKFilterAfterPartitionGC(t *testing.T) {
	p := testutil.InitEnginePack(testutil.TestOptions{
		TaeEngineOptions: config.WithLongScanAndCKPOpts(nil),
	}, t)
	defer p.Close()

	const database = "branch_catalog_pk"
	op := p.StartCNTxn()
	p.CreateDB(op, database)
	require.NoError(t, op.Commit(p.Ctx))

	flushCatalog := func() {
		t.Helper()
		require.NoError(t, p.T.GetDB().FlushTable(p.Ctx, catalog.System_Account,
			catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, types.TimestampToTS(p.D.Now())))
	}
	flushCatalog()
	from := p.T.GetDB().TxnMgr.Now()
	created := make(map[uint64]types.TS)

	createTable := func(name string) uint64 {
		t.Helper()
		schema := catalog2.MockSchemaAll(3, 1)
		schema.Name = name
		op := p.StartCNTxn()
		rel := p.CreateTableInDB(op, database, schema)
		id := rel.GetTableID(p.Ctx)
		require.NoError(t, op.Commit(p.Ctx))
		created[id] = types.TimestampToTS(op.Txn().CommitTS)
		// Separate flushes give unrelated catalog keys disjoint on-disk objects.
		flushCatalog()
		return id
	}
	deletedID := createTable("a_unrelated")
	targetID := createTable("customers")
	otherID := createTable("z_unrelated")
	op = p.StartCNTxn()
	p.DeleteTableInDB(op, database, "a_unrelated")
	require.NoError(t, op.Commit(p.Ctx))
	flushCatalog()

	to := p.T.GetDB().TxnMgr.Now()
	require.NoError(t, p.T.GetDB().ForceCheckpoint(p.Ctx, to))
	require.NoError(t, p.T.GetDB().ForceCheckpoint(p.Ctx, p.T.GetDB().TxnMgr.Now()))
	ssStub := gostub.Stub(&disttae.RequestSnapshotRead,
		disttae.GetSnapshotReadFnWithHandler(p.T.GetRPCHandle().HandleSnapshotRead))
	defer ssStub.Reset()

	read := p.StartCNTxn()
	defer func() { require.NoError(t, read.Rollback(p.Ctx)) }()
	_, _, rel, err := p.D.Engine.GetRelationById(p.Ctx, read, catalog.MO_TABLES_ID)
	require.NoError(t, err)
	p.D.Engine.ForceGC(p.Ctx, to)
	part := p.D.Engine.GetOrCreateLatestPart(p.Ctx, uint64(catalog.System_Account), catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID)
	stateStart := part.Snapshot().GetStart()
	require.True(t, stateStart.GT(&from), "the requested history must require checkpoint replay")

	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(catalog.System_Account)
	packer.EncodeStringType([]byte(database))
	packer.EncodeStringType([]byte("customers"))
	key := append([]byte(nil), packer.Bytes()...)
	zm := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(zm, key)
	filter := &engine.PKFilter{
		Segments:      [][]byte{zm},
		PrimarySeqnum: catalog.MO_TABLES_CPKEY_IDX,
		ObjectZMIsPK:  true,
		ReplaySpec: &engine.PKReplaySpec{
			Op:   function.EQUAL,
			Keys: [][]byte{append([]byte(nil), readutil.EncodePrimaryKey(key, packer)...)},
		},
	}

	type catalogVersion struct {
		id       uint64
		commitTS types.TS
	}
	collect := func(t *testing.T, ctx context.Context) (versions []catalogVersion, deletedKeys []string) {
		t.Helper()
		h, err := rel.CollectChanges(ctx, from, to, true, p.Mp)
		require.NoError(t, err)
		defer func() { require.NoError(t, h.Close()) }()
		for {
			data, tombstone, _, err := h.Next(ctx, p.Mp)
			require.NoError(t, err)
			if data == nil && tombstone == nil {
				break
			}
			if data != nil {
				ids := vector.MustFixedColWithTypeCheck[uint64](data.Vecs[0])
				commitTS := vector.MustFixedColWithTypeCheck[types.TS](data.Vecs[len(data.Vecs)-1])
				for i, id := range ids {
					versions = append(versions, catalogVersion{id, commitTS[i]})
				}
				data.Clean(p.Mp)
			}
			if tombstone != nil {
				for i := 0; i < tombstone.RowCount(); i++ {
					deletedKeys = append(deletedKeys, string(tombstone.Vecs[0].GetBytesAt(i)))
				}
				tombstone.Clean(p.Mp)
			}
		}
		return
	}

	// The unfiltered control proves there really is unrelated history to prune.
	versions, deletedKeys := collect(t, p.Ctx)
	require.Contains(t, versions, catalogVersion{targetID, created[targetID]})
	require.Contains(t, versions, catalogVersion{otherID, created[otherID]})
	require.NotContains(t, versions, catalogVersion{deletedID, created[deletedID]})
	require.NotEmpty(t, deletedKeys)

	for _, policy := range []engine.SnapshotReadPolicy{
		engine.SnapshotReadPolicyCheckpointReplay, engine.SnapshotReadPolicyVisibleState,
	} {
		name := "checkpoint replay"
		if policy == engine.SnapshotReadPolicyVisibleState {
			name = "visible state"
		}
		t.Run(name, func(t *testing.T) {
			ctx := engine.WithSnapshotReadPolicy(engine.WithPKFilter(p.Ctx, filter), policy)
			versions, deletedKeys := collect(t, ctx)
			require.NotEmpty(t, versions)
			for _, version := range versions {
				require.Equal(t, catalogVersion{targetID, created[targetID]}, version)
			}
			require.Empty(t, deletedKeys, "unrelated catalog deletions must also be pruned")
		})
	}
}
