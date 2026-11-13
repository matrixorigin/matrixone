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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// Replay reads only the accounts whose complete snapshot has already been
// admitted by startup, reconnect, or activation. Ordinary SQL cannot use this
// shortcut: sys catalog scans may include any tenant, including cold ones.
type catalogReplayContextKey struct{}

func (tbl *txnTable) needsAuthoritativeCatalogRead(ctx context.Context) bool {
	if tbl.db.databaseId != catalog.MO_CATALOG_ID || !catalog.IsLazyCatalogTableID(tbl.tableId) {
		return false
	}
	eng := tbl.eng.(*Engine)
	lc := eng.pClient.lazyCatalog
	if lc == nil || !lc.isEnabled() || ctx.Value(catalogReplayContextKey{}) == true {
		return false
	}
	accountID, err := defines.GetAccountId(ctx)
	return err != nil || accountID == 0 || !eng.pClient.CanServeAccount(accountID, tbl.db.op.SnapshotTS())
}

// readCatalogSnapshot does not publish into the filtered push partition or
// change tenant readiness. Its immutable result belongs to the ordinary reader
// and contains a complete TN snapshot plus the CN-only bootstrap catalog rows.
func (tbl *txnTable) readCatalogSnapshot(ctx context.Context) (*logtailreplay.PartitionState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	eng := tbl.eng.(*Engine)
	// BTree.Copy updates the source tree's COW generation, even for an
	// otherwise immutable snapshot. Serialize only the bounded bootstrap copy.
	eng.Lock()
	base := eng.catalogReadBase[tbl.tableId]
	var state *logtailreplay.PartitionState
	if base != nil {
		state = base.Copy()
	}
	eng.Unlock()
	if state == nil {
		return nil, moerr.NewInternalError(ctx, "catalog read bootstrap is not initialized")
	}

	stores := tbl.getTxn().tnStores
	if len(stores) == 0 || len(stores[0].Shards) == 0 {
		return nil, moerr.NewInternalError(ctx, "catalog read has no TN shard")
	}
	store, shard := stores[0], stores[0].Shards[0]
	snapshot := tbl.db.op.SnapshotTS()
	request := api.SyncLogTailReq{
		CnHave: &timestamp.Timestamp{},
		CnWant: &snapshot,
		Table:  &api.TableID{DbId: tbl.db.databaseId, TbId: tbl.tableId},
	}
	payload, err := request.MarshalBinary()
	if err != nil {
		return nil, err
	}

	pullCtx, cancel := context.WithTimeoutCause(ctx, defaultRPCReadTimeout, moerr.CauseReceiveOneLogtail)
	defer cancel()
	result, err := tbl.db.op.Read(pullCtx, []txn.TxnRequest{{
		CNRequest: &txn.CNOpRequest{
			OpCode: uint32(api.OpCode_OpGetLogTail), Payload: payload,
			Target: metadata.TNShard{
				TNShardRecord: metadata.TNShardRecord{ShardID: shard.ShardID},
				ReplicaID:     shard.ReplicaID, Address: store.TxnServiceAddress,
			},
		},
		Options: &txn.TxnRequestOptions{
			RetryCodes: []int32{int32(moerr.ErrTNShardNotFound)}, RetryInterval: int64(time.Second),
		},
	}})
	if err != nil {
		return nil, moerr.AttachCause(pullCtx, err)
	}
	if result == nil {
		return nil, moerr.NewInternalError(ctx, "catalog read returned no response")
	}
	defer result.Release()
	if len(result.Responses) != 1 || result.Responses[0].CNOpResponse == nil {
		return nil, moerr.NewInternalError(ctx, "catalog read returned an invalid response count")
	}
	var tail api.SyncLogTailResp
	if err := tail.UnmarshalBinary(result.Responses[0].CNOpResponse.Payload); err != nil {
		return nil, err
	}
	if tail.CkpLocation != "" {
		checkpoint := logtail.TableLogtail{CkpLocation: tail.CkpLocation}
		parseCkpDuration(&checkpoint)
		if err := taelogtail.ConsumeCheckpointEntries(pullCtx, eng.service, checkpoint.CkpLocation,
			tbl.tableId, tbl.tableName, tbl.db.databaseId, tbl.db.databaseName,
			state.HandleObjectEntry, eng.mp, eng.fs); err != nil {
			return nil, err
		}
	}
	var packer *types.Packer
	put := eng.packerPool.Get(&packer)
	defer put.Put()
	for _, entry := range tail.Commands {
		if err := pullCtx.Err(); err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, moerr.NewInternalError(ctx, "catalog read contains a nil entry")
		}
		state.HandleLogtailEntry(pullCtx, eng.fs, entry, tbl.primarySeqnum, packer, eng.mp)
	}
	if err := pullCtx.Err(); err != nil {
		return nil, err
	}
	ts := types.TimestampToTS(snapshot)
	state.UpdateDuration(types.TS{}, ts)
	state.UpdateAppliedTo(ts)
	return state, nil
}
