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
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

// A scoped SQL scan can reuse the subscribed partition when every account
// allowed by its predicate has been admitted. Keep readiness and the partition
// snapshot in one generation: reconnect clears readiness before replacing the
// partition map, and activation publishes ready only after replay.
func (tbl *txnTable) readyCatalogSnapshot(ctx context.Context, filters []*plan.Expr) *logtailreplay.PartitionState {
	if len(filters) == 0 || ctx.Err() != nil {
		return nil
	}
	eng := tbl.eng.(*Engine)
	lc := eng.pClient.lazyCatalog
	snapshot := tbl.db.op.SnapshotTS()
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	ready := func(id uint32) bool {
		entry := lc.accounts[id]
		return entry != nil && entry.state == accountReady && (entry.readyTS.IsEmpty() || snapshot.GreaterEq(entry.readyTS))
	}
	for _, filter := range filters {
		if !catalogReadAccountsReady(filter, ready, 0) {
			continue
		}
		eng.RLock()
		part := eng.partitions[[2]uint64{tbl.db.databaseId, tbl.tableId}]
		eng.RUnlock()
		if part != nil {
			state := part.Snapshot()
			if state.CanServe(types.TimestampToTS(snapshot)) {
				return state
			}
		}
		return nil
	}
	return nil
}

// Only prove direct integral account equalities. Unsupported predicates fail
// closed to the complete reader. One AND arm bounds the result; every OR arm
// must do so. Never promote an approximate candidate filter into this proof.
func catalogReadAccountsReady(expr *plan.Expr, ready func(uint32) bool, depth int) bool {
	if expr == nil || expr.CandidateFilter || depth > 64 {
		return false
	}
	f := expr.GetF()
	if f == nil || f.Func == nil || len(f.Args) < 2 {
		return false
	}
	switch f.Func.ObjName {
	case "and", "or":
		conjunction := f.Func.ObjName == "and"
		for _, arg := range f.Args {
			if catalogReadAccountsReady(arg, ready, depth+1) == conjunction {
				return conjunction
			}
		}
		return !conjunction
	case "=":
		if len(f.Args) != 2 || f.Args[0] == nil || f.Args[1] == nil {
			return false
		}
		col, bound := f.Args[0].GetCol(), f.Args[1]
		if col == nil {
			col, bound = f.Args[1].GetCol(), f.Args[0]
		}
		// ColPos is scan-local after projection pruning; catalog columns retain
		// their canonical (possibly qualified) names, not relation-wide positions.
		if col == nil || col.Name[strings.LastIndexByte(col.Name, '.')+1:] != catalog.SystemRelAttr_AccID {
			return false
		}
		if folded := bound.GetFold(); folded != nil {
			return folded.IsConst && bound.Typ.Id == int32(types.T_uint32) && len(folded.Data) == 4 &&
				ready(types.DecodeUint32(folded.Data))
		}
		value := bound.GetLit()
		if value == nil || value.Isnull {
			return false
		}
		var id uint64
		switch v := value.Value.(type) {
		case *plan.Literal_U8Val:
			id = uint64(v.U8Val)
		case *plan.Literal_U16Val:
			id = uint64(v.U16Val)
		case *plan.Literal_U32Val:
			id = uint64(v.U32Val)
		case *plan.Literal_U64Val:
			id = v.U64Val
		case *plan.Literal_I8Val:
			id = uint64(v.I8Val)
		case *plan.Literal_I16Val:
			id = uint64(v.I16Val)
		case *plan.Literal_I32Val:
			id = uint64(v.I32Val)
		case *plan.Literal_I64Val:
			id = uint64(v.I64Val)
		default:
			return false
		}
		return id <= math.MaxUint32 && ready(uint32(id))
	}
	return false
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
