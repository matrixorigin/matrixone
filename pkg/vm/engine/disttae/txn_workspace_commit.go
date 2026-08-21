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

package disttae

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
)

// workspaceCommitBuilder is the only bridge from CN workspace state to the
// existing TN precommit protocol. Construction freezes logical mutation
// order and pins every referenced payload generation. Build therefore never
// reads workspace mutations and cannot observe compaction, spill or rollback halfway
// through request encoding.
type workspaceCommitBuilder struct {
	entries             *workspaceEntrySet
	droppedTables       workspaceDroppedTables
	op                  client.TxnOperator
	service             string
	syncProtectionJobID string
	mp                  *mpool.MPool
}

func (txn *Transaction) newWorkspaceCommitBuilder() (*workspaceCommitBuilder, error) {
	txn.Lock()
	defer txn.Unlock()

	entries, err := txn.workspace.commitEntries()
	if err != nil {
		return nil, err
	}
	return &workspaceCommitBuilder{
		entries:             entries,
		droppedTables:       txn.workspace.droppedTablesSnapshot(),
		op:                  txn.op,
		service:             txn.proc.GetService(),
		syncProtectionJobID: txn.syncProtectionJobID,
		mp:                  txn.proc.Mp(),
	}, nil
}

// materializeEntry returns an owned commit-only Entry. Logical selections are
// part of a payload generation and must never be applied back to the workspace
// merely to encode a TN request: a later statement-finalization failure may
// still roll the attempt back. The temporary batch is safe to release as soon
// as toPBEntry has copied its vectors into protobuf values.
func (b *workspaceCommitBuilder) materializeEntry(
	view *workspaceEntryView,
) (Entry, func(), error) {
	entry := view.Entry
	if len(view.selections) == 0 {
		return entry, func() {}, nil
	}
	if b.mp == nil {
		return Entry{}, nil, moerr.NewInternalErrorNoCtx(
			"workspace commit builder has no memory pool")
	}
	bat, err := view.bat.Dup(b.mp)
	if err != nil {
		return Entry{}, nil, err
	}
	shrinkBatchWithRowids(bat, view.selections)
	entry.bat = bat
	return entry, func() { bat.Clean(b.mp) }, nil
}

func (b *workspaceCommitBuilder) Close() {
	if b == nil || b.entries == nil {
		return
	}
	b.entries.Close()
}

func (b *workspaceCommitBuilder) Build(ctx context.Context) ([]txn.TxnRequest, error) {
	var pkChkByTN int8
	if v := ctx.Value(defines.PkCheckByTN{}); v != nil {
		pkChkByTN = v.(int8)
	}

	var tnID string
	var tnService metadata.TNService
	entries := make([]*api.Entry, 0, len(b.entries.entries))
	for idx := range b.entries.entries {
		view := &b.entries.entries[idx]
		e, release, err := b.materializeEntry(view)
		if err != nil {
			return nil, err
		}
		if tnID == "" {
			tnID = e.tnStore.ServiceID
			tnService = e.tnStore
		}
		if tnID != "" && tnID != e.tnStore.ServiceID {
			panic(fmt.Sprintf(
				"workspace commit contains entries from different TNs, %s != %s",
				tnID,
				e.tnStore.ServiceID,
			))
		}
		if e.bat == nil || e.bat.IsEmpty() {
			release()
			continue
		}

		e.pkChkByTN = pkChkByTN
		pe, err := toPBEntry(e)
		release()
		if err != nil {
			return nil, err
		}
		// ALTER may produce a delete and an insert. If the table is dropped
		// later in the same transaction, the delete must be sent as an ordinary
		// catalog delete. Otherwise TN must treat the entry as ALTER metadata.
		isAlter, typ, id, name := noteSplitAlter(e.note)
		if b.droppedTables.containsTableID(id) && isAlter && typ == DELETE {
			e.note = noteForDrop(id, name)
		} else if isAlter {
			pe.TableName = "alter"
		}
		entries = append(entries, pe)
	}

	if requiresAutoIncrEpochFenceCommit(entries) &&
		!client.RequireAutoIncrEpochFenceCommit(b.op) {
		return nil, moerr.NewNotSupported(
			ctx,
			"transaction operator cannot enforce AUTO_INCREMENT epochs",
		)
	}
	if len(entries) == 0 {
		return nil, nil
	}

	trace.GetService(b.service).TxnCommit(b.op, entries)
	payload, err := types.Encode(&api.PrecommitWriteCmd{
		EntryList:           entries,
		SyncProtectionJobId: b.syncProtectionJobID,
	})
	if err != nil {
		return nil, err
	}
	reqs := make([]txn.TxnRequest, 0, len(entries))
	for _, info := range tnService.Shards {
		reqs = append(reqs, txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{
				OpCode:  uint32(api.OpCode_OpPreCommit),
				Payload: payload,
				Target: metadata.TNShard{
					TNShardRecord: metadata.TNShardRecord{
						ShardID: info.ShardID,
					},
					ReplicaID: info.ReplicaID,
					Address:   tnService.TxnServiceAddress,
				},
			},
			Options: &txn.TxnRequestOptions{
				RetryCodes:    []int32{int32(moerr.ErrTNShardNotFound)},
				RetryInterval: int64(time.Second),
			},
		})
	}
	return reqs, nil
}
