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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// SourceCommitTS returns the timestamp an async index watermark must cover for
// this relation's partition state at the transaction snapshot. It is the max of
// outstanding source DML commits and the partition-state retention boundary:
// entries before that boundary may have been truncated from local state.
// Transaction workspace writes intentionally fail closed: they are visible to
// the query but cannot have reached ISCP yet.
func (tbl *txnTable) SourceCommitTS(ctx context.Context, mustExceed types.TS) (types.TS, error) {
	// The workspace write list is guarded by the transaction mutex and re-sliced
	// in place by concurrent dumps/compactions; read it under the lock.
	txn := tbl.getTxn()
	txn.Lock()
	hasLocalWrite := false
	for _, entry := range txn.writes {
		if entry.tableId == tbl.tableId && entry.bat != nil && entry.bat.RowCount() > 0 {
			hasLocalWrite = true
			break
		}
	}
	txn.Unlock()
	if hasLocalWrite {
		return types.TS{}, moerr.NewInternalErrorNoCtx("source commit ts is unavailable with transaction-local writes")
	}
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return types.TS{}, err
	}
	proc := tbl.proc.Load()
	if proc == nil {
		return types.TS{}, moerr.NewInternalErrorNoCtx("source commit ts has no process")
	}
	info, err := state.SourceCommitTSAt(
		ctx,
		types.TimestampToTS(tbl.getTxn().op.SnapshotTS()),
		proc.GetFileService(),
		proc.Mp(),
		mustExceed,
	)
	if err != nil {
		return types.TS{}, err
	}
	return info.Max(), nil
}

func (tbl *txnTableDelegate) SourceCommitTS(ctx context.Context, mustExceed types.TS) (types.TS, error) {
	if tbl.combined.is {
		return tbl.combined.tbl.SourceCommitTS(ctx, mustExceed)
	}
	return tbl.origin.SourceCommitTS(ctx, mustExceed)
}

func (t *combinedTxnTable) SourceCommitTS(ctx context.Context, mustExceed types.TS) (types.TS, error) {
	if t.tablesFunc == nil {
		return types.TS{}, moerr.NewInternalErrorNoCtx("source commit ts has no partitioned members")
	}
	relations, err := t.tablesFunc()
	if err != nil {
		return types.TS{}, err
	}
	maxTS := types.TS{}
	for _, relation := range relations {
		provider, ok := relation.(engine.SourceCommitTSProvider)
		if !ok {
			return types.TS{}, moerr.NewInternalErrorNoCtx(
				"source commit ts is unavailable for a partition member",
			)
		}
		sourceTS, err := provider.SourceCommitTS(ctx, mustExceed)
		if err != nil {
			return types.TS{}, err
		}
		if maxTS.IsEmpty() || sourceTS.GT(&maxTS) {
			maxTS = sourceTS
		}
		if !mustExceed.IsEmpty() && maxTS.GT(&mustExceed) {
			return maxTS, nil
		}
	}
	return maxTS, nil
}
