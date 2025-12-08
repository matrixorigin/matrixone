// Copyright 2025 Matrix Origin
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

package disttae

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"go.uber.org/zap"
)

func (tbl *txnTable) CollectObjectList(
	ctx context.Context,
	from, to types.TS,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	if from.IsEmpty() {
		return collectObjectListFromSnapshot(ctx, tbl, to, mp)
	}
	return collectObjectListFromPartition(ctx, tbl, from, to, mp)
}

func collectObjectListFromSnapshot(
	ctx context.Context,
	tbl *txnTable,
	snapshotTS types.TS,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	// Get partition state for snapshot
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	// Create batch
	bat := logtailreplay.CreateObjectListBatch()

	// Collect object list from snapshot partition state
	_, err = logtailreplay.CollectSnapshotObjectList(ctx, state, snapshotTS, &bat, mp)
	if err != nil {
		bat.Clean(mp)
		return nil, err
	}

	return bat, nil
}

func collectObjectListFromPartition(
	ctx context.Context,
	tbl *txnTable,
	from, to types.TS,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	if to.IsEmpty() || from.GT(&to) {
		return nil, moerr.NewInternalErrorNoCtx("invalid timestamp")
	}

	bat := logtailreplay.CreateObjectListBatch()
	currentPSFrom := types.TS{}
	currentPSTo := types.TS{}
	handleIdx := 0

	fs := tbl.getTxn().engine.fs

	for {
		if currentPSTo.EQ(&to) {
			break
		}

		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
		state, err := tbl.getPartitionState(ctxWithTimeout)
		cancel()
		if err != nil {
			bat.Clean(mp)
			return nil, err
		}

		var nextFrom types.TS
		if currentPSFrom.IsEmpty() {
			nextFrom = from
		} else {
			nextFrom = currentPSTo.Next()
		}

		stateStart := state.GetStart()
		if stateStart.LE(&nextFrom) {
			// Use current partition state
			currentPSTo = to
			currentPSFrom = nextFrom

			logutil.Info("ObjectList-Split collect from partition state",
				zap.String("from", from.ToString()),
				zap.String("to", to.ToString()),
				zap.String("ps from", currentPSFrom.ToString()),
				zap.String("ps to", currentPSTo.ToString()),
				zap.Int("handle idx", handleIdx),
			)

			// Collect object list from partition state
			_, err = logtailreplay.CollectObjectList(ctx, state, currentPSFrom, currentPSTo, &bat, mp)
			if err != nil {
				bat.Clean(mp)
				return nil, err
			}

			handleIdx++
			// Continue to check if we need more data
			if currentPSTo.EQ(&to) {
				break
			}
			continue
		}

		// Need to request snapshot read
		logutil.Info("ObjectList-Split request snapshot read",
			zap.String("from", nextFrom.ToString()),
		)

		ctxWithDeadline, cancel := context.WithTimeout(ctx, time.Minute)
		response, err := RequestSnapshotRead(ctxWithDeadline, tbl, &nextFrom)
		cancel()
		if err != nil {
			bat.Clean(mp)
			return nil, err
		}

		resp, ok := response.(*cmd_util.SnapshotReadResp)
		var checkpointEntries []*checkpoint.CheckpointEntry
		minTS := types.MaxTs()
		maxTS := types.TS{}

		if ok && resp.Succeed && len(resp.Entries) > 0 {
			checkpointEntries = make([]*checkpoint.CheckpointEntry, 0, len(resp.Entries))
			entries := resp.Entries
			for _, entry := range entries {
				logutil.Infof("ObjectList-Split get checkpoint entry: %v", entry.String())
				start := types.TimestampToTS(*entry.Start)
				end := types.TimestampToTS(*entry.End)
				if start.LT(&minTS) {
					minTS = start
				}
				if end.GT(&maxTS) {
					maxTS = end
				}
				entryType := entry.EntryType
				checkpointEntry := checkpoint.NewCheckpointEntry("", start, end, checkpoint.EntryType(entryType))
				checkpointEntry.SetLocation(entry.Location1, entry.Location2)
				checkpointEntries = append(checkpointEntries, checkpointEntry)
			}
		}

		if nextFrom.LT(&minTS) || nextFrom.GT(&maxTS) {
			logutil.Infof("ObjectList-Split nextFrom is not in the checkpoint entry range: %s-%s", minTS.ToString(), maxTS.ToString())
			bat.Clean(mp)
			return nil, moerr.NewErrStaleReadNoCtx(minTS.ToString(), nextFrom.ToString())
		}

		currentPSFrom = nextFrom
		currentPSTo = maxTS
		if to.LT(&maxTS) {
			currentPSTo = to
		}

		logutil.Info("ObjectList-Split collect from checkpoint",
			zap.String("from", from.ToString()),
			zap.String("to", to.ToString()),
			zap.String("ps from", currentPSFrom.ToString()),
			zap.String("ps to", currentPSTo.ToString()),
			zap.Int("handle idx", handleIdx),
		)

		// Collect object list from checkpoint
		sid := tbl.proc.Load().GetService()
		err = logtailreplay.GetObjectListFromCKP(
			ctx,
			tbl.tableId,
			sid,
			currentPSFrom,
			currentPSTo,
			checkpointEntries,
			&bat,
			mp,
			fs,
		)
		if err != nil {
			bat.Clean(mp)
			return nil, err
		}

		handleIdx++
		if currentPSTo.EQ(&to) {
			break
		}
	}

	return bat, nil
}
