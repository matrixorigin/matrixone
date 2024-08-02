// Copyright 2024 Matrix Origin
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

package testutil

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

type TestCdcEngine struct {
	Engine          *disttae.CdcEngine
	logtailReceiver chan morpc.Message
	broken          chan struct{}
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	txnClient       client.TxnClient
	txnOperator     client.TxnOperator
	timestampWaiter client.TimestampWaiter
}

func NewTestCdcEngine(ctx context.Context, mp *mpool.MPool,
	fs fileservice.FileService, rpcAgent *MockRPCAgent, storage *TestTxnStorage) (*TestCdcEngine, error) {
	de := new(TestCdcEngine)
	de.logtailReceiver = make(chan morpc.Message)
	de.broken = make(chan struct{})

	de.ctx, de.cancel = context.WithCancel(ctx)

	initRuntime()

	wait := make(chan struct{})
	de.timestampWaiter = client.NewTimestampWaiter(runtime.GetLogger(""))

	txnSender := service.NewTestSender(storage)
	de.txnClient = client.NewTxnClient("", txnSender, client.WithTimestampWaiter(de.timestampWaiter))

	de.txnClient.Resume()

	hakeeper := newTestHAKeeperClient()
	colexec.NewServer(hakeeper)

	catalog.SetupDefines("")
	de.Engine = disttae.NewCdcEngine(ctx, "", mp, fs, nil)
	de.Engine.PushClient().LogtailRPCClientFactory = rpcAgent.MockLogtailRPCClientFactory

	go func() {
		done := false
		for !done {
			select {
			case <-wait:
				done = true
			default:
				de.timestampWaiter.NotifyLatestCommitTS(de.Now())
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	op, err := de.txnClient.New(ctx, types.TS{}.ToTimestamp())
	if err != nil {
		return nil, err
	}

	close(wait)

	de.txnOperator = op
	if err = de.Engine.New(ctx, op); err != nil {
		return nil, err
	}

	err = disttae.InitLogTailPushModel(ctx, de.Engine, de.timestampWaiter)
	if err != nil {
		return nil, err
	}

	//qc, _ := qclient.NewQueryClient("", morpc.Config{})
	//sqlExecutor := compile.NewSQLExecutor(
	//	"127.0.0.1:2000",
	//	de.Engine,
	//	mp,
	//	de.txnClient,
	//	fs,
	//	qc,
	//	hakeeper,
	//	nil, //s.udfService
	//)
	//runtime.ServiceRuntime("").SetGlobalVariables(runtime.InternalSQLExecutor, sqlExecutor)
	//err = de.prevSubscribeSysTables(ctx, rpcAgent)
	return de, err
}

func (de *TestCdcEngine) NewTxnOperator(ctx context.Context,
	commitTS timestamp.Timestamp, opts ...client.TxnOption) (client.TxnOperator, error) {
	op, err := de.txnClient.New(ctx, commitTS, opts...)

	op.AddWorkspace(de.txnOperator.GetWorkspace())
	de.txnOperator.GetWorkspace().BindTxnOp(op)

	return op, err
}

func (de *TestCdcEngine) waitLogtail(ctx context.Context) error {
	ts := de.Now()
	ticker := time.NewTicker(time.Second)
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	done := false
	for !done {
		select {
		case <-ctx.Done():
			return moerr.NewInternalErrorNoCtx("wait partition state waterline timeout")
		case <-ticker.C:
			latestAppliedTS := de.Engine.PushClient().LatestLogtailAppliedTime()
			if latestAppliedTS.GreaterEq(ts) && de.Engine.PushClient().IsSubscriberReady() {
				done = true
			}
		}
	}

	return nil
}

func (de *TestCdcEngine) analyzeDataObjects(state *logtailreplay.PartitionState,
	stats *PartitionStateStats, ts types.TS) (err error) {

	iter, err := state.NewObjectsIter(ts, false)
	if err != nil {
		return err
	}

	for iter.Next() {
		item := iter.Entry()
		if item.Visible(ts) {
			stats.DataObjectsVisible.ObjCnt += 1
			stats.DataObjectsVisible.BlkCnt += int(item.BlkCnt())
			stats.DataObjectsVisible.RowCnt += int(item.Rows())
			stats.Details.DataObjectList.Visible = append(stats.Details.DataObjectList.Visible, item)
		} else {
			stats.DataObjectsInvisible.ObjCnt += 1
			stats.DataObjectsInvisible.BlkCnt += int(item.BlkCnt())
			stats.DataObjectsInvisible.RowCnt += int(item.Rows())
			stats.Details.DataObjectList.Invisible = append(stats.Details.DataObjectList.Invisible, item)
		}
	}

	return
}

func (de *TestCdcEngine) analyzeInmemRows(state *logtailreplay.PartitionState,
	stats *PartitionStateStats, ts types.TS) (err error) {

	distinct := make(map[objectio.Blockid]struct{})
	iter := state.NewRowsIter(ts, nil, false)
	for iter.Next() {
		stats.InmemRows.VisibleCnt++
		distinct[iter.Entry().BlockID] = struct{}{}
	}

	stats.InmemRows.VisibleDistinctBlockCnt += len(distinct)
	if err = iter.Close(); err != nil {
		return
	}

	distinct = make(map[objectio.Blockid]struct{})
	iter = state.NewRowsIter(ts, nil, true)
	for iter.Next() {
		distinct[iter.Entry().BlockID] = struct{}{}
		stats.InmemRows.InvisibleCnt++
	}
	stats.InmemRows.InvisibleDistinctBlockCnt += len(distinct)
	err = iter.Close()
	return
}

func (de *TestCdcEngine) analyzeCheckpoint(state *logtailreplay.PartitionState,
	stats *PartitionStateStats, ts types.TS) (err error) {

	ckps := state.Checkpoints()
	for x := range ckps {
		locAndVersions := strings.Split(ckps[x], ";")
		stats.CheckpointCnt += len(locAndVersions) / 2
		for y := 0; y < len(locAndVersions); y += 2 {
			stats.Details.CheckpointLocs[0] = append(stats.Details.CheckpointLocs[0], locAndVersions[y])
			stats.Details.CheckpointLocs[1] = append(stats.Details.CheckpointLocs[1], locAndVersions[y+1])
		}
	}

	return
}

func (de *TestCdcEngine) analyzeTombstone(state *logtailreplay.PartitionState,
	stats *PartitionStateStats, ts types.TS) (outErr error) {

	iter, err := state.NewObjectsIter(ts, true)
	if err != nil {
		return nil
	}

	for iter.Next() {
		disttae.ForeachBlkInObjStatsList(false, nil, func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
			loc, _, ok := state.GetBockDeltaLoc(blk.BlockID)
			if ok {
				bat, _, release, err := blockio.ReadBlockDelete(context.Background(), loc[:], de.Engine.FS())
				if err != nil {
					outErr = err
					return false
				}
				stats.DeltaLocationRowsCnt += bat.RowCount()
				stats.Details.DeletedRows = append(stats.Details.DeletedRows, bat)

				release()
			}
			return true
		}, iter.Entry().ObjectStats)

		if outErr != nil {
			return
		}
	}

	stats.Details.DirtyBlocks = make(map[types.Blockid]struct{})
	iter2 := state.NewDirtyBlocksIter()
	for iter2.Next() {
		item := iter2.Entry()
		stats.Details.DirtyBlocks[item] = struct{}{}
	}

	return
}

func (de *TestCdcEngine) SubscribeTable(ctx context.Context, dbID, tbID uint64, setSubscribed bool) (err error) {
	ticker := time.NewTicker(time.Second)
	timeout := 5

	for range ticker.C {
		if timeout <= 0 {
			logutil.Errorf("test disttae engine subscribe table err %v, timeout", err)
			break
		}

		err = de.Engine.TryToSubscribeTable(ctx, dbID, tbID)
		if err != nil {
			timeout--
			logutil.Errorf("test disttae engine subscribe table err %v, left trie %d", err, timeout)
			continue
		}

		break
	}

	if err == nil && setSubscribed {
		de.Engine.PushClient().SetSubscribeState(dbID, tbID, disttae.Subscribed)
	}

	return
}

func (de *TestCdcEngine) GetPartitionStateStats(ctx context.Context, databaseId, tableId uint64) (
	stats PartitionStateStats, err error) {

	if err = de.waitLogtail(ctx); err != nil {
		return stats, err
	}

	var (
		state *logtailreplay.PartitionState
	)

	ts := types.TimestampToTS(de.Now())
	state = de.Engine.GetOrCreateLatestPart(databaseId, tableId).Snapshot()

	// data objects
	if err = de.analyzeDataObjects(state, &stats, ts); err != nil {
		return
	}

	// ckp count
	if err = de.analyzeCheckpoint(state, &stats, ts); err != nil {
		return
	}

	// in mem rows
	if err = de.analyzeInmemRows(state, &stats, ts); err != nil {
		return
	}

	if err = de.analyzeTombstone(state, &stats, ts); err != nil {
		return
	}
	// tombstones
	return
}

func (de *TestCdcEngine) GetTxnOperator() client.TxnOperator {
	return de.txnOperator
}

func (de *TestCdcEngine) Now() timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
}

func (de *TestCdcEngine) Close(ctx context.Context) {
	de.timestampWaiter.Close()
	de.txnClient.Close()
	close(de.logtailReceiver)
	de.cancel()
	de.wg.Wait()
}
