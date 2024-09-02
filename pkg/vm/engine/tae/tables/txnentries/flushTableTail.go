package txnentries

// Copyright 2021 Matrix Origin
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

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

type flushTableTailEntry struct {
	txn txnif.AsyncTxn

	taskName   string
	tableEntry *catalog.TableEntry

	aobjMetas        []*catalog.ObjectEntry
	aobjHandles      []handle.Object
	createdObjHandle handle.Object
	createdMergeFile string
	transMappings    api.TransferMaps

	atombstonesMetas        []*catalog.ObjectEntry
	atombstoneksHandles     []handle.Object
	createdTombstoneHandles handle.Object
	createdDeletesFile      string

	rt *dbutils.Runtime
	// use TxnMgr.Now as collectTs to do the first collect deletes,
	// which is a relief for the second try in the commit queue
	collectTs types.TS
	// we have to record the ACTUAL commit time of those deletes that happened
	// in the flushing process, before packed them into the created object.
	delTbls []*objectio.Blockid
	// some statistics
	pageIds              []*common.ID
	transCntBeforeCommit int
	nextRoundDirties     map[*catalog.ObjectEntry]struct{}
}

func NewFlushTableTailEntry(
	ctx context.Context,
	txn txnif.AsyncTxn,
	taskName string,
	mapping api.TransferMaps,
	tableEntry *catalog.TableEntry,
	aobjsMetas []*catalog.ObjectEntry,
	aobjsHandles []handle.Object,
	createdObjHandles handle.Object,
	createdMergedObjFile string,
	atombstonesMetas []*catalog.ObjectEntry,
	atombstonesHandles []handle.Object,
	createdTombstoneHandles handle.Object,
	createdMergedTombstoneFile string,
	rt *dbutils.Runtime,
) (*flushTableTailEntry, error) {

	entry := &flushTableTailEntry{
		txn:                     txn,
		taskName:                taskName,
		transMappings:           mapping,
		tableEntry:              tableEntry,
		aobjMetas:               aobjsMetas,
		aobjHandles:             aobjsHandles,
		createdObjHandle:        createdObjHandles,
		createdMergeFile:        createdMergedObjFile,
		atombstonesMetas:        atombstonesMetas,
		atombstoneksHandles:     atombstonesHandles,
		createdTombstoneHandles: createdTombstoneHandles,
		createdDeletesFile:      createdMergedTombstoneFile,
		rt:                      rt,
	}

	if entry.transMappings != nil {
		if entry.createdObjHandle != nil {
			entry.delTbls = make([]*types.Blockid, entry.createdObjHandle.GetMeta().(*catalog.ObjectEntry).GetLatestNode().BlockCnt())
			entry.nextRoundDirties = make(map[*catalog.ObjectEntry]struct{})
			// collect deletes phase 1
			entry.collectTs = rt.Now()
			_, _, ok := fault.TriggerFault("tae: slow transfer deletes")
			if ok {
				time.Sleep(time.Second)
			}
			var err error
			entry.transCntBeforeCommit, err = entry.collectDelsAndTransfer(ctx, entry.txn.GetStartTS(), entry.collectTs)
			if err != nil {
				return nil, err
			}
		}
		// prepare transfer pages
		entry.addTransferPages(ctx)
	}

	return entry, nil
}

// add transfer pages for dropped aobjects
func (entry *flushTableTailEntry) addTransferPages(ctx context.Context) {
	isTransient := !entry.tableEntry.GetLastestSchemaLocked(false).HasPK()
	ioVector := model.InitTransferPageIO()
	pages := make([]*model.TransferHashPage, 0, len(entry.transMappings))
	var duration time.Duration
	var start time.Time
	bts := time.Now().Add(time.Hour)
	for i, m := range entry.transMappings {
		if len(m) == 0 {
			continue
		}
		id := entry.aobjHandles[i].Fingerprint()
		entry.pageIds = append(entry.pageIds, id)
		objectIDs := []*objectio.ObjectId{entry.createdObjHandle.GetID()}
		page := model.NewTransferHashPage(id, bts, isTransient, entry.rt.LocalFs.Service, model.GetTTL(), model.GetDiskTTL(), objectIDs)
		page.Train(m)

		start = time.Now()
		err := model.AddTransferPage(page, ioVector)
		if err != nil {
			return
		}
		duration += time.Since(start)
		pages = append(pages, page)
	}

	start = time.Now()
	model.WriteTransferPage(ctx, entry.rt.LocalFs.Service, pages, *ioVector)
	now := time.Now()
	for _, page := range pages {
		if page.BornTS() != bts {
			page.SetBornTS(now.Add(time.Minute))
		} else {
			page.SetBornTS(now)
		}
		entry.rt.TransferTable.AddPage(page)
	}
	duration += time.Since(start)
	v2.TransferPageFlushLatencyHistogram.Observe(duration.Seconds())
}

// collectDelsAndTransfer collects deletes in flush process and moves them to the created obj
// ATTENTION !!! (from, to] !!!
func (entry *flushTableTailEntry) collectDelsAndTransfer(
	ctx context.Context, from, to types.TS,
) (transCnt int, err error) {
	if len(entry.aobjHandles) == 0 {
		return
	}
	// if created obj handles is nil, all rows in aobjs are deleted
	if entry.createdObjHandle == nil {
		return
	}
	for i, obj := range entry.aobjMetas {
		// For ablock, there is only one block in it.
		// Checking the block mapping once is enough
		mapping := entry.transMappings[i]
		if len(mapping) == 0 {
			// empty frozen aobjects, it can not has any more deletes
			continue
		}
		var bat *containers.Batch
		if bat, err = tables.TombstoneRangeScanByObject(
			ctx,
			entry.tableEntry,
			*obj.ID(),
			from.Next(), // NOTE HERE
			to,
			common.MergeAllocator,
			entry.rt.VectorPool.Small,
		); err != nil {
			return
		}

		if bat == nil || bat.Length() == 0 {
			continue
		}
		rowid := vector.MustFixedCol[types.Rowid](bat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector())
		ts := vector.MustFixedCol[types.TS](bat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector())

		count := len(rowid)
		transCnt += count
		for i := 0; i < count; i++ {
			row := rowid[i].GetRowOffset()
			destpos, ok := mapping[row]
			if !ok {
				panic(fmt.Sprintf("%s find no transfer mapping for row %d", obj.ID().String(), row))
			}
			blkID := objectio.NewBlockidWithObjectID(entry.createdObjHandle.GetID(), destpos.BlkIdx)
			entry.delTbls[destpos.BlkIdx] = blkID
			entry.rt.TransferDelsMap.SetDelsForBlk(*blkID, int(destpos.RowIdx), entry.txn.GetPrepareTS(), ts[i])
			id := entry.createdObjHandle.Fingerprint()
			id.SetBlockOffset(uint16(destpos.BlkIdx))
			if err = entry.createdObjHandle.GetRelation().RangeDelete(
				id, uint32(destpos.RowIdx), uint32(destpos.RowIdx), handle.DT_MergeCompact,
			); err != nil {
				bat.Close()
				return
			}
		}
		bat.Close()
		entry.nextRoundDirties[obj] = struct{}{}
	}
	return
}

// PrepareCommit check deletes between start ts and commit ts
func (entry *flushTableTailEntry) PrepareCommit() error {
	inst := time.Now()
	defer func() {
		v2.TaskCommitTableTailDurationHistogram.Observe(time.Since(inst).Seconds())
	}()
	if entry.transMappings == nil {
		// no del table, no transfer
		return nil
	}
	ctx := context.Background()
	trans, err := entry.collectDelsAndTransfer(ctx, entry.collectTs, entry.txn.GetPrepareTS().Prev())
	if err != nil {
		return err
	}

	if aconflictCnt, totalTrans := len(entry.nextRoundDirties), trans+entry.transCntBeforeCommit; aconflictCnt > 0 || totalTrans > 0 {
		logutil.Info(
			"[FLUSH-PREPARE-COMMIT]",
			zap.String("task", entry.taskName),
			zap.String("commit-ts", entry.txn.GetPrepareTS().ToString()),
			zap.Int("ablks", aconflictCnt),
			zap.Int("transfer-rows", totalTrans),
			zap.Int("in-queue-transfers", trans),
		)
	}
	return nil
}

// PrepareRollback remove transfer page and written files
func (entry *flushTableTailEntry) PrepareRollback() (err error) {
	logutil.Warn(
		"[FLUSH-PREPARE-ROLLBACK]",
		zap.String("task", entry.taskName),
	)
	// remove transfer page
	for _, id := range entry.pageIds {
		_ = entry.rt.TransferTable.DeletePage(id)
	}

	for _, blkID := range entry.delTbls {
		if blkID != nil {
			entry.rt.TransferDelsMap.DeleteDelsForBlk(*blkID)
		}
	}

	// why not clean TranDel?
	// 1. There's little tiny chance for a txn to fail after PrepareCommit
	// 2. If txn failed, no txn will see the transfered deletes,
	//    so no one will consult the TransferDelsMap about the right commite time.
	//    It's ok to leave the DelsMap fade away naturally.

	// remove written file
	fs := entry.rt.Fs.Service

	// object for snapshot read of aobjects
	aobjNames := make([]string, 0, len(entry.aobjMetas))
	for _, obj := range entry.aobjMetas {
		if !obj.HasPersistedData() {
			logutil.Info(
				"[FLUSH-PREPARE-ROLLBACK]",
				zap.String("task", entry.taskName),
				zap.String("extra-info", fmt.Sprintf("skip empty ablk %s when rollback", obj.ID().String())),
			)
			continue
		}
		seg := obj.ID().Segment()
		name := objectio.BuildObjectName(seg, 0).String()
		aobjNames = append(aobjNames, name)
	}
	for _, obj := range entry.atombstonesMetas {
		if !obj.HasPersistedData() {
			logutil.Infof("[FlushTabletail] skip empty atombstone %s when rollback", obj.ID().String())
			continue
		}
		seg := obj.ID().Segment()
		name := objectio.BuildObjectName(seg, 0).String()
		aobjNames = append(aobjNames, name)
	}

	// for io task, dispatch by round robin, scope can be nil
	entry.rt.Scheduler.ScheduleScopedFn(&tasks.Context{}, tasks.IOTask, nil, func() error {
		// TODO: variable as timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, name := range aobjNames {
			_ = fs.Delete(ctx, name)
		}
		if entry.createdDeletesFile != "" {
			_ = fs.Delete(ctx, entry.createdDeletesFile)
		}
		if entry.createdMergeFile != "" {
			_ = fs.Delete(ctx, entry.createdMergeFile)
		}
		return nil
	})
	return
}

// ApplyCommit Gc in memory deletes and update table compact status
func (entry *flushTableTailEntry) ApplyCommit(_ string) (err error) {
	for _, obj := range entry.aobjMetas {
		_ = obj.GetObjectData().TryUpgrade()
	}
	for _, obj := range entry.atombstonesMetas {
		_ = obj.GetObjectData().TryUpgrade()
	}
	return
}

func (entry *flushTableTailEntry) ApplyRollback() (err error) {
	return
}

func (entry *flushTableTailEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	return &flushTableTailCmd{}, nil
}
func (entry *flushTableTailEntry) IsAborted() bool { return false }

////////////////////////////////////////
// flushTableTailCmd
////////////////////////////////////////

type flushTableTailCmd struct{}

func (cmd *flushTableTailCmd) GetType() uint16 { return IOET_WALTxnCommand_Compact }
func (cmd *flushTableTailCmd) WriteTo(w io.Writer) (n int64, err error) {
	typ := IOET_WALTxnCommand_FlushTableTail
	if _, err = w.Write(types.EncodeUint16(&typ)); err != nil {
		return
	}
	n = 2
	ver := IOET_WALTxnCommand_FlushTableTail_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	n = 2
	return
}
func (cmd *flushTableTailCmd) MarshalBinary() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *flushTableTailCmd) ReadFrom(r io.Reader) (n int64, err error) { return }
func (cmd *flushTableTailCmd) UnmarshalBinary(buf []byte) (err error)    { return }
func (cmd *flushTableTailCmd) Desc() string                              { return "CmdName=CPCT" }
func (cmd *flushTableTailCmd) String() string                            { return "CmdName=CPCT" }
func (cmd *flushTableTailCmd) VerboseString() string                     { return "CmdName=CPCT" }
func (cmd *flushTableTailCmd) ApplyCommit()                              {}
func (cmd *flushTableTailCmd) ApplyRollback()                            {}
func (cmd *flushTableTailCmd) SetReplayTxn(txnif.AsyncTxn)               {}
func (cmd *flushTableTailCmd) Close()                                    {}
