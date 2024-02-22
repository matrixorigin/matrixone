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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushTableTailEntry struct {
	txn txnif.AsyncTxn

	taskID     uint64
	tableEntry *catalog.TableEntry

	transMappings      *BlkTransferBooking
	ablksMetas         []*catalog.BlockEntry
	delSrcMetas        []*catalog.BlockEntry
	ablksHandles       []handle.Block
	delSrcHandles      []handle.Block
	createdBlkHandles  []handle.Block
	pageIds            []*common.ID
	nextRoundDirties   []*catalog.BlockEntry
	createdDeletesFile string
	createdMergeFile   string
	dirtyLen           int
	rt                 *dbutils.Runtime
	dirtyEndTs         types.TS
}

func NewFlushTableTailEntry(
	txn txnif.AsyncTxn,
	taskID uint64,
	mapping *BlkTransferBooking,
	tableEntry *catalog.TableEntry,
	ablksMetas []*catalog.BlockEntry,
	nblksMetas []*catalog.BlockEntry,
	ablksHandles []handle.Block,
	nblksHandles []handle.Block,
	createdBlkHandles []handle.Block,
	createdDeletesFile string,
	createdMergeFile string,
	dirtyLen int,
	rt *dbutils.Runtime,
	dirtyEndTs types.TS,
) *flushTableTailEntry {

	entry := &flushTableTailEntry{
		txn:                txn,
		taskID:             taskID,
		transMappings:      mapping,
		tableEntry:         tableEntry,
		ablksMetas:         ablksMetas,
		delSrcMetas:        nblksMetas,
		ablksHandles:       ablksHandles,
		delSrcHandles:      nblksHandles,
		createdBlkHandles:  createdBlkHandles,
		createdDeletesFile: createdDeletesFile,
		createdMergeFile:   createdMergeFile,
		dirtyLen:           dirtyLen,
		rt:                 rt,
		dirtyEndTs:         dirtyEndTs,
	}
	entry.addTransferPages()
	return entry
}

// add transfer pages for dropped ablocks
func (entry *flushTableTailEntry) addTransferPages() {
	isTransient := !entry.tableEntry.GetLastestSchema().HasPK()
	for i, m := range entry.transMappings.Mappings {
		if len(m) == 0 {
			continue
		}
		id := entry.ablksHandles[i].Fingerprint()
		entry.pageIds = append(entry.pageIds, id)
		page := model.NewTransferHashPage(id, time.Now(), isTransient)
		for srcRow, dst := range m {
			blkid := entry.createdBlkHandles[dst.Idx].ID()
			page.Train(uint32(srcRow), *objectio.NewRowid(&blkid, uint32(dst.Row)))
		}
		entry.rt.TransferTable.AddPage(page)
	}
}

// PrepareCommit check deletes between start ts and commit ts
func (entry *flushTableTailEntry) PrepareCommit() error {
	var aconflictCnt, totalTrans int
	// transfer deletes in (startts .. committs] for ablocks
	delTbls := make([]*model.TransDels, len(entry.createdBlkHandles))
	for i, blk := range entry.ablksMetas {
		mapping := entry.transMappings.Mappings[i]
		if len(mapping) == 0 {
			// empty frozen ablocks, it can not has any more deletes
			continue
		}
		dataBlock := blk.GetBlockData()
		bat, err := dataBlock.CollectDeleteInRange(
			entry.txn.GetContext(),
			entry.txn.GetStartTS().Next(),
			entry.txn.GetPrepareTS(),
			false,
			common.MergeAllocator,
		)
		if err != nil {
			return err
		}
		if bat == nil || bat.Length() == 0 {
			continue
		}
		rowid := vector.MustFixedCol[types.Rowid](bat.GetVectorByName(catalog.PhyAddrColumnName).GetDownstreamVector())
		ts := vector.MustFixedCol[types.TS](bat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector())

		count := len(rowid)
		totalTrans += count
		aconflictCnt++
		for i := 0; i < count; i++ {
			row := rowid[i].GetRowOffset()
			destpos, ok := mapping[int(row)]
			if !ok {
				panic(fmt.Sprintf("%s find no transfer mapping for row %d", blk.ID.String(), row))
			}
			if delTbls[destpos.Idx] == nil {
				delTbls[destpos.Idx] = model.NewTransDels(entry.txn.GetPrepareTS())
			}
			delTbls[destpos.Idx].Mapping[destpos.Row] = ts[i]
			if err = entry.createdBlkHandles[destpos.Idx].RangeDelete(
				uint32(destpos.Row), uint32(destpos.Row), handle.DT_MergeCompact, common.MergeAllocator,
			); err != nil {
				return err
			}
		}
		entry.nextRoundDirties = append(entry.nextRoundDirties, blk)
	}
	for i, delTbl := range delTbls {
		if delTbl != nil {
			destid := entry.createdBlkHandles[i].ID()
			entry.rt.TransferDelsMap.SetDelsForBlk(destid, delTbl)
		}
	}

	if aconflictCnt > 0 || totalTrans > 0 {
		logutil.Infof(
			"[FlushTabletail] task %d ww (%s .. %s): on %d ablk, transfer %v rows",
			entry.taskID,
			entry.txn.GetStartTS().ToString(),
			entry.txn.GetPrepareTS().ToString(),
			aconflictCnt,
			totalTrans,
		)
	}
	return nil
}

// PrepareRollback remove transfer page and written files
func (entry *flushTableTailEntry) PrepareRollback() (err error) {
	logutil.Warnf("[FlushTabletail] FT task %d rollback", entry.taskID)
	// remove transfer page
	for _, id := range entry.pageIds {
		_ = entry.rt.TransferTable.DeletePage(id)
	}

	// remove written file
	fs := entry.rt.Fs.Service

	// object for snapshot read of ablocks
	ablkNames := make([]string, 0, len(entry.ablksMetas))
	for _, blk := range entry.ablksMetas {
		if !blk.HasPersistedData() {
			logutil.Infof("[FlushTabletail] skip empty ablk %s when rollback", blk.ID.String())
			continue
		}
		seg := blk.ID.Segment()
		num, _ := blk.ID.Offsets()
		name := objectio.BuildObjectName(seg, num).String()
		ablkNames = append(ablkNames, name)
	}

	// for io task, dispatch by round robin, scope can be nil
	entry.rt.Scheduler.ScheduleScopedFn(&tasks.Context{}, tasks.IOTask, nil, func() error {
		// TODO: variable as timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, name := range ablkNames {
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
func (entry *flushTableTailEntry) ApplyCommit() (err error) {
	for i, blk := range entry.ablksMetas {
		_ = blk.GetBlockData().TryUpgrade()
		blk.GetBlockData().GCInMemeoryDeletesByTS(entry.ablksHandles[i].GetDeltaPersistedTS())
	}

	for i, blk := range entry.delSrcMetas {
		blk.GetBlockData().GCInMemeoryDeletesByTS(entry.delSrcHandles[i].GetDeltaPersistedTS())
	}

	tbl := entry.tableEntry
	tbl.Stats.Lock()
	defer tbl.Stats.Unlock()
	tbl.Stats.LastFlush = entry.dirtyEndTs
	// no merge tasks touch the dirties, we are good to clean all
	if entry.dirtyLen == len(tbl.DeletedDirties) {
		tbl.DeletedDirties = tbl.DeletedDirties[:0]
	} else {
		// some merge tasks touch the dirties, we need to keep those new dirties
		tbl.DeletedDirties = tbl.DeletedDirties[entry.dirtyLen:]
	}
	tbl.DeletedDirties = append(tbl.DeletedDirties, entry.nextRoundDirties...)
	return
}

func (entry *flushTableTailEntry) ApplyRollback() (err error) {
	return
}

func (entry *flushTableTailEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	return &flushTableTailCmd{}, nil
}
func (entry *flushTableTailEntry) IsAborted() bool { return false }
func (entry *flushTableTailEntry) Set1PC()         {}
func (entry *flushTableTailEntry) Is1PC() bool     { return false }

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

////////////////////////////////////////
// transferMapping
////////////////////////////////////////

type DestPos struct {
	Idx int // idx of the created blk
	Row int // destination row number
}

type BlkTransferBooking struct {
	// row in the deleted blk -> row in the created blk
	Mappings []map[int]DestPos
}

func NewBlkTransferBooking(size int) *BlkTransferBooking {
	mappings := make([]map[int]DestPos, size)
	for i := 0; i < size; i++ {
		mappings[i] = make(map[int]DestPos)
	}
	return &BlkTransferBooking{
		Mappings: mappings,
	}
}

func (b *BlkTransferBooking) Clean() {
	for i := 0; i < len(b.Mappings); i++ {
		b.Mappings[i] = make(map[int]DestPos)
	}
}

func (b *BlkTransferBooking) AddSortPhaseMapping(idx int, originRowCnt int, deletes *nulls.Nulls, mapping []int32) {
	// TODO: remove panic check
	if mapping != nil {
		deletecnt := 0
		if deletes != nil {
			deletecnt = deletes.GetCardinality()
		}
		if len(mapping) != originRowCnt-deletecnt {
			panic(fmt.Sprintf("mapping length %d != originRowCnt %d - deletes %s", len(mapping), originRowCnt, deletes))
		}
		// mapping sortedVec[i] = originalVec[sortMapping[i]]
		// transpose it, originalVec[sortMapping[i]] = sortedVec[i]
		// [9 4 8 5 2 6 0 7 3 1](orignVec)  -> [6 9 4 8 1 3 5 7 2 0](sortedVec)
		// [0 1 2 3 4 5 6 7 8 9](sortedVec) -> [0 1 2 3 4 5 6 7 8 9](originalVec)
		// TODO: use a more efficient way to transpose, in place
		transposedMapping := make([]int32, len(mapping))
		for sortedPos, originalPos := range mapping {
			transposedMapping[originalPos] = int32(sortedPos)
		}
		mapping = transposedMapping
	}
	posInVecApplyDeletes := 0
	targetMapping := b.Mappings[idx]
	for origRow := 0; origRow < originRowCnt; origRow++ {
		if deletes != nil && deletes.Contains(uint64(origRow)) {
			// this row has been deleted, skip its mapping
			continue
		}
		if mapping == nil {
			// no sort phase, the mapping is 1:1, just use posInVecApplyDeletes
			targetMapping[origRow] = DestPos{Idx: -1, Row: posInVecApplyDeletes}
		} else {
			targetMapping[origRow] = DestPos{Idx: -1, Row: int(mapping[posInVecApplyDeletes])}
		}
		posInVecApplyDeletes++
	}
}

func (b *BlkTransferBooking) UpdateMappingAfterMerge(mapping, fromLayout, toLayout []uint32) {
	bisectHaystack := make([]uint32, 0, len(toLayout)+1)
	bisectHaystack = append(bisectHaystack, 0)
	for _, x := range toLayout {
		bisectHaystack = append(bisectHaystack, bisectHaystack[len(bisectHaystack)-1]+x)
	}

	// given toLayout and a needle, find its corresponding block index and row index in the block
	// For example, toLayout [8192, 8192, 1024], needle = 0 -> (0, 0); needle = 8192 -> (1, 0); needle = 8193 -> (1, 1)
	bisectPinpoint := func(needle uint32) (int, uint32) {
		i, j := 0, len(bisectHaystack)
		for i < j {
			m := (i + j) / 2
			if bisectHaystack[m] > needle {
				j = m
			} else {
				i = m + 1
			}
		}
		// bisectHaystack[i] is the first number > needle, so the needle falls into i-1 th block
		blkIdx := i - 1
		rows := needle - bisectHaystack[blkIdx]
		return blkIdx, rows
	}

	var totalHandledRows int

	for _, m := range b.Mappings {
		var curTotal int     // index in the flatten src array
		var destTotal uint32 // index in the flatten merged array
		for srcRow := range m {
			curTotal = totalHandledRows + m[srcRow].Row
			if mapping == nil {
				destTotal = uint32(curTotal)
			} else {
				destTotal = mapping[curTotal]
			}
			destBlkIdx, destRowIdx := bisectPinpoint(destTotal)
			m[srcRow] = DestPos{Idx: destBlkIdx, Row: int(destRowIdx)}
		}
		totalHandledRows += len(m)
	}
}
