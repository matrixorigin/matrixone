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

package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"matrixone/pkg/logutil"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/google/btree"
)

var (
	GlobalSeqNum         uint64 = 0
	ErrParseTableCkpFile        = errors.New("parse table file name error")
)

//func MakeTableCkpFile(tid, version uint64) string {
//	return fmt.Sprintf("%d_v%d", tid, version)
//}

func ParseTableCkpFile(name string) (tid, version uint64, err error) {
	strs := strings.Split(name, "_v")
	if len(strs) != 2 {
		return tid, version, ErrParseTableCkpFile
	}
	if tid, err = strconv.ParseUint(strs[0], 10, 64); err != nil {
		return tid, version, err
	}
	if version, err = strconv.ParseUint(strs[1], 10, 64); err != nil {
		return tid, version, err
	}
	return tid, version, err
}

func NextGlobalSeqNum() uint64 {
	return atomic.AddUint64(&GlobalSeqNum, uint64(1))
}

func GetGlobalSeqNum() uint64 {
	return atomic.LoadUint64(&GlobalSeqNum)
}

type GenericTableWrapper struct {
	ID uint64
	TimeStamp
	LogHistory
}

func NewTable(logIdx uint64, info *MetaInfo, schema *Schema, ids ...uint64) *Table {
	var id uint64
	if len(ids) == 0 {
		id = info.Sequence.GetTableID()
	} else {
		id = ids[0]
	}
	tbl := &Table{
		ID:         id,
		Segments:   make([]*Segment, 0),
		IdMap:      make(map[uint64]int),
		TimeStamp:  *NewTimeStamp(),
		Info:       info,
		Conf:       info.Conf,
		Schema:     schema,
		Stat:       new(Statistics),
		LogHistory: LogHistory{CreatedIndex: logIdx},
	}
	return tbl
}

func (tbl *Table) Marshal() ([]byte, error) {
	return json.Marshal(tbl)
}

func (tbl *Table) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, tbl)
}

func (tbl *Table) ReadFrom(r io.Reader) (int64, error) {
	decoder := json.NewDecoder(r)
	err := decoder.Decode(tbl)
	return decoder.InputOffset(), err
}

func (tbl *Table) GetID() uint64 {
	return tbl.ID
}

func (tbl *Table) GetRows() uint64 {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tbl.Stat)))
	return (*Statistics)(ptr).Rows
}

func (tbl *Table) Less(item btree.Item) bool {
	return tbl.Schema.Name < (item.(*Table)).Schema.Name
}

func (tbl *Table) GetReplayIndex() *LogIndex {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tbl.ReplayIndex)))
	if ptr == nil {
		return nil
	}
	return (*LogIndex)(ptr)
}

func (tbl *Table) ResetReplayIndex() {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tbl.ReplayIndex)))
	if ptr == nil {
		panic("logic error")
	}
	var netIndex *LogIndex
	nptr := (*unsafe.Pointer)(unsafe.Pointer(&netIndex))
	if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&tbl.ReplayIndex)), ptr, *nptr) {
		panic("logic error")
	}
}

func (tbl *Table) AppendStat(rows, size uint64) *Statistics {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tbl.Stat)))
	stat := (*Statistics)(ptr)
	newStat := new(Statistics)
	newStat.Rows = stat.Rows + rows
	newStat.Size = stat.Size + size
	nptr := (*unsafe.Pointer)(unsafe.Pointer(&newStat))
	for !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&tbl.Stat)), ptr, *nptr) {
		ptr = atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tbl.Stat)))
		stat = (*Statistics)(ptr)
		newStat.Rows = stat.Rows + rows
		newStat.Size = stat.Size + size
	}
	return newStat
}

func (tbl *Table) GetSize() uint64 {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&tbl.Stat)))
	return (*Statistics)(ptr).Size
}

func (tbl *Table) CloneSegment(segmentId uint64, ctx CopyCtx) (seg *Segment, err error) {
	tbl.RLock()
	seg, err = tbl.referenceSegmentNoLock(segmentId)
	if err != nil {
		tbl.RUnlock()
		return nil, err
	}
	tbl.RUnlock()
	segCpy := seg.Copy(ctx)
	if !ctx.Attached {
		err = segCpy.Detach()
	}
	return segCpy, err
}

func (tbl *Table) ReferenceBlock(segmentId, blockId uint64) (blk *Block, err error) {
	tbl.RLock()
	seg, err := tbl.referenceSegmentNoLock(segmentId)
	if err != nil {
		tbl.RUnlock()
		return nil, err
	}
	tbl.RUnlock()

	blk, err = seg.ReferenceBlock(blockId)

	return blk, err
}

func (tbl *Table) ReferenceSegment(segmentId uint64) (seg *Segment, err error) {
	tbl.RLock()
	defer tbl.RUnlock()
	seg, err = tbl.referenceSegmentNoLock(segmentId)
	return seg, err
}

func (tbl *Table) referenceSegmentNoLock(segmentId uint64) (seg *Segment, err error) {
	idx, ok := tbl.IdMap[segmentId]
	if !ok {
		return nil, errors.New(fmt.Sprintf("specified segment %d not found in table %d", segmentId, tbl.ID))
	}
	seg = tbl.Segments[idx]
	return seg, nil
}

func (tbl *Table) GetSegmentBlockIDs(segmentId uint64, args ...int64) map[uint64]uint64 {
	tbl.RLock()
	seg, err := tbl.referenceSegmentNoLock(segmentId)
	tbl.RUnlock()
	if err == nil {
		return make(map[uint64]uint64, 0)
	}
	return seg.BlockIDs(args)
}

func (tbl *Table) SegmentIDs(args ...int64) map[uint64]uint64 {
	var ts int64
	if len(args) == 0 {
		ts = NowMicro()
	} else {
		ts = args[0]
	}
	ids := make(map[uint64]uint64)
	tbl.RLock()
	defer tbl.RUnlock()
	for _, seg := range tbl.Segments {
		if !seg.Select(ts) {
			continue
		}
		ids[seg.ID] = seg.ID
	}
	return ids
}

func (tbl *Table) CreateSegment() (seg *Segment, err error) {
	seg = NewSegment(tbl, tbl.Info.Sequence.GetSegmentID())
	return seg, err
}

func (tbl *Table) NextActiveSegment() *Segment {
	var seg *Segment
	if tbl.ActiveSegment >= len(tbl.Segments) {
		return seg
	}
	tbl.ActiveSegment++
	return tbl.GetActiveSegment()
}

func (tbl *Table) GetActiveSegment() *Segment {
	if tbl.ActiveSegment >= len(tbl.Segments) {
		return nil
	}
	seg := tbl.Segments[tbl.ActiveSegment]
	blk := seg.GetActiveBlk()
	if blk == nil && uint64(len(seg.Blocks)) == tbl.Info.Conf.SegmentMaxBlocks {
		return nil
	}
	return seg
}

func (tbl *Table) GetInfullSegment() (seg *Segment, err error) {
	tbl.RLock()
	defer tbl.RUnlock()
	for _, seg := range tbl.Segments {
		if seg.DataState == EMPTY || seg.DataState == PARTIAL {
			return seg, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("no infull segment found in table %d", tbl.ID))
}

func (tbl *Table) String() string {
	s := fmt.Sprintf("Tbl(%d) %d", tbl.ID, tbl.ActiveSegment)
	s += "["
	for i, seg := range tbl.Segments {
		if i != 0 {
			s += "\n"
		}
		s += seg.String()
	}
	if len(tbl.Segments) > 0 {
		s += "\n"
	}
	s += "]"
	return s
}

func (tbl *Table) RegisterSegment(seg *Segment) error {
	if tbl.ID != seg.GetTableID() {
		return errors.New(fmt.Sprintf("table id mismatch %d:%d", tbl.ID, seg.GetTableID()))
	}
	tbl.Lock()
	defer tbl.Unlock()

	err := seg.Attach()
	if err != nil {
		return err
	}

	_, ok := tbl.IdMap[seg.ID]
	if ok {
		return errors.New(fmt.Sprintf("Duplicate segment %d found in table %d", seg.GetID(), tbl.ID))
	}
	tbl.IdMap[seg.GetID()] = len(tbl.Segments)
	tbl.Segments = append(tbl.Segments, seg)
	atomic.StoreUint64(&tbl.SegmentCnt, uint64(len(tbl.Segments)))
	tbl.UpdateVersion()
	return nil
}

func (tbl *Table) GetSegmentCount() uint64 {
	return atomic.LoadUint64(&tbl.SegmentCnt)
}

func (tbl *Table) GetMaxSegIDAndBlkID() (uint64, uint64) {
	blkid := uint64(0)
	segid := uint64(0)
	for _, seg := range tbl.Segments {
		sid := seg.GetID()
		maxBlkId := seg.GetMaxBlkID()
		if maxBlkId > blkid {
			blkid = maxBlkId
		}
		if sid > segid {
			segid = sid
		}
	}

	return segid, blkid
}

func (tbl *Table) UpdateVersion() {
	atomic.AddUint64(&tbl.CheckPoint, uint64(1))
}

func (tbl *Table) GetFileName() string {
	return fmt.Sprintf("%d_v%d", tbl.ID, tbl.CheckPoint)
}

func (tbl *Table) GetLastFileName() string {
	return fmt.Sprintf("%d_v%d", tbl.ID, tbl.CheckPoint-1)
}

func (tbl *Table) Serialize(w io.Writer) error {
	bytes, err := tbl.Marshal()
	if err != nil {
		return err
	}
	_, err = w.Write(bytes)
	return err
}

func (tbl *Table) GetResourceType() ResourceType {
	return ResTable
}

func (tbl *Table) GetTableId() uint64 {
	return tbl.ID
}

func (tbl *Table) LiteCopy() *Table {
	newTbl := &Table{
		ID:         tbl.ID,
		TimeStamp:  tbl.TimeStamp,
		LogHistory: tbl.LogHistory,
	}
	return newTbl
}

func (tbl *Table) Copy(ctx CopyCtx) *Table {
	if ctx.Ts == 0 {
		ctx.Ts = NowMicro()
	}
	newTbl := NewTable(tbl.CreatedIndex, tbl.Info, tbl.Schema, tbl.ID)
	newTbl.TimeStamp = tbl.TimeStamp
	newTbl.CheckPoint = tbl.CheckPoint
	newTbl.BoundSate = tbl.BoundSate
	newTbl.LogHistory = tbl.LogHistory
	newTbl.Conf = tbl.Conf
	for _, v := range tbl.Segments {
		if !v.Select(ctx.Ts) {
			continue
		}
		seg, _ := tbl.CloneSegment(v.ID, ctx)
		newTbl.IdMap[seg.GetID()] = len(newTbl.Segments)
		newTbl.Segments = append(newTbl.Segments, seg)
	}
	newTbl.SegmentCnt = uint64(len(newTbl.Segments))

	return newTbl
}

func (tbl *Table) Replay() {
	ts := NowMicro()
	if len(tbl.Schema.Indices) > 0 {
		if tbl.Schema.Indices[len(tbl.Schema.Indices)-1].ID > tbl.Info.Sequence.NextIndexID {
			tbl.Info.Sequence.NextIndexID = tbl.Schema.Indices[len(tbl.Schema.Indices)-1].ID
		}
	}
	maxTblSegId, maxTblBlkId := tbl.GetMaxSegIDAndBlkID()
	if tbl.ID > tbl.Info.Sequence.NextTableID {
		tbl.Info.Sequence.NextTableID = tbl.ID
	}
	if maxTblSegId > tbl.Info.Sequence.NextSegmentID {
		tbl.Info.Sequence.NextSegmentID = maxTblSegId
	}
	if maxTblBlkId > tbl.Info.Sequence.NextBlockID {
		tbl.Info.Sequence.NextBlockID = maxTblBlkId
	}
	if tbl.IsDeleted(ts) {
		tbl.Info.Tombstone[tbl.ID] = true
	} else {
		tbl.Info.TableIds[tbl.ID] = true
		tbl.Info.NameMap[tbl.Schema.Name] = tbl.ID
		tbl.Info.NameTree.ReplaceOrInsert(tbl)
	}
	tbl.IdMap = make(map[uint64]int)
	segFound := false
	for idx, seg := range tbl.Segments {
		tbl.IdMap[seg.GetID()] = idx
		seg.Table = tbl
		blkFound := false
		for iblk, blk := range seg.Blocks {
			if !blkFound {
				if blk.DataState < FULL {
					blkFound = true
					seg.ActiveBlk = iblk
				} else {
					seg.ActiveBlk++
				}
			}
			blk.Segment = seg
		}
		if !segFound {
			if seg.DataState < FULL {
				segFound = true
				tbl.ActiveSegment = idx
			} else if seg.DataState == FULL {
				blk := seg.GetActiveBlk()
				if blk != nil {
					tbl.ActiveSegment = idx
					segFound = true
				}
			} else {
				tbl.ActiveSegment++
			}
		}
	}
}

func MockTable(info *MetaInfo, schema *Schema, blkCnt uint64) *Table {
	if schema == nil {
		schema = MockSchema(2)
	}
	tbl, _ := info.CreateTable(atomic.AddUint64(&GlobalSeqNum, uint64(1)), schema)
	if err := info.RegisterTable(tbl); err != nil {
		panic(err)
	}
	var activeSeg *Segment
	for i := uint64(0); i < blkCnt; i++ {
		if activeSeg == nil {
			activeSeg, _ = tbl.CreateSegment()
			if err := tbl.RegisterSegment(activeSeg); err != nil {
				panic(err)
			}
		}
		blk, _ := activeSeg.CreateBlock()
		err := activeSeg.RegisterBlock(blk)
		if err != nil {
			logutil.Errorf("seg blks = %d, maxBlks = %d", len(activeSeg.Blocks), activeSeg.MaxBlockCount)
			panic(err)
		}
		if len(activeSeg.Blocks) == int(info.Conf.SegmentMaxBlocks) {
			activeSeg = nil
		}
	}
	return tbl
}
