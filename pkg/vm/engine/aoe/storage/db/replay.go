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

package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

// -------------------------------------------
// **************** Meta Files ***************
// -------------------------------------------
// 4.ckp
// |  |
// |   --------> Global meta file suffix
//  -----------> Version

// 3_v2.tckp
// |  |  |
// |  |   -----> Table meta file suffix
// |   --------> Version
//  -----------> Table ID

// -------------------------------------------
// **************** Data Files ***************
// -------------------------------------------
// 2_4_3_0.tblk
// | | | |  |
// | | | |   --> Transient block file suffix
// | | |   ----> Version
// | |  -------> Block ID
// |   --------> Segment ID
//  -----------> Table ID

// 2_4_3.blk
// | | |  |
// | | |   ----> Block file suffix
// | |  -------> Block ID
// |   --------> Segment ID
//  -----------> Table ID

// 2_4.seg
// | |  |
// | |   ------> Segment file suffix
// |   --------> Segment ID
//  -----------> Table ID

// -------------------------------------------
// ****** Possiable replay files layout ******
// -------------------------------------------
// {$db}/
//   |-meta/
//   |-data/
// db just created

// {$db}/
//   |-meta/
//   |   |- 1.ckp -----------> (will be gc'ed)
//   |   |- 2.ckp
//   |-data/
// only ddl without dml

// {$db}/
//   |-meta/
//   |   |- 2.ckp
//   |   |- 1_v8.tckp --------> (seg1[blk1,blk2], seg2[blk3,blk4])
//   |-data/
//       |- 1_2_4.blk
//       |- 1_2_3.blk
//       |- 1_1.seg
// One table with 2 segments(seg1, seg2). seg1 is a sorted seg while seg2 is an unsorted seg.

// {$db}/
//   |-meta/
//   |   |- 1.ckp
//   |   |- 1_v8.tckp
//   |-data/
//       |- 1_2_4.blk
//       |- 1_1.seg
// AOE supports parallel writing of block files, the sequence of block file flush is different
// from the sequence of data. For example, blk3 and blk4 are both in flush queue and blk4 is
// successfully flushed first. Then the db crashes. When restarting, it should replay from the
// indx just before blk3, and erase all data files after blk3

// {$db}/
//   |-meta/
//   |   |- 1.ckp
//   |   |- 1_v8.tckp
//   |-data/
//       |- 1_2_4.blk
//       |- 1_2_4_1.tblk
//       |- 1_2_4_0.tblk
//       |- 1_2_3_0.tblk
//       |- 1_1.seg

// {$db}/
//   |-meta/
//   |   |- 1.ckp
//   |   |- 1_v8.tckp
//   |-data/
//       |- 1_2_4_1.tblk
//       |- 1_2_3.tblk
//       |- 1_1.seg

type IReplayObserver interface {
	OnRemove(string)
}

type flushsegCtx struct {
	id common.ID
}

type cleanable interface {
	clean()
}

type blockfile struct {
	h         *replayHandle
	id        common.ID
	name      string
	transient bool
	next      *blockfile
	commited  bool
	meta      *metadata.Block
	ver       uint64
}

func (bf *blockfile) version() uint64 {
	if bf.transient {
		return bf.ver
	} else {
		return ^uint64(0)
	}
}

func (bf *blockfile) markCommited() {
	bf.commited = true
}

func (bf *blockfile) isCommited() bool {
	return bf.commited
}

func (bf *blockfile) isTransient() bool {
	return bf.transient
}

func (bf *blockfile) clean() {
	bf.h.doRemove(bf.name)
}

type sortedSegmentFile struct {
	h    *replayHandle
	name string
	id   common.ID
}

type unsortedSegmentFile struct {
	h          *replayHandle
	id         common.ID
	files      map[common.ID]*blockfile
	uncommited []*blockfile
	meta       *metadata.Segment
}

type bsiFile struct {
	h    *replayHandle
	id   common.ID
	name string
}

func (bf *bsiFile) clean() {
	bf.h.doRemove(bf.name)
}

func newUnsortedSegmentFile(id common.ID, h *replayHandle) *unsortedSegmentFile {
	return &unsortedSegmentFile{
		h:          h,
		id:         id,
		files:      make(map[common.ID]*blockfile),
		uncommited: make([]*blockfile, 0),
	}
}

func (sf *sortedSegmentFile) clean() {
	sf.h.doRemove(sf.name)
}

func (sf *sortedSegmentFile) size() int64 {
	stat, _ := os.Stat(sf.name)
	return stat.Size()
}

func (usf *unsortedSegmentFile) addBlock(bid common.ID, name string, ver uint64, transient bool) {
	id := bid.AsBlockID()
	bf := &blockfile{id: bid, name: name, transient: transient, h: usf.h, ver: ver}
	head := usf.files[id]
	if head == nil {
		usf.files[id] = bf
		return
	}
	var prev *blockfile
	curr := head
	for curr != nil {
		if curr.version() < bf.version() {
			bf.next = curr
			if prev == nil {
				usf.files[id] = bf
			} else {
				prev.next = bf
			}
			return
		} else if curr.version() > bf.version() {
			prev = curr
			curr = curr.next
		} else {
			panic("logic error")
		}
	}
	prev.next = bf
}

func (usf *unsortedSegmentFile) hasblock(id common.ID) bool {
	_, ok := usf.files[id]
	return ok
}

func (usf *unsortedSegmentFile) isfull(maxcnt int) bool {
	if len(usf.files) != maxcnt {
		return false
	}
	for id, _ := range usf.files {
		meta := usf.meta.SimpleGetBlock(id.BlockID)
		if meta == nil {
			panic(metadata.BlockNotFoundErr)
		}
		if !meta.IsFullLocked() {
			return false
		}
	}
	return true
}

func (usf *unsortedSegmentFile) clean() {
	for _, f := range usf.files {
		for f != nil {
			f.clean()
			f = f.next
		}
	}
}

func (usf *unsortedSegmentFile) tryCleanBlocks(cleaner *replayHandle, meta *metadata.Segment) {
	usf.meta = meta
	files := make(map[common.ID]*blockfile)
	for id, file := range usf.files {
		blk := meta.SimpleGetBlock(file.id.BlockID)
		if blk == nil {
			// For block that not found in metadata, just remove it. This situation is only
			// possible if the metadata and data are committed asynchronously
			head := file
			for head != nil {
				cleaner.addCleanable(file)
				head = head.next
			}
			continue
		}
		if blk.CommitInfo.Op >= metadata.OpUpgradeFull {
			// Block was committed as FULL
			file.markCommited()
		} else {
			// Block was not committed as FULL.
			// Multi-version compositions for a block file:
			// 1. *.blk
			// 2. *.blk -> *_0.tblk[T]
			// 3. *.blk -> *_2.tblk[T] -> *_1.tblk[T] -> *_0.tblk[T]
			// 4. *_0.tblk[T]
			// 5. *_2.tblk[T] -> *_1.tblk[T] -> *_0.tblk[T]
			head := file
			if !head.isTransient() {
				logutil.Infof("1. Add cleanable %s", head.name)
				cleaner.addCleanable(head)
				head = head.next
			}
			if head == nil {
				continue
			}
			head.meta = blk
			usf.uncommited = append(usf.uncommited, head)
			file = head
		}

		// Multi-version block files linked from latest to oldest. Here we just keep
		// the latest version and remove others
		head := file.next
		for head != nil {
			logutil.Infof("2. Add cleanable %s", head.name)
			cleaner.addCleanable(head)
			head = head.next
		}
		files[id] = file
	}
	usf.files = files
}

type tableDataFiles struct {
	sortedfiles   map[common.ID]*sortedSegmentFile
	unsortedfiles map[common.ID]*unsortedSegmentFile
	bsifiles      map[common.ID]*bsiFile
}

func (tdf *tableDataFiles) HasBlockFile(id *common.ID) bool {
	sid := id.AsSegmentID()
	unsorted := tdf.unsortedfiles[sid]
	if unsorted == nil {
		return false
	}
	return unsorted.hasblock(*id)
}

func (tdf *tableDataFiles) HasSegementFile(id *common.ID) bool {
	unsorted := tdf.unsortedfiles[*id]
	if unsorted != nil {
		return true
	}
	sorted := tdf.sortedfiles[*id]
	return sorted != nil
}

func (ctx *tableDataFiles) PresentedBsiFiles(id common.ID) []string {
	files := make([]string, 0)
	for cid, file := range ctx.bsifiles {
		if cid.IsSameSegment(id) {
			files = append(files, file.name)
		}
	}
	return files
}

func (tdf *tableDataFiles) clean() {
	for _, file := range tdf.sortedfiles {
		file.clean()
	}
	for _, file := range tdf.unsortedfiles {
		file.clean()
	}
	for _, file := range tdf.bsifiles {
		file.clean()
	}
}

type replayHandle struct {
	catalog    *metadata.Catalog
	tables     *table.Tables
	others     []string
	workDir    string
	dataDir    string
	mask       *roaring.Bitmap
	cleanables []cleanable
	files      map[uint64]*tableDataFiles
	flushsegs  []flushsegCtx
	indicesMap map[common.ID][]uint16
	compactdbs []*metadata.Database
	observer   IReplayObserver
	cbs        []func() error
}

func NewReplayHandle(workDir string, catalog *metadata.Catalog, tables *table.Tables, observer IReplayObserver) *replayHandle {
	fs := &replayHandle{
		catalog:    catalog,
		tables:     tables,
		workDir:    workDir,
		others:     make([]string, 0),
		mask:       roaring.NewBitmap(),
		files:      make(map[uint64]*tableDataFiles),
		cleanables: make([]cleanable, 0),
		flushsegs:  make([]flushsegCtx, 0),
		indicesMap: make(map[common.ID][]uint16),
		compactdbs: make([]*metadata.Database, 0),
		observer:   observer,
		cbs:        make([]func() error, 0),
	}
	empty := false
	var err error
	{
		dataDir := common.MakeDataDir(workDir)
		if _, err = os.Stat(dataDir); os.IsNotExist(err) {
			err = os.MkdirAll(dataDir, os.ModePerm)
			empty = true
		}
		fs.dataDir = dataDir
	}
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}
	if empty {
		return fs
	}

	dataFiles, err := ioutil.ReadDir(fs.dataDir)
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}

	for _, database := range catalog.Databases {
		for _, tbl := range database.TableSet {
			indices := tbl.Schema.Indices2
			for _, idx := range indices {
				// todo: str bsi
				if idx.Type == metadata.NumBsi {
					for _, seg := range tbl.SegmentSet {
						if !seg.IsSortedLocked() {
							continue
						}
						for _, col := range idx.Columns {
							id := seg.AsCommonID()
							id.Idx = col
							fs.indicesMap[*id] = append(fs.indicesMap[*id], col)
						}
					}
				}
			}
		}
	}

	for _, file := range dataFiles {
		fs.addDataFile(file.Name())
	}
	for _, file := range dataFiles {
		fs.addIndexFile(file.Name())
	}
	return fs
}

func (h *replayHandle) ScheduleEvents(opts *storage.Options, tables *table.Tables) {
	for _, ctx := range h.flushsegs {
		t, _ := tables.WeakRefTable(ctx.id.TableID)
		segment := t.StrongRefSegment(ctx.id.SegmentID)
		if segment == nil {
			panic(fmt.Sprintf("segment %d is nil", ctx.id.SegmentID))
		}
		flushCtx := &sched.Context{Opts: opts}
		flushEvent := sched.NewFlushSegEvent(flushCtx, segment)
		opts.Scheduler.Schedule(flushEvent)
	}
	h.flushsegs = h.flushsegs[:0]
	for id, cols := range h.indicesMap {
		t, _ := tables.WeakRefTable(id.TableID)
		segment := t.StrongRefSegment(id.SegmentID)
		if segment == nil {
			panic("unexpected error")
		}
		flushCtx := &sched.Context{Opts: opts}
		flushEvent := sched.NewFlushIndexEvent(flushCtx, segment)
		flushEvent.Cols = cols
		opts.Scheduler.Schedule(flushEvent)
	}
	h.indicesMap = make(map[common.ID][]uint16)
	for _, database := range h.compactdbs {
		gcReq := gcreqs.NewDropDBRequest(opts, database, tables)
		opts.GC.Acceptor.Accept(gcReq)
	}
	h.compactdbs = h.compactdbs[:0]
}

func (h *replayHandle) addCleanable(f cleanable) {
	h.cleanables = append(h.cleanables, f)
}

func (h *replayHandle) addBlock(id common.ID, name string, ver uint64, transient bool) {
	tbl, ok := h.files[id.TableID]
	if !ok {
		tbl = &tableDataFiles{
			unsortedfiles: make(map[common.ID]*unsortedSegmentFile),
			sortedfiles:   make(map[common.ID]*sortedSegmentFile),
			bsifiles:      make(map[common.ID]*bsiFile),
		}
		h.files[id.TableID] = tbl
	}
	segId := id.AsSegmentID()
	file, ok := tbl.unsortedfiles[segId]
	if !ok {
		tbl.unsortedfiles[segId] = newUnsortedSegmentFile(segId, h)
		file = tbl.unsortedfiles[segId]
	}
	file.addBlock(id, name, ver, transient)
}

func (h *replayHandle) addSegment(id common.ID, name string) {
	tbl, ok := h.files[id.TableID]
	if !ok {
		tbl = &tableDataFiles{
			unsortedfiles: make(map[common.ID]*unsortedSegmentFile),
			sortedfiles:   make(map[common.ID]*sortedSegmentFile),
			bsifiles:      make(map[common.ID]*bsiFile),
		}
		h.files[id.TableID] = tbl
	}
	_, ok = tbl.sortedfiles[id]
	if ok {
		panic("logic error")
	}
	tbl.sortedfiles[id] = &sortedSegmentFile{
		h:    h,
		id:   id,
		name: name,
	}
}

// addBSI basically means replace if exists
func (h *replayHandle) addBSI(id common.ID, filename string) {
	tbl, ok := h.files[id.TableID]
	if !ok {
		tbl = &tableDataFiles{
			unsortedfiles: make(map[common.ID]*unsortedSegmentFile),
			sortedfiles:   make(map[common.ID]*sortedSegmentFile),
			bsifiles:      make(map[common.ID]*bsiFile),
		}
		h.files[id.TableID] = tbl
	}
	tbl.bsifiles[id] = &bsiFile{
		h:    h,
		id:   id,
		name: filename,
	}
	if cols, ok := h.indicesMap[id]; ok {
		for i, col := range cols {
			if col == id.Idx {
				if i == len(cols)-1 {
					cols = cols[:len(cols)-1]
				} else {
					cols = append(cols[:i], cols[i+1:]...)
				}
			}
		}
		h.indicesMap[id] = cols
	}
}

func (h *replayHandle) addDataFile(fname string) {
	if name, ok := common.ParseTBlockfileName(fname); ok {
		count, _, id, err := dataio.ParseTBlockfileName(name)
		if err != nil {
			panic(err)
		}
		fullname := path.Join(h.dataDir, fname)
		h.addBlock(id, fullname, count, true)
		return
	}
	if name, ok := common.ParseBlockfileName(fname); ok {
		id, err := common.ParseBlkNameToID(name)
		if err != nil {
			panic(err)
		}
		fullname := path.Join(h.dataDir, fname)
		h.addBlock(id, fullname, 0, false)
		return
	}
	if name, ok := common.ParseSegmentFileName(fname); ok {
		id, err := common.ParseSegmentNameToID(name)
		if err != nil {
			panic(err)
		}
		fullname := path.Join(h.dataDir, fname)
		h.addSegment(id, fullname)
		return
	}
	if _, ok := common.ParseBitSlicedIndexFileName(fname); ok {
		return
	}
	h.others = append(h.others, path.Join(h.dataDir, fname))
}

func (h *replayHandle) addIndexFile(fname string) {
	if name, ok := common.ParseBitSlicedIndexFileName(fname); ok {
		version, tid, sid, col, ok := common.ParseBitSlicedIndexFileNameToInfo(name)
		if !ok {
			panic("unexpected error")
		}
		id := &common.ID{TableID: tid, SegmentID: sid}
		if h.files[tid] == nil || !h.files[tid].HasSegementFile(id) {
			h.others = append(h.others, path.Join(h.dataDir, fname))
			return
		}
		id.Idx = col
		if bf, ok := h.files[tid].bsifiles[*id]; ok {
			fn, _ := common.ParseBitSlicedIndexFileName(bf.name)
			v, _, _, _, _ := common.ParseBitSlicedIndexFileNameToInfo(fn)
			if version > v {
				h.others = append(h.others, bf.name)
				h.addBSI(*id, path.Join(h.dataDir, fname))
				logutil.Infof("detect stale index file | %s", bf.name)
			} else {
				h.others = append(h.others, path.Join(h.dataDir, fname))
				logutil.Infof("detect stale index file | %s", fname)
			}
		} else {
			h.addBSI(*id, path.Join(h.dataDir, fname))
		}
		return
	}
}

func (h *replayHandle) rebuildTable(meta *metadata.Table) error {
	var err error
	tablesFiles, ok := h.files[meta.Id]
	if !ok {
		// No need to change the table metadata if there is no table files
		if h.tables != nil && !meta.IsDeleted() {
			_, err = h.tables.RegisterTable(meta)
		}
		return err
	}
	if meta.IsDeleted() {
		h.addCleanable(tablesFiles)
		if meta.IsHardDeleted() {
			return nil
		}
		err = meta.HardDelete()
		return err
	}

	for i := len(meta.SegmentSet) - 1; i >= 0; i-- {
		segment := meta.SegmentSet[i]
		if segment.CommitInfo.Op == metadata.OpUpgradeSorted {
			// The following segments should be all SORTED
			break
		}
		file := tablesFiles.sortedfiles[*segment.AsCommonID()]
		if file != nil {
			// There exists segments with sorted segment files but their metadata were not committed
			// as SORTED. For example, a crash happened after creating a sorted segment file and
			// before committing the metadata as SORTED. These segments will be committed as SORTED
			// during replaying.
			segment.SimpleUpgrade(file.size(), nil)
			continue
		}
	}
	for id, _ := range tablesFiles.sortedfiles {
		unsorted, ok := tablesFiles.unsortedfiles[id]
		if ok {
			// There are multi versions for a segment (Ex. Unsorted -> Sorted). Under normal
			// circumstances, once the Sorted version is generated, the Unsorted version will
			// be GC'ed in a short period of time. When restarting, we need to check and
			// delete the stale versions
			h.addCleanable(unsorted)
			delete(tablesFiles.unsortedfiles, id)
		}
	}

	flushsegs := make([]common.ID, 0)
	unclosedSegFiles := make([]*unsortedSegmentFile, 0)
	for id, unsorted := range tablesFiles.unsortedfiles {
		segMeta := meta.SimpleGetSegment(id.SegmentID)
		if segMeta == nil {
			// For segment that not found in metadata, just remove it. This situation is only
			// possible if the metadata and data are committed asynchronously
			h.addCleanable(unsorted)
			continue
		} else {
			// log.Info(segMeta.String())
			unsorted.tryCleanBlocks(h, segMeta)
		}
		if !unsorted.isfull(int(meta.Schema.SegmentMaxBlocks)) {
			unclosedSegFiles = append(unclosedSegFiles, unsorted)
			continue
		}

		flushsegs = append(flushsegs, id)
	}
	sort.Slice(flushsegs, func(i, j int) bool { return flushsegs[i].SegmentID < flushsegs[j].SegmentID })
	for _, id := range flushsegs {
		h.flushsegs = append(h.flushsegs, flushsegCtx{id: id})
	}
	h.processUnclosedSegmentFiles(unclosedSegFiles, meta)

	if h.tables == nil {
		return nil
	}

	data, err := h.tables.PrepareInstallTable(meta, tablesFiles)
	if err != nil {
		return err
	}
	err = h.tables.InstallTable(data)
	return err
}

func (h *replayHandle) processUnclosedSegmentFile(file *unsortedSegmentFile) {
	if len(file.uncommited) == 0 {
		return
	}
	sort.Slice(file.uncommited, func(i, j int) bool {
		return file.uncommited[i].id.BlockID < file.uncommited[j].id.BlockID
	})
	bf := file.uncommited[0]
	if !bf.isTransient() {
		h.addCleanable(bf)
		return
	}
	emeta := bf.meta.Segment.FirstInFullBlock()
	if emeta != bf.meta {
		logutil.Infof("3. Add cleanable %s", bf.name)
		logutil.Infof("%s -- %s", emeta.String(), bf.meta.String())
		h.addCleanable(bf)
		return
	}

	files := file.uncommited[1:]
	// TODO: uncommited block file can be converted to committed
	for _, f := range files {
		h.addCleanable(f)
	}
}

func (h *replayHandle) processUnclosedSegmentFiles(files []*unsortedSegmentFile, meta *metadata.Table) {
	if len(files) == 0 {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].id.SegmentID < files[j].id.SegmentID
	})
	for i := 0; i < len(meta.SegmentSet); i++ {
		if len(files) == 0 {
			break
		}
		seg := meta.SegmentSet[i]
		if seg.IsSortedLocked() || (seg.HasMaxBlocks() && seg.BlockSet[len(seg.BlockSet)-1].IsFullLocked()) {
			file := files[0]
			if file.meta == seg {
				files = files[1:]
			}
		} else {
			file := files[0]
			if file.meta == seg {
				h.processUnclosedSegmentFile(file)
				files = files[1:]
			}
			for _, file := range files {
				h.addCleanable(file)
			}
			break
		}
	}
}

func (h *replayHandle) Replay() error {
	for _, database := range h.catalog.Databases {
		for _, tbl := range database.TableSet {
			if err := h.rebuildTable(tbl); err != nil {
				return err
			}
		}
		if database.IsDeleted() && !database.IsHardDeleted() {
			h.compactdbs = append(h.compactdbs, database)
		}
	}
	h.Cleanup()
	logutil.Infof(h.String())
	return nil
}

func (h *replayHandle) doRemove(name string) {
	os.Remove(name)
	if h.observer != nil {
		h.observer.OnRemove(name)
	}
	logutil.Infof("%s | Removed", name)
}

func (h *replayHandle) cleanupFile(fname string) {
	h.doRemove(fname)
}

func (h *replayHandle) Cleanup() {
	if h.cleanables != nil {
		for _, f := range h.cleanables {
			f.clean()
		}
		h.cleanables = nil
	}
	if h.others != nil {
		for _, f := range h.others {
			h.cleanupFile(f)
		}
	}
	h.files = nil
}

func (h *replayHandle) String() string {
	s := fmt.Sprintf("[InfoFiles]:")
	for tid, tbl := range h.files {
		s = fmt.Sprintf("%s\nTable %d [Sorted]: %d", s, tid, len(tbl.sortedfiles))
		for _, fs := range tbl.sortedfiles {
			s = fmt.Sprintf("%s\n%s", s, fs.name)
		}
		s = fmt.Sprintf("%s\nTable %d [Unsorted]: %d", s, tid, len(tbl.unsortedfiles))
		for _, fs := range tbl.unsortedfiles {
			s = fmt.Sprintf("%s\n%s", s, fs.id.SegmentString())
		}
	}
	s = fmt.Sprintf("%s\n[Others]: %d", s, len(h.others))
	for _, other := range h.others {
		s = fmt.Sprintf("%s\n%s", s, other)
	}
	return s
}
