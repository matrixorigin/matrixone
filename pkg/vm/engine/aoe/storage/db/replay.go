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
	"matrixone/pkg/logutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path"
	"sort"
	"sync"

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

type tableFile struct {
	name    string
	version uint64
	id      uint64
	next    *tableFile
}

type blockfile struct {
	h         *replayHandle
	id        common.ID
	name      string
	transient bool
	next      *blockfile
	commited  bool
	meta      *md.Block
}

func (bf *blockfile) version() uint32 {
	if bf.transient {
		return bf.id.PartID
	} else {
		return ^uint32(0)
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
	meta       *md.Segment
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

func (usf *unsortedSegmentFile) addBlock(bid common.ID, name string, transient bool) {
	id := bid.AsBlockID()
	bf := &blockfile{id: bid, name: name, transient: transient, h: usf.h}
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

func (usf *unsortedSegmentFile) isfull(maxcnt int) bool {
	if len(usf.files) != maxcnt {
		return false
	}
	for id, _ := range usf.files {
		meta, err := usf.meta.ReferenceBlock(id.BlockID)
		if err != nil {
			panic(err)
		}
		if meta.DataState < md.FULL {
			return false
		}
	}
	return true
}

func (usf *unsortedSegmentFile) clean() {
	for _, f := range usf.files {
		f.clean()
	}
}

func (usf *unsortedSegmentFile) tryCleanBlocks(cleaner *replayHandle, meta *md.Segment) {
	usf.meta = meta
	files := make(map[common.ID]*blockfile)
	for id, file := range usf.files {
		blk, err := meta.ReferenceBlock(file.id.BlockID)
		if err != nil {
			head := file
			for head != nil {
				cleaner.addCleanable(file)
				head = head.next
			}
			continue
		}
		if blk.DataState > md.PARTIAL {
			file.markCommited()
		} else {
			head := file
			if !head.isTransient() {
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

		head := file.next
		for head != nil {
			cleaner.addCleanable(head)
			head = head.next
		}
		files[id] = file
	}
	usf.files = files
}

func (f *tableFile) Open() *os.File {
	r, err := os.OpenFile(f.name, os.O_RDONLY, 06666)
	if err != nil {
		panic(err)
	}
	return r
}

type infoFile struct {
	name    string
	version uint64
	next    *infoFile
}

type tableDataFiles struct {
	sortedfiles   map[common.ID]*sortedSegmentFile
	unsortedfiles map[common.ID]*unsortedSegmentFile
}

func (tdf *tableDataFiles) clean() {
	for _, file := range tdf.sortedfiles {
		file.clean()
	}
	for _, file := range tdf.unsortedfiles {
		file.clean()
	}
}

type replayHandle struct {
	infos         *infoFile
	tables        map[uint64]*tableFile
	others        []string
	workDir       string
	metaDir       string
	dataDir       string
	tablesToClean map[uint64]*tableFile
	mask          *roaring.Bitmap
	cleanables    []cleanable
	files         map[uint64]*tableDataFiles
	flushsegs     []flushsegCtx
	observer      IReplayObserver
}

func NewReplayHandle(workDir string, observer IReplayObserver) *replayHandle {
	fs := &replayHandle{
		workDir:    workDir,
		tables:     make(map[uint64]*tableFile),
		others:     make([]string, 0),
		mask:       roaring.NewBitmap(),
		files:      make(map[uint64]*tableDataFiles),
		cleanables: make([]cleanable, 0),
		flushsegs:  make([]flushsegCtx, 0),
		observer:   observer,
	}
	empty := false
	var err error
	{
		dataDir := e.MakeDataDir(workDir)
		if _, err = os.Stat(dataDir); os.IsNotExist(err) {
			err = os.MkdirAll(dataDir, os.ModePerm)
		}
		fs.dataDir = dataDir
	}
	metaDir := e.MakeMetaDir(workDir)
	if _, err = os.Stat(metaDir); os.IsNotExist(err) {
		err = os.MkdirAll(metaDir, 0755)
		empty = true
	}
	fs.metaDir = metaDir
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}
	if empty {
		return fs
	}

	metaFiles, err := ioutil.ReadDir(fs.metaDir)
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}
	for _, file := range metaFiles {
		fs.addMetaFile(file.Name())
	}

	dataFiles, err := ioutil.ReadDir(fs.dataDir)
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}
	for _, file := range dataFiles {
		fs.addDataFile(file.Name())
	}
	return fs
}

func (h *replayHandle) ScheduleEvents(opts *e.Options, tables *table.Tables) {
	for _, ctx := range h.flushsegs {
		t, _ := tables.WeakRefTable(ctx.id.TableID)
		segment := t.StrongRefSegment(ctx.id.SegmentID)
		if segment == nil {
			panic(fmt.Sprintf("segment %d is nil", ctx.id.SegmentID))
		}
		flushCtx := &dbsched.Context{Opts: opts}
		flushEvent := dbsched.NewFlushSegEvent(flushCtx, segment)
		opts.Scheduler.Schedule(flushEvent)
	}
	h.flushsegs = h.flushsegs[:0]
}

func (h *replayHandle) addCleanable(f cleanable) {
	h.cleanables = append(h.cleanables, f)
}

func (h *replayHandle) addTable(f *tableFile) {
	head := h.tables[f.id]
	if head == nil {
		h.tables[f.id] = f
		return
	}
	var prev *tableFile
	curr := head
	for curr != nil {
		if curr.version < f.version {
			f.next = curr
			if prev == nil {
				h.tables[f.id] = f
			} else {
				prev.next = f
			}
			return
		} else if curr.version > f.version {
			prev = curr
			curr = curr.next
		} else {
			panic("logic error")
		}
	}
	prev.next = f
}

func (h *replayHandle) addInfo(f *infoFile) {
	var prev *infoFile
	curr := h.infos
	for curr != nil {
		if curr.version > f.version {
			prev = curr
			curr = curr.next
		} else if curr.version < f.version {
			f.next = curr
			if prev == nil {
				h.infos = f
			} else {
				prev.next = f
			}
			return
		} else {
			panic(fmt.Sprintf("logic error: f.version=%d, curr.version=%d", f.version, curr.version))
		}
	}

	if prev == nil {
		h.infos = f
	} else {
		prev.next = f
	}
}

func (h *replayHandle) addBlock(id common.ID, name string, transient bool) {
	tbl, ok := h.files[id.TableID]
	if !ok {
		tbl = &tableDataFiles{
			unsortedfiles: make(map[common.ID]*unsortedSegmentFile),
			sortedfiles:   make(map[common.ID]*sortedSegmentFile),
		}
		h.files[id.TableID] = tbl
	}
	segId := id.AsSegmentID()
	file, ok := tbl.unsortedfiles[segId]
	if !ok {
		tbl.unsortedfiles[segId] = newUnsortedSegmentFile(segId, h)
		file = tbl.unsortedfiles[segId]
	}
	file.addBlock(id, name, transient)
}

func (h *replayHandle) addSegment(id common.ID, name string) {
	tbl, ok := h.files[id.TableID]
	if !ok {
		tbl = &tableDataFiles{
			unsortedfiles: make(map[common.ID]*unsortedSegmentFile),
			sortedfiles:   make(map[common.ID]*sortedSegmentFile),
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

func (h *replayHandle) addDataFile(fname string) {
	if name, ok := e.ParseTBlockfileName(fname); ok {
		id, err := common.ParseTBlockfileName(name)
		if err != nil {
			panic(err)
		}
		fullname := path.Join(h.dataDir, fname)
		h.addBlock(id, fullname, true)
		return
	}
	if name, ok := e.ParseBlockfileName(fname); ok {
		id, err := common.ParseBlockFileName(name)
		if err != nil {
			panic(err)
		}
		fullname := path.Join(h.dataDir, fname)
		h.addBlock(id, fullname, false)
		return
	}
	if name, ok := e.ParseSegmentfileName(fname); ok {
		id, err := common.ParseSegmentFileName(name)
		if err != nil {
			panic(err)
		}
		fullname := path.Join(h.dataDir, fname)
		h.addSegment(id, fullname)
		return
	}
	h.others = append(h.others, path.Join(h.dataDir, fname))
}

func (h *replayHandle) addMetaFile(fname string) {
	name, ok := e.ParseTableMetaName(fname)
	if ok {
		tid, version, err := md.ParseTableCkpFile(name)
		if err != nil {
			panic(fmt.Sprintf("parse table ckp file %s err: %s", fname, err))
		}
		f := new(tableFile)
		f.name = path.Join(h.metaDir, fname)
		f.version = version
		f.id = tid
		h.addTable(f)
		return
	}

	version, ok := e.ParseInfoMetaName(fname)
	if ok {
		f := new(infoFile)
		f.name = path.Join(h.metaDir, fname)
		f.version = uint64(version)
		h.addInfo(f)
		return
	}
	h.others = append(h.others, path.Join(h.metaDir, fname))
}

func (h *replayHandle) correctTable(meta *md.Table) {
	tablesFiles, ok := h.files[meta.ID]
	if !ok {
		return
	}
	if meta.IsDeleted(md.NowMicro()) {
		h.addCleanable(tablesFiles)
		return
	}
	for i := len(meta.Segments) - 1; i >= 0; i-- {
		segment := meta.Segments[i]
		if segment.DataState == md.SORTED {
			break
		}
		file := tablesFiles.sortedfiles[*segment.AsCommonID()]
		if file != nil {
			segment.TrySorted()
			continue
		}
	}
	for id, _ := range tablesFiles.sortedfiles {
		unsorted, ok := tablesFiles.unsortedfiles[id]
		if ok {
			h.addCleanable(unsorted)
			delete(tablesFiles.unsortedfiles, id)
		}
	}

	flushsegs := make([]common.ID, 0)
	unclosedSegFiles := make([]*unsortedSegmentFile, 0)
	for id, unsorted := range tablesFiles.unsortedfiles {
		segMeta, err := meta.ReferenceSegment(id.SegmentID)
		if err != nil {
			h.addCleanable(unsorted)
			continue
		} else {
			// log.Info(segMeta.String())
			unsorted.tryCleanBlocks(h, segMeta)
		}
		if !unsorted.isfull(int(meta.Conf.SegmentMaxBlocks)) {
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
	emeta := bf.meta.Segment.GetActiveBlk()
	if emeta != bf.meta {
		h.addCleanable(bf)
		return
	}
	bf.meta.DataState = md.PARTIAL
	bf.meta.Segment.NextActiveBlk()

	files := file.uncommited[1:]
	// TODO: uncommited block file can be converted to committed
	for _, f := range files {
		f.meta.DataState = md.EMPTY
		h.addCleanable(f)
	}
}

func (h *replayHandle) processUnclosedSegmentFiles(files []*unsortedSegmentFile, meta *md.Table) {
	if len(files) == 0 {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].id.SegmentID < files[j].id.SegmentID
	})
	// for _, file := range files {
	// 	for _, f := range file.uncommited {
	// 	}
	// }
	i := meta.ActiveSegment
	for {
		if i >= len(meta.Segments) || len(files) == 0 {
			break
		}
		seg := meta.Segments[i]
		if seg.HasUncommitted() {
			file := files[0]
			if file.meta == seg {
				h.processUnclosedSegmentFile(file)
				files = files[1:]
			}
			for _, file := range files {
				h.addCleanable(file)
			}
			break
		} else {
			file := files[0]
			if file.meta == seg {
				files = files[1:]
			}
		}
		i++
	}
}

func (h *replayHandle) rebuildTable(tbl *md.Table) *md.Table {
	head := h.tables[tbl.ID]
	if head == nil {
		return nil
	}
	h.mask.Add(tbl.ID)
	r := head.Open()
	ret := &md.Table{}
	if _, err := ret.ReadFrom(r); err != nil {
		panic(err)
	}
	ret.Info = tbl.Info
	ret.Replay()
	h.correctTable(ret)
	// log.Info(ret.String())
	logutil.Infof(h.String())
	return ret
}

func (h *replayHandle) RebuildInfo(mu *sync.RWMutex, cfg *md.Configuration) *md.MetaInfo {
	info := md.NewMetaInfo(mu, cfg)
	if h.infos == nil {
		return info
	}

	r, err := os.OpenFile(h.infos.name, os.O_RDONLY, 06666)
	if err != nil {
		panic(err)
	}
	defer r.Close()
	if _, err = info.ReadFrom(r); err != nil {
		panic(err)
	}
	ts := md.NowMicro()
	tbls := make(map[uint64]*md.Table)
	for idx, tbl := range info.Tables {
		tbl.Info = info
		if tbl.IsDeleted(ts) {
			continue
		}
		newTbl := h.rebuildTable(tbl)
		if newTbl == nil {
			continue
		}
		tbls[idx] = newTbl
	}
	info.Tables = tbls

	return info
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
	for tid, head := range h.tables {
		if !h.mask.Contains(tid) {
			h.cleanupFile(head.name)
		}
		next := head.next
		for next != nil {
			h.cleanupFile(next.name)
			next = next.next
		}
	}
	h.tables = nil

	if h.infos != nil {
		next := h.infos.next
		for next != nil {
			h.cleanupFile(next.name)
			next = next.next
		}
		h.infos = nil
	}
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

func (h *replayHandle) CleanupWithCtx(maxVer int) {
	if maxVer <= 1 {
		panic("logic error")
	}
	for _, head := range h.tables {
		depth := 0
		next := head
		for next != nil {
			if depth >= maxVer {
				h.cleanupFile(next.name)
			}
			next = next.next
			depth++
		}
	}
	if h.infos != nil {
		depth := 0
		next := h.infos
		for next != nil {
			if depth >= maxVer {
				h.cleanupFile(next.name)
			}
			depth++
			next = next.next
		}
	}
}

func (h *replayHandle) String() string {
	s := fmt.Sprintf("[InfoFiles]:")
	{
		curr := h.infos
		for curr != nil {
			s = fmt.Sprintf("%s\n%s", s, curr.name)
			curr = curr.next
		}
	}
	s = fmt.Sprintf("%s\n[TableFiles]: %d", s, len(h.tables))
	for _, fs := range h.tables {
		curr := fs
		for curr != nil {
			s = fmt.Sprintf("%s\n%s", s, curr.name)
			curr = curr.next
		}
	}
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
