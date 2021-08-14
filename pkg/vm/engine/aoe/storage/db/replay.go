package db

import (
	"fmt"
	"io/ioutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path"
	"sort"
	"sync"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	log "github.com/sirupsen/logrus"
)

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

type sortedSegmentFile struct {
	name string
	id   common.ID
}

type unsortedSegmentFile struct {
	id     common.ID
	names  []string
	blkids []common.ID
}

func newUnsortedSegmentFile(id common.ID) *unsortedSegmentFile {
	return &unsortedSegmentFile{
		id:     id,
		names:  make([]string, 0),
		blkids: make([]common.ID, 0),
	}
}

func (sf *sortedSegmentFile) clean() {
	os.Remove(sf.name)
	log.Infof("%s | Removed", sf.name)
}

func (usf *unsortedSegmentFile) addBlock(bid common.ID, name string) {
	usf.names = append(usf.names, name)
	usf.blkids = append(usf.blkids, bid)
}

func (usf *unsortedSegmentFile) isfull(maxcnt int) bool {
	return len(usf.names) == maxcnt
}

func (usf *unsortedSegmentFile) clean() {
	for _, name := range usf.names {
		os.Remove(name)
		log.Infof("%s | Removed", name)
	}
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
}

func NewReplayHandle(workDir string) *replayHandle {
	fs := &replayHandle{
		workDir:    workDir,
		tables:     make(map[uint64]*tableFile),
		others:     make([]string, 0),
		mask:       roaring.NewBitmap(),
		files:      make(map[uint64]*tableDataFiles),
		cleanables: make([]cleanable, 0),
		flushsegs:  make([]flushsegCtx, 0),
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

func (h *replayHandle) DispatchEvents(opts *e.Options, tables *table.Tables) {
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

func (h *replayHandle) addBlock(id common.ID, name string) {
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
		tbl.unsortedfiles[segId] = newUnsortedSegmentFile(segId)
		file = tbl.unsortedfiles[segId]
	}
	file.addBlock(id, name)
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
		id:   id,
		name: name,
	}
}

func (h *replayHandle) addDataFile(fname string) {
	if name, ok := e.ParseBlockfileName(fname); ok {
		id, err := common.ParseBlockFileName(name)
		if err != nil {
			panic(err)
		}
		fullname := path.Join(h.dataDir, fname)
		h.addBlock(id, fullname)
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
		file2 := tablesFiles.unsortedfiles[*segment.AsCommonID()]
		if file2 != nil {
		}
	}
	for id, _ := range tablesFiles.sortedfiles {
		unsorted, ok := tablesFiles.unsortedfiles[id]
		if ok {
			h.addCleanable(unsorted)
			delete(tablesFiles.unsortedfiles, id)
		}
	}
	segids := make([]common.ID, 0)
	for id, unsorted := range tablesFiles.unsortedfiles {
		if !unsorted.isfull(int(meta.Conf.SegmentMaxBlocks)) {
			continue
		}
		segids = append(segids, id)
	}
	sort.Slice(segids, func(i, j int) bool { return segids[i].SegmentID < segids[j].SegmentID })
	for _, id := range segids {
		h.flushsegs = append(h.flushsegs, flushsegCtx{id: id})
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
	if err := ret.ReadFrom(r); err != nil {
		panic(err)
	}
	ret.Info = tbl.Info
	ret.Replay()
	h.correctTable(ret)
	// log.Info(ret.String())
	log.Info(h.String())
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
	if err = info.ReadFrom(r); err != nil {
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

func (h *replayHandle) cleanupFile(fname string) {
	os.Remove(fname)
	log.Infof("%s | Removed", fname)
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
