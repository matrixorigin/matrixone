package db

import (
	"fmt"
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path"
)

type tableFile struct {
	name    string
	version uint64
	id      uint64
	next    *tableFile
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

type replayHandle struct {
	infos         *infoFile
	tables        map[uint64]*tableFile
	others        []string
	workDir       string
	metaDir       string
	tablesToClean map[uint64]*tableFile
	mask          *roaring.Bitmap
}

func NewReplayHandle(workDir string) *replayHandle {
	fs := &replayHandle{
		workDir: workDir,
		tables:  make(map[uint64]*tableFile),
		others:  make([]string, 0),
		mask:    roaring.NewBitmap(),
	}
	empty := false
	var err error
	dir := e.MakeMetaDir(workDir)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		empty = true
	}
	fs.metaDir = dir
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}
	if empty {
		return fs
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}
	for _, file := range files {
		fs.addFile(file.Name())
	}
	return fs
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

func (h *replayHandle) addFile(fname string) {
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
	h.others = append(h.others, fname)
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
	// log.Info(ret.String())
	return ret
}

func (h *replayHandle) RebuildInfo(cfg *md.Configuration) *md.MetaInfo {
	info := md.NewMetaInfo(cfg)
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
		tbls[idx] = h.rebuildTable(tbl)
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

	if h.infos != nil {
		next := h.infos.next
		for next != nil {
			h.cleanupFile(next.name)
			next = next.next
		}
	}
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
	s = fmt.Sprintf("%s\n[Others]: %d", s, len(h.others))
	for _, other := range h.others {
		s = fmt.Sprintf("%s\n%s", s, other)
	}
	return s
}
