package dataio

import (
	"fmt"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/types"
	"matrixone/pkg/logutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type versionBlockFile struct {
	common.RefHelper
	*BlockFile
	version uint32
}

func newVersionBlockFile(version uint32, host base.ISegmentFile, id common.ID) *versionBlockFile {
	getter := tblkFileGetter{version: version}
	vbf := &versionBlockFile{
		version:   version,
		BlockFile: NewBlockFile(host, id, getter.NameFactory),
	}
	vbf.OnZeroCB = vbf.close
	vbf.Ref()
	return vbf
}

func (f *versionBlockFile) close() {
	f.Close()
	f.Destory()
}

type tblkFileGetter struct {
	version uint32
}

func makeTransientBlockFileName(version uint32, id common.ID) string {
	return fmt.Sprintf("%s_%d", id.ToBlockFileName(), version)
}

func (getter *tblkFileGetter) NameFactory(dir string, id common.ID) string {
	return e.MakeTBlockFileName(dir, makeTransientBlockFileName(getter.version, id), false)
}

func (getter *tblkFileGetter) Getter(dir string, meta *md.Block) (*os.File, error) {
	id := meta.AsCommonID()
	filename := e.MakeTBlockFileName(dir, makeTransientBlockFileName(getter.version, *id), true)
	fdir := filepath.Dir(filename)
	if _, err := os.Stat(fdir); os.IsNotExist(err) {
		err = os.MkdirAll(fdir, 0755)
		if err != nil {
			return nil, err
		}
	}
	w, err := os.Create(filename)
	return w, err
}

type TransientBlockFile struct {
	common.RefHelper
	host    base.ISegmentFile
	id      common.ID
	maxver  uint32
	files   []*versionBlockFile
	currpos uint32
	mu      sync.RWMutex
}

func NewTBlockFile(host base.ISegmentFile, id common.ID) *TransientBlockFile {
	f := &TransientBlockFile{
		id:   id,
		host: host,
	}
	f.files = make([]*versionBlockFile, 0)
	f.init()
	f.Ref()
	f.OnZeroCB = f.close
	return f
}

func (f *TransientBlockFile) init() {
	pattern := filepath.Join(e.MakeDataDir(f.host.GetDir()), fmt.Sprintf("%s_*tblk", f.id.ToBlockFileName()))
	files, _ := filepath.Glob(pattern)
	if len(files) == 0 {
		return
	}
	if len(files) > 1 {
		panic("logic error")
	}
	name := filepath.Base(files[0])
	name, _ = e.ParseTBlockfileName(name)
	if idv, err := common.ParseTBlockfileName(name); err != nil {
		panic(err)
	} else {
		f.maxver = idv.PartID + 1
	}
	bf := newVersionBlockFile(f.maxver-1, f.host, f.id)
	f.commit(bf, uint32(bf.Count))
}

func (f *TransientBlockFile) close() {
	f.Close()
	f.Destory()
}

func (f *TransientBlockFile) nextVersion() uint32 {
	return atomic.AddUint32(&f.maxver, uint32(1)) - 1
}

func (f *TransientBlockFile) PreSync(pos uint32) bool {
	f.mu.RLock()
	if pos < f.currpos {
		panic(fmt.Sprintf("PreSync %d but lastpos is %d", pos, f.currpos))
	}
	ret := pos > f.currpos
	f.mu.RUnlock()
	return ret
}

func (f *TransientBlockFile) InitMeta(meta *md.Block) {
	if meta.DataState != md.PARTIAL {
		return
	}
	meta.Count = f.files[0].Count
	meta.PrevIndex = f.files[0].PrevIdx
	meta.Index = f.files[0].Idx
}

func (f *TransientBlockFile) LoadBatch(meta *md.Block) batch.IBatch {
	f.mu.RLock()
	if len(f.files) == 0 {
		f.mu.RUnlock()
		colcnt := len(meta.Segment.Table.Schema.ColDefs)
		attrs := make([]int, colcnt)
		vecs := make([]vector.IVector, colcnt)
		for i, colDef := range meta.Segment.Table.Schema.ColDefs {
			vec := vector.NewVector(colDef.Type, meta.Segment.Table.Conf.BlockMaxRows)
			vecs[i] = vec
			attrs[i] = i
		}
		return batch.NewBatch(attrs, vecs)
	}
	file := f.files[len(f.files)-1]
	file.Ref()
	f.mu.RUnlock()
	defer file.Unref()
	id := *meta.AsCommonID()
	colcnt := len(meta.Segment.Table.Schema.ColDefs)
	vecs := make([]vector.IVector, colcnt)
	cols := make([]int, colcnt)
	for i, colDef := range meta.Segment.Table.Schema.ColDefs {
		cols[i] = i
		sz := file.PartSize(uint64(i), id, false)
		osz := file.PartSize(uint64(i), id, true)
		node := common.GPool.Alloc(uint64(sz))
		defer common.GPool.Free(node)
		buf := node.Buf[:sz]
		file.ReadPart(uint64(i), id, buf)
		obuf := make([]byte, osz)
		_, err := compress.Decompress(buf, obuf, compress.Lz4)
		if err != nil {
			panic(err)
		}
		switch colDef.Type.Oid {
		case types.T_char, types.T_varchar, types.T_json:
			vec := vector.NewStrVector(colDef.Type, meta.MaxRowCount)
			err = vec.Unmarshall(obuf)
			if err != nil {
				panic(err)
			}
			vecs[i] = vec
		default:
			vec := vector.NewStdVector(colDef.Type, meta.MaxRowCount)
			err = vec.Unmarshall(obuf)
			if err != nil {
				panic(err)
			}
			vecs[i] = vec
		}
	}
	meta.SetCount(uint64(vecs[0].Length()))
	return batch.NewBatch(cols, vecs)
}

func (f *TransientBlockFile) Sync(data batch.IBatch, meta *md.Block) error {
	writer := NewIBatchWriter(data, meta, meta.Segment.Table.Conf.Dir)
	version := f.nextVersion()
	getter := tblkFileGetter{version: version}
	writer.SetFileGetter(getter.Getter)
	writer.SetPreExecutor(func() {
		logutil.Infof(" %s | TransientBlock | Flushing", writer.GetFileName())
	})
	writer.SetPostExecutor(func() {
		logutil.Infof(" %s | TransientBlock | Flushed", writer.GetFileName())
	})
	if err := writer.Execute(); err != nil {
		return err
	}
	bf := newVersionBlockFile(version, f.host, f.id)
	f.commit(bf, uint32(data.Length()))
	return nil
}

func (f *TransientBlockFile) commit(bf *versionBlockFile, pos uint32) {
	f.mu.Lock()
	f.files = append(f.files, bf)
	f.currpos = pos
	if len(f.files) <= 1 {
		f.mu.Unlock()
		return
	}
	files := f.files[:len(f.files)-1]
	f.files = f.files[len(f.files)-1:]
	f.mu.Unlock()
	for _, file := range files {
		file.Unref()
	}
}

func (f *TransientBlockFile) Close() error {
	return nil
}

func (f *TransientBlockFile) GetIndicesMeta() *base.IndexMeta {
	return nil
}

func (f *TransientBlockFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	f.mu.RLock()
	file := f.files[len(f.files)-1]
	file.Ref()
	f.mu.RUnlock()
	file.ReadPoint(ptr, buf)
	file.Unref()
}

func (f *TransientBlockFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	f.mu.RLock()
	file := f.files[len(f.files)-1]
	file.Ref()
	f.mu.RUnlock()
	file.ReadPart(colIdx, id, buf)
	file.Unref()
}

func (f *TransientBlockFile) PartSize(colIdx uint64, id common.ID, isOrigin bool) int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	file := f.files[len(f.files)-1]
	return file.PartSize(colIdx, id, isOrigin)
}

func (f *TransientBlockFile) DataCompressAlgo(common.ID) int {
	return compress.Lz4
}

func (f *TransientBlockFile) Destory() {
	for _, file := range f.files {
		file.Unref()
	}
}

func (f *TransientBlockFile) Stat() common.FileInfo {
	f.mu.RLock()
	file := f.files[len(f.files)-1]
	f.mu.RUnlock()
	return file.Stat()
}

func (f *TransientBlockFile) MakeVirtualIndexFile(*base.IndexMeta) common.IVFile {
	return nil
}

func (f *TransientBlockFile) GetDir() string {
	return f.host.GetDir()
}
