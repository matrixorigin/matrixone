package dataio

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/vector"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type versionBlockFile struct {
	*BlockFile
	version uint32
}

func newVersionBlockFile(version uint32, host base.ISegmentFile, id common.ID) *versionBlockFile {
	getter := tblkFileGetter{version: version}
	vbf := &versionBlockFile{
		version:   version,
		BlockFile: NewBlockFile(host, id, getter.NameFactory),
	}
	return vbf
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

type transientBlockFile struct {
	common.RefHelper
	host   base.ISegmentFile
	id     common.ID
	maxver uint32
	files  []*versionBlockFile
	mu     sync.RWMutex
}

func NewTBlockFile(host base.ISegmentFile, id common.ID) *transientBlockFile {
	tblk := &transientBlockFile{
		id:   id,
		host: host,
	}
	tblk.files = make([]*versionBlockFile, 0)
	tblk.OnZeroCB = tblk.close
	tblk.Ref()
	return tblk
}

func (f *transientBlockFile) NextVersion() uint32 {
	return atomic.AddUint32(&f.maxver, uint32(1)) - 1
}

func (f *transientBlockFile) Write(data []*vector.Vector, meta *md.Block, dir string) error {
	writer := NewBlockWriter(data, meta, dir)
	version := f.NextVersion()
	getter := tblkFileGetter{version: version}
	writer.SetFileGetter(getter.Getter)
	writer.SetPreExecutor(func() {
		log.Infof(" %s | TransientBlock | Flushing", writer.GetFileName())
	})
	writer.SetPostExecutor(func() {
		log.Infof(" %s | TransientBlock | Flushed", writer.GetFileName())
	})
	if err := writer.Execute(); err != nil {
		return err
	}
	bf := newVersionBlockFile(version, f.host, f.id)
	f.commit(bf)
	return nil
}

func (f *transientBlockFile) commit(bf *versionBlockFile) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = append(f.files, bf)
}

func (f *transientBlockFile) CleanStales() {
	f.mu.Lock()
	if len(f.files) <= 1 {
		f.mu.Unlock()
		return
	}
	files := f.files[:len(f.files)-1]
	f.files = f.files[len(f.files)-2:]
	f.mu.Unlock()
	for _, file := range files {
		file.Close()
		file.Destory()
	}
}

func (f *transientBlockFile) close() {
	err := f.Close()
	if err != nil {
		panic(err)
	}
	f.Destory()
}

func (f *transientBlockFile) Close() error {
	for _, file := range f.files {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (f *transientBlockFile) GetIndicesMeta() *base.IndexMeta {
	return nil
}

func (f *transientBlockFile) ReadPoint(ptr *base.Pointer, buf []byte) {
	f.mu.RLock()
	file := f.files[len(f.files)-1]
	f.mu.RUnlock()
	file.ReadPoint(ptr, buf)
}

func (f *transientBlockFile) ReadPart(colIdx uint64, id common.ID, buf []byte) {
	f.mu.RLock()
	file := f.files[len(f.files)-1]
	f.mu.RUnlock()
	file.ReadPart(colIdx, id, buf)
}

func (f *transientBlockFile) PartSize(colIdx uint64, id common.ID, isOrigin bool) int64 {
	f.mu.RLock()
	file := f.files[len(f.files)-1]
	f.mu.RUnlock()
	return file.PartSize(colIdx, id, isOrigin)
}

func (f *transientBlockFile) DataCompressAlgo(common.ID) int {
	return compress.None
}

func (f *transientBlockFile) Destory() {
	for _, file := range f.files {
		name := file.Name()
		log.Infof(" %s | TransientBlockFile | Destorying", name)
		if err := os.Remove(name); err != nil {
			panic(err)
		}
	}
}

func (f *transientBlockFile) Stat() common.FileInfo {
	f.mu.RLock()
	file := f.files[len(f.files)-1]
	f.mu.RUnlock()
	return file.Stat()
}

func (f *transientBlockFile) MakeVirtualIndexFile(*base.IndexMeta) common.IVFile {
	return nil
}

func (f *transientBlockFile) GetDir() string {
	return f.host.GetDir()
}
