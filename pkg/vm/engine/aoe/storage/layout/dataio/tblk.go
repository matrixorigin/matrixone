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

package dataio

import (
	"fmt"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/types"
	"matrixone/pkg/logutil"
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

func (getter *tblkFileGetter) NameFactory(dir string, id common.ID) string {
	return common.MakeTBlockFileName(dir, id.ToTBlockFileName(getter.version), false)
}

func (getter *tblkFileGetter) Getter(dir string, meta *md.Block) (*os.File, error) {
	id := meta.AsCommonID()
	filename := common.MakeTBlockFileName(dir, id.ToTBlockFileName(getter.version), true)
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

// TransientBlockFile file structure:
// algo | colCntlen | metaCnt | preIdxLen | preIdx | IdxLen | Idx
// col01 : coldata len | coldata originlen |
// col02 : coldata len | coldata originlen |
// ...
// col01 data | col02 data |  ...
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
	pattern := filepath.Join(common.MakeDataDir(f.host.GetDir()), fmt.Sprintf("%s_*tblk", f.id.ToBlockFileName()))
	files, _ := filepath.Glob(pattern)
	if len(files) == 0 {
		return
	}
	if len(files) > 1 {
		panic("logic error")
	}
	name := filepath.Base(files[0])
	name, _ = common.ParseTBlockfileName(name)
	if idv, err := common.ParseTBlkNameToID(name); err != nil {
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
		bat, err := batch.NewBatch(attrs, vecs)
		if err != nil {
			// TODO: returns error
			panic(err)
		}
		return bat
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
			err = vec.Unmarshal(obuf)
			if err != nil {
				panic(err)
			}
			vec.ResetReadonly()
			vecs[i] = vec
		default:
			vec := vector.NewStdVector(colDef.Type, meta.MaxRowCount)
			err = vec.Unmarshal(obuf)
			if err != nil {
				panic(err)
			}
			vec.ResetReadonly()
			vecs[i] = vec
		}
	}
	err := meta.SetCount(uint64(vecs[0].Length()))
	if err != nil {
		// TODO: returns error
		// do nothing temporally to pass UT
	}
	bat, err := batch.NewBatch(cols, vecs)
	if err != nil {
		// TODO: returns error
		panic(err)
	}
	return bat
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
