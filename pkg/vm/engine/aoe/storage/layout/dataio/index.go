package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	// log "github.com/sirupsen/logrus"
)

type EmbbedIndexFile struct {
	SegmentFile base.ISegmentFile
	Meta        *base.IndexMeta
	Info        *fileStat
}

type EmbbedBlockIndexFile struct {
	EmbbedIndexFile
	ID common.ID
}

func newEmbbedIndexFile(host base.ISegmentFile, meta *base.IndexMeta) common.IVFile {
	f := &EmbbedIndexFile{
		SegmentFile: host,
		Meta:        meta,
		Info: &fileStat{
			size: int64(meta.Ptr.Len),
		},
	}
	return f
}

func newEmbbedBlockIndexFile(id *common.ID, host base.ISegmentFile, meta *base.IndexMeta) common.IVFile {
	f := &EmbbedBlockIndexFile{
		EmbbedIndexFile: EmbbedIndexFile{
			SegmentFile: host,
			Meta:        meta,
			Info: &fileStat{
				size: int64(meta.Ptr.Len),
			},
		},
		ID: *id,
	}
	f.Ref()
	return f
}

func (f *EmbbedIndexFile) Stat() common.FileInfo {
	return f.Info
}

func (f *EmbbedIndexFile) Ref() {
	f.SegmentFile.Ref()
}

func (f *EmbbedIndexFile) Unref() {
	f.SegmentFile.Ref()
}

func (f *EmbbedIndexFile) Read(buf []byte) (n int, err error) {
	if len(buf) != int(f.Meta.Ptr.Len) {
		panic("logic error")
	}
	f.SegmentFile.ReadPoint(f.Meta.Ptr, buf)
	return len(buf), nil
}

func (bf *EmbbedBlockIndexFile) Stat() common.FileInfo {
	return bf.Info
}
func (bf *EmbbedBlockIndexFile) Ref() {
	bf.SegmentFile.RefBlock(bf.ID)
}

func (bf *EmbbedBlockIndexFile) Unref() {
	bf.SegmentFile.UnrefBlock(bf.ID)
}

func (bf *EmbbedBlockIndexFile) Read(buf []byte) (n int, err error) {
	if len(buf) != int(bf.Meta.Ptr.Len) {
		panic("logic error")
	}
	bf.SegmentFile.ReadBlockPoint(bf.ID, bf.Meta.Ptr, buf)
	return len(buf), nil
}
