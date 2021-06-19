package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	// log "github.com/sirupsen/logrus"
)

type EmbbedIndexFile struct {
	SegmentFile base.ISegmentFile
	Meta        *base.IndexMeta
}

type EmbbedBlockIndexFile struct {
	EmbbedIndexFile
	ID common.ID
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

func (bf *EmbbedBlockIndexFile) Read(buf []byte) (n int, err error) {
	if len(buf) != int(bf.Meta.Ptr.Len) {
		panic("logic error")
	}
	bf.SegmentFile.ReadBlockPoint(bf.ID, bf.Meta.Ptr, buf)
	return len(buf), nil
}
