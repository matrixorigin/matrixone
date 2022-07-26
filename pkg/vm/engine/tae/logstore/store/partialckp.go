package store

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type partialCkpInfo struct {
	size uint32
	ckps *roaring.Bitmap
}

func newPartialCkpInfo(size uint32) *partialCkpInfo {
	return &partialCkpInfo{
		ckps: &roaring.Bitmap{},
		size: size,
	}
}

func (info *partialCkpInfo) String() string {
	return fmt.Sprintf("%s/%d", info.ckps.String(), info.size)
}

func (info *partialCkpInfo) IsAllCheckpointed() bool {
	return info.size == uint32(info.ckps.GetCardinality())
}

func (info *partialCkpInfo) MergePartialCkpInfo(o *partialCkpInfo) {
	if info.size != o.size {
		panic("logic error")
	}
	info.ckps.Or(o.ckps)
}

func (info *partialCkpInfo) MergeCommandInfos(cmds *entry.CommandInfo) {
	if info.size != cmds.Size {
		panic("logic error")
	}
	for _, csn := range cmds.CommandIds {
		info.ckps.Add(csn)
	}
}

func (info *partialCkpInfo) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, info.size); err != nil {
		return
	}
	n += 4
	ckpsn, err := info.ckps.WriteTo(w)
	n += ckpsn
	if err != nil {
		return
	}
	return
}

func (info *partialCkpInfo) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &info.size); err != nil {
		return
	}
	n += 4
	info.ckps = roaring.New()
	ckpsn, err := info.ckps.ReadFrom(r)
	n += ckpsn
	if err != nil {
		return
	}
	return
}
