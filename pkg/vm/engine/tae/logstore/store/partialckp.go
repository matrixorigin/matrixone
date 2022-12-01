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

package store

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
		panic(moerr.NewInternalErrorNoCtx("logic error %d != %d", info.size, o.size))
	}
	info.ckps.Or(o.ckps)
}

func (info *partialCkpInfo) MergeCommandInfos(cmds *entry.CommandInfo) {
	if info.size != cmds.Size {
		panic(moerr.NewInternalErrorNoCtx("logic error %d != %d", info.size, cmds.Size))
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
