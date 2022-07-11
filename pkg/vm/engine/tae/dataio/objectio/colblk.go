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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"os"
)

type columnBlock struct {
	common.RefHelper
	block   *blockFile
	ts      uint64
	indexes int
	id      *common.ID
}

func newColumnBlock(block *blockFile, indexCnt int, col int) *columnBlock {
	cId := &common.ID{
		SegmentID: block.id.SegmentID,
		BlockID:   block.id.BlockID,
		Idx:       uint16(col),
	}
	cb := &columnBlock{
		block:   block,
		indexes: indexCnt,
		id:      cId,
	}
	cb.OnZeroCB = cb.close
	cb.Ref()
	return cb
}

func (cb *columnBlock) WriteTS(ts uint64) (err error) {
	cb.ts = ts
	return
}

func (cb *columnBlock) WriteData(buf []byte) (err error) {
	return cb.block.writer.WriteData(cb.ts, cb.id, buf)
}

func (cb *columnBlock) WriteUpdates(buf []byte) (err error) {
	return cb.block.writer.WriteUpdates(cb.ts, cb.id, buf)
}

func (cb *columnBlock) WriteIndex(idx int, buf []byte) (err error) {
	return cb.block.writer.WriteIndex(cb.id, idx, buf)
}

func (cb *columnBlock) ReadTS() uint64 { return cb.ts }

func (cb *columnBlock) ReadData(buf []byte) (err error) {
	return
}

func (cb *columnBlock) ReadUpdates(buf []byte) (err error) {
	return
}

func (cb *columnBlock) ReadIndex(idx int, buf []byte) (err error) {
	return
}

func (cb *columnBlock) GetDataFileStat() (stat common.FileInfo) {
	return nil
}

func (cb *columnBlock) OpenIndexFile(idx int) (vfile common.IRWFile, err error) {
	name := EncodeIndexName(cb.id, idx, nil)
	vfile, err = cb.block.seg.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return
	}
	vfile.Ref()
	return
}

func (cb *columnBlock) OpenUpdateFile() (vfile common.IRWFile, err error) {
	name := EncodeUpdateNameWithVersion(cb.id, cb.ts, nil)
	vfile, err = cb.block.seg.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return
	}
	vfile.Ref()
	return
}

func (cb *columnBlock) OpenDataFile() (vfile common.IRWFile, err error) {
	name := EncodeColBlkNameWithVersion(cb.id, cb.ts, nil)
	vfile, err = cb.block.seg.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return
	}
	vfile.Ref()
	return
}

func (cb *columnBlock) Close() error {
	cb.Unref()
	// cb.data.Unref()
	// cb.updates.Unref()
	// for _, index := range cb.indexes {
	// 	index.Unref()
	// }
	return nil
}

func (cb *columnBlock) close() {
	cb.Destroy()
}

func (cb *columnBlock) Destroy() {
	logutil.Infof("Destroying Block %d Col @ TS %d", cb.block.id, cb.ts)
}
