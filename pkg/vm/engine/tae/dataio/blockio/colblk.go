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

package blockio

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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

func (cb *columnBlock) GetDataObject(metaLoc string) objectio.ColumnObject {
	object, err := cb.block.GetMeta(metaLoc).GetColumn(cb.id.Idx)
	if err != nil {
		panic(any(err))
	}
	return object
}

func (cb *columnBlock) Close() error {
	cb.Unref()
	return nil
}

func (cb *columnBlock) close() {
	cb.Destroy()
}

func (cb *columnBlock) Destroy() {
	logutil.Infof("Destroying Block %d Col @ TS %d", cb.block.id, cb.ts)
}
