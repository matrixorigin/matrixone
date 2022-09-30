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
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type columnBlock struct {
	common.RefHelper
	block *blockFile
	id    *common.ID
}

func newColumnBlock(block *blockFile, col int) *columnBlock {
	cId := &common.ID{
		SegmentID: block.id.SegmentID,
		BlockID:   block.id.BlockID,
		Idx:       uint16(col),
	}
	cb := &columnBlock{
		block: block,
		id:    cId,
	}
	cb.OnZeroCB = cb.close
	cb.Ref()
	return cb
}

func (cb *columnBlock) GetDataObject(metaLoc string) objectio.ColumnObject {
	if cb.block.getMetaKey().End() > 0 {
		object, err := cb.block.GetMeta().GetColumn(cb.id.Idx)
		if err != nil {
			panic(any(err))
		}
		return object
	}

	// FIXME: Now the block that is gc will also be replayed, here is a work around
	block := cb.block.GetMetaFormKey(metaLoc)
	if block == nil {
		return nil
	}
	object, err := block.GetColumn(cb.id.Idx)
	if err != nil {
		panic(any(err))
	}
	return object
}

func (cb *columnBlock) GetData(
	metaLoc string,
	NullAbility bool,
	typ types.Type,
	_ *bytes.Buffer,
) (vec containers.Vector, err error) {
	var fsVector *fileservice.IOVector
	fsVector, err = cb.GetDataObject(metaLoc).GetData()
	if err != nil {
		return
	}

	srcBuf := fsVector.Entries[0].Data
	vector := vector.New(typ)
	vector.Read(srcBuf)
	vec = containers.MOToVectorTmp(vector, NullAbility)
	return
}

func (cb *columnBlock) Close() error {
	cb.Unref()
	return nil
}

func (cb *columnBlock) close() {
	cb.Destroy()
}

func (cb *columnBlock) Destroy() {
	logutil.Infof("Destroying Block %d Col", cb.block.id)
}
