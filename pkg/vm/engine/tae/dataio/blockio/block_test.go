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
	mobat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	ModuleName = "BlockIO"
)

func TestBlock1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	colCnt := 4
	indexCnt := make(map[int]int)
	schema := catalog.MockSchema(colCnt, 2)
	schema.BlockMaxRows = 1000
	for col := 0; col < colCnt; col++ {
		indexCnt[col] = 2
	}
	data := catalog.MockBatch(schema, int(schema.BlockMaxRows*2))
	newbat := mobat.New(true, data.Attrs)
	newbat.Vecs = containers.CopyToMoVectors(data.Vecs)
	var block file.Block
	id := common.NextGlobalSeqNum()
	SegmentFactory := NewObjectFactory(dir)
	seg := SegmentFactory.Build(dir, id, 0, SegmentFactory.(*ObjectFactory).Fs).(*segmentFile)
	block = newBlock(common.NextGlobalSeqNum(), seg, colCnt)
	var ts types.TS
	_, err := block.WriteBatch(data, ts)
	assert.Nil(t, err)
	err = block.Sync()
	assert.Nil(t, err)

	col, err := block.OpenColumn(3)
	assert.Nil(t, err)
	iovector, err := col.GetDataObject("").GetData()
	assert.Nil(t, err)
	buf := iovector.Entries[0].Data
	buf1, err := newbat.Vecs[3].Show()
	assert.Nil(t, err)
	assert.Equal(t, buf1, buf)

	block.Unref()
}
