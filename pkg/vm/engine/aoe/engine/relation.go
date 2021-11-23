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

package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe"

	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	//"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe/protocol"
	"time"
)

//Close closes the relation. It closes all relations of the tablet in the aoe store.
func (r *relation) Close() {
	for _, v := range r.mp {
		v.Close()
	}
}

//ID returns the name of the table.
func (r *relation) ID() string {
	return r.tbl.Name
}

//Segment returns the segment according to the segmentInfo.
func (r *relation) Segment(si SegmentInfo) aoe.Segment {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	return r.mp[si.TabletId].Segment(binary.BigEndian.Uint64([]byte(si.Id)))
}

//Segments returns all the SegmentIfo in the relation.
func (r *relation) Segments() []SegmentInfo {
	return r.segments
}

//Index returns all the indexes of the table.
func (r *relation) Index() []*engine.IndexTableDef {
	return helper.Index(*r.tbl)
}

//Attribute returns all the attributes of the table.
func (r *relation) Attribute() []engine.Attribute {
	return helper.Attribute(*r.tbl)
}

//Attribute writes the batch into the table.
func (r *relation) Write(_ uint64, bat *batch.Batch) error {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0).Milliseconds())
	}()
	if len(r.tablets) == 0 {
		return errors.New("no tablets exists")
	}
	targetTbl := r.tablets[rand.Intn(len(r.tablets))]
	var buf bytes.Buffer
	if err := protocol.EncodeBatch(bat, &buf); err != nil {
		return err
	}
	if buf.Len() == 0 {
		return errors.New("empty batch")
	}
	return r.catalog.Driver.Append(targetTbl.Name, targetTbl.ShardId, buf.Bytes())
}

func (r *relation) AddAttribute(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) DelAttribute(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) Rows() int64 {
	return 0
}

func (r *relation) Size(_ string) int64 {
	return 0
}

func (r *relation) Nodes() engine.Nodes {
	return r.nodes
}

func (r *relation) TableDefs() []engine.TableDef {
	_, _, _, _, defs, _ := helper.UnTransfer(*r.tbl)
	return defs
}

func (r *relation) AddTableDef(u uint64, def engine.TableDef) error {
	return nil
}

func (r *relation) DelTableDef(u uint64, def engine.TableDef) error {
	return nil
}

func (r *relation) NewReader(num int) []engine.Reader {
	if len(r.segments) == 0 {
		return nil
	}
	blockNum := 0
	blocks := make([]aoe.Block, 0)
	for _, sid := range r.segments {
		segment := r.Segment(sid)
		ids := segment.Blocks()
		blockNum += len(ids)
		for _, id := range ids {
			blocks = append(blocks, segment.Block(id))
		}
	}
	readers := make([]engine.Reader, num)
	mod := blockNum / num
	if mod == 0 {
		mod = 1
	}
	var i int
	for i = 0; i < num; i++ {
		if i == num-1 || i == blockNum-1 {
			readers[i] = &aoeReader{
				blocks: blocks[i*mod:],
			}
			break
		}
		readers[i] = &aoeReader{
			blocks: blocks[i*mod : (i+1)*mod],
		}
	}
	if len(readers) < num {
		n := num - len(readers)
		for j := 0; j < n; j++ {
			readers[i+j] = &aoeReader{blocks: nil}
		}
	}
	return readers
}
