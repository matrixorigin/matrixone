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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
func (r *relation) Segment(si engine.SegmentInfo, proc *process.Process) engine.Segment {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	return r.mp[si.TabletId].Segment(binary.BigEndian.Uint64([]byte(si.Id)), proc)
}

//Segments returns all the SegmentIfo in the relation.
func (r *relation) Segments() []engine.SegmentInfo {
	return r.segments
}

//Index returns all the indexes of the table.
func (r *relation) Index() []*engine.IndexTableDef {
	return helper.Index(*r.tbl)
}

//Attribute returns all the attributes of the table.
func (r *relation) Attribute() []metadata.Attribute {
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
func (r *relation) CreateIndex(epoch uint64, defs []engine.TableDef) error{
	idxInfo:= helper.IndexDefs(r.pid,r.tbl.Id,nil,defs)
	//TODO
	return r.catalog.CreateIndex(epoch,idxInfo[0])
}
func (r *relation) DropIndex(epoch uint64, name string) error{
	return r.catalog.DropIndex(epoch,r.tbl.Id,r.tbl.SchemaId,name)
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
