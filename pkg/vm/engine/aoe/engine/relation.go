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
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
	"time"
)

func (r *relation) Close() {
	for _, v := range r.mp {
		v.Close()
	}
}

func (r *relation) ID() string {
	return r.tbl.Name
}

func (r *relation) Segment(si engine.SegmentInfo, proc *process.Process) engine.Segment {
	t0 := time.Now()
	defer func() {
		logutil.Debugf("time cost %d ms", time.Since(t0))
	}()
	return r.mp[si.TabletId].Segment(binary.BigEndian.Uint64([]byte(si.Id)), proc)
}

func (r *relation) Segments() []engine.SegmentInfo {
	return r.segments
}

func (r *relation) Index() []*engine.IndexTableDef {
	return helper.Index(*r.tbl)
}

func (r *relation) Attribute() []metadata.Attribute {
	return helper.Attribute(*r.tbl)
}

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
