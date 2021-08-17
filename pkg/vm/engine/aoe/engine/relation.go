package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
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
