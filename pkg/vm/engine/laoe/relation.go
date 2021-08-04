package laoe

import (
	"encoding/binary"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func (r *relation) ID() string {
	return r.id
}

func (r *relation) Rows() int64 {
	return 0
}

func (r *relation) Size(_ string) int64 {
	return 0
}

func (r *relation) Segment(si engine.SegmentInfo, proc *process.Process) engine.Segment {
	return r.mp[si.TabletId].Segment(binary.BigEndian.Uint64([]byte(si.Id)), proc)
}

func (r *relation) Segments() []engine.SegmentInfo {
	return nil
}

func (r *relation) Index() []*engine.IndexTableDef {
	return nil
}

func (r *relation) Attribute() []metadata.Attribute {
	return nil
}

func (r *relation) Write(_ uint64, _ *batch.Batch) error {
	return nil
}

func (r *relation) AddAttribute(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) DelAttribute(_ uint64, _ engine.TableDef) error {
	return nil
}

func (r *relation) Close() {
	for _, v := range r.mp {
		v.Close()
	}
}
