package engine

import (
	"github.com/fagongzi/util/format"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func (r *relation) ID() string {
	return string(format.Uint64ToBytes(r.id))
}

func (r *relation) Segment(si engine.SegmentInfo, proc *process.Process) engine.Segment {
	return nil
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

func (r *relation) Write(bat *batch.Batch) error {

	return nil
}

func (r *relation) AddAttribute(_ engine.TableDef) error {
	return nil
}

func (r *relation) DelAttribute(_ engine.TableDef) error {
	return nil
}