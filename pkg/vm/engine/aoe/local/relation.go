package local

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
	"strconv"
)

type localRoRelation struct {
	impl *db.Relation
}

func NewLocalRoRelation(impl *db.Relation) *localRoRelation {
	return &localRoRelation{
		impl: impl,
	}
}

func (r *localRoRelation) Segments() []engine.SegmentInfo {
	panic("not supported")
}

func (r *localRoRelation) ID() string {
	return r.impl.ID()
}

func (r *localRoRelation) Rows() int64 {
	return r.impl.Rows()
}

func (r *localRoRelation) Size(attr string) int64 {
	return r.impl.Size(attr)
}

func (r *localRoRelation) Close() {
	r.impl.Close()
}

func (r *localRoRelation) Index() []*engine.IndexTableDef {
	return r.impl.Index()
}

func (r *localRoRelation) Segment(segInfo engine.SegmentInfo, proc *process.Process) engine.Segment {
	id, err := strconv.ParseUint(segInfo.Id, 10, 64)
	if err != nil {
		return nil
	}
	return r.impl.Segment(id, proc)
}

func (r *localRoRelation) Attribute() []metadata.Attribute {
	return r.impl.Attribute()
}

func (r *localRoRelation) Write(_ uint64, _ *batch.Batch) error {
	panic("not supported")
}

func (r *localRoRelation) AddAttribute(_ uint64, _ engine.TableDef) error {
	panic("not supported")
}

func (r *localRoRelation) DelAttribute(_ uint64, _ engine.TableDef) error {
	panic("not supported")
}
