package tpEngine

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	dist2 "matrixone/pkg/vm/engine/dist"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func NewTpRelation(n string, id uint64, cinfo string, m tpMetadata, kv dist2.CubeDriver, proc *process.Process) *tpRelation {
	return &tpRelation{
		relName:    n,
		relId:      id,
		createInfo: cinfo,
		md:         m,
		kv:         kv,
		proc:       proc,
	}
}

func (tr *tpRelation) ID() string {
	return tr.relName
}

func (tr *tpRelation) Segments() []engine.SegmentInfo {
	tr.rwlock.RLock()
	defer tr.rwlock.RUnlock()
	segs := make([]engine.SegmentInfo, tr.md.Segs)
	for i := range segs {
		segs[i].Id = sKey(i, tr.relName)
	}
	return segs
}

func (tr *tpRelation) Index() []*engine.IndexTableDef {
	return nil
}

func (tr *tpRelation) Attribute() []metadata.Attribute {
	tr.rwlock.RLock()
	defer tr.rwlock.RUnlock()
	return tr.md.Attrs
}

func (tr *tpRelation) Segment(engine.SegmentInfo, *process.Process) engine.Segment {
	tr.rwlock.RLock()
	defer tr.rwlock.RUnlock()
	//return segment.New(si.Id, r.db, proc, r.md.Attrs)
	return nil
}

func (tr *tpRelation) Write(_ uint64, batch *batch.Batch) error {
	return nil
}

func (tr *tpRelation) AddTableDef(_ uint64, def engine.TableDef) error {
	return nil
}

func (tr *tpRelation) DelTableDef(_ uint64, def engine.TableDef) error {
	return nil
}
