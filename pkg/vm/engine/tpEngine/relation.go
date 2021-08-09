package tpEngine

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func NewTpRelation(n string, id uint64, cinfo string,m tpMetadata, kv dist.Storage, proc *process.Process)*tpRelation {
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

func (tr *tpRelation) Write(batch *batch.Batch) error{
	return nil
}

func (tr *tpRelation) AddAttribute(def engine.TableDef) error{
	return nil
}

func (tr *tpRelation) DelAttribute(def engine.TableDef) error{
	return nil
}