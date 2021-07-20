package engine

import (
	"github.com/fagongzi/util/format"
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
)

func (r *relation) ID() string {
	return string(format.UInt64ToString(r.tbl.Id))
}

func (r *relation) Segment(si engine.SegmentInfo, proc *process.Process) engine.Segment {
	tRelation, err := r.catalog.Store.Relation(si.TabletId)
	if err != nil {
		log.Errorf("Generate relation for tablet %s failed, %s", si.TabletId, err.Error())
		return nil
	}
	return tRelation.Segment(si, proc)
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

func (r *relation) Write(bat *batch.Batch) error {

	//TODO: Choose one tablet and write
	return nil
}

func (r *relation) AddAttribute(_ engine.TableDef) error {
	return nil
}

func (r *relation) DelAttribute(_ engine.TableDef) error {
	return nil
}

func (r *relation) Rows() int64 {
	count := int64(0)
	for _, t := range r.tablets{
		count += t.Rows()
	}
	return count
}

func (r *relation) Size(_ string) int64 {
	return 0
}

