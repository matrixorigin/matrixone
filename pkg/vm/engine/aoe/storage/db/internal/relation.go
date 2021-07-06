package internal

import (
	// log "github.com/sirupsen/logrus"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
	"strconv"
	"sync"
	"sync/atomic"
)

type Relation struct {
	TableData iface.ITableData
	tree      struct {
		sync.RWMutex
		Segments map[string]*Segment
	}
}

func NewRelation(data iface.ITableData) *Relation {
	r := &Relation{
		TableData: data,
	}
	r.tree.Segments = make(map[string]*Segment)
	return r
}

func (r *Relation) Rows() int64 {
	return 0
}

func (r *Relation) Size(attr string) int64 {
	return 0
}

func (r *Relation) ID() string {
	return r.TableData.GetName()
}

func (r *Relation) Close() error {
	if r.TableData != nil {
		for _, seg := range r.tree.Segments {
			seg.Data.Unref()
		}
		r.TableData.Unref()
		r.TableData = nil
	}
	return nil
}

func (r *Relation) Segments() []engine.SegmentInfo {
	ids := r.TableData.SegmentIds()
	infos := make([]engine.SegmentInfo, len(ids))
	for idx, id := range ids {
		infos[idx].Id = strconv.FormatUint(id, 10)
	}
	return infos
}

func (r *Relation) Index() []*engine.IndexTableDef {
	return nil
}

func (r *Relation) Attribute() []metadata.Attribute {
	meta := r.TableData.GetMeta()
	attrs := make([]metadata.Attribute, len(meta.Schema.ColDefs))
	for idx, attr := range attrs {
		attr.Name = meta.Schema.ColDefs[idx].Name
		attr.Type = meta.Schema.ColDefs[idx].Type
		attrs[idx] = attr
	}
	return attrs
}

func (r *Relation) Segment(info engine.SegmentInfo, proc *process.Process) engine.Segment {
	id, err := strconv.ParseUint(info.Id, 10, 64)
	if err != nil {
		return nil
	}
	r.tree.RLock()
	seg := r.tree.Segments[info.Id]
	if seg != nil {
		r.tree.RUnlock()
		return seg
	}
	r.tree.RUnlock()
	r.tree.Lock()
	seg = r.tree.Segments[info.Id]
	if seg != nil {
		r.tree.Unlock()
		return seg
	}
	seg = &Segment{
		Ids:  new(atomic.Value),
		Data: r.TableData.StrongRefSegment(id),
	}
	r.tree.Segments[info.Id] = seg
	r.tree.Unlock()
	return seg
}

// func (r *Relation) Write(bat *batch.Batch, index *md.LogIndex) error {
func (r *Relation) Write(bat *batch.Batch) error {
	return nil
}

func (r *Relation) AddAttribute(_ engine.TableDef) error {
	return nil
}

func (r *Relation) DelAttribute(_ engine.TableDef) error {
	return nil
}
