package db

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/process"
	"strconv"
	"sync"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

type Relation struct {
	Data   iface.ITableData
	DBImpl *DB
	Meta   *md.Table
	tree   struct {
		sync.RWMutex
		Segments map[string]*Segment
	}
}

func NewRelation(impl *DB, data iface.ITableData, meta *md.Table) *Relation {
	r := &Relation{
		DBImpl: impl,
		Meta:   meta,
		Data:   data,
	}
	r.tree.Segments = make(map[string]*Segment)
	return r
}

func (r *Relation) Rows() int64 {
	return int64(r.Data.GetRowCount())
}

func (r *Relation) Size(attr string) int64 {
	return int64(r.Data.Size(attr))
}

func (r *Relation) ID() string {
	return r.Meta.Schema.Name
}

func (r *Relation) Close() error {
	r.tree.Lock()
	for _, seg := range r.tree.Segments {
		seg.Data.Unref()
	}
	r.tree.Unlock()
	r.Data.Unref()
	return nil
}

func (r *Relation) Segments() []engine.SegmentInfo {
	ids := r.Data.SegmentIds()
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
	meta := r.Data.GetMeta()
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
		Data: r.Data.StrongRefSegment(id),
	}
	r.tree.Segments[info.Id] = seg
	r.tree.Unlock()
	return seg
}

func (r *Relation) Write(bat *batch.Batch, index *md.LogIndex) error {
	return r.DBImpl.Append(r.Meta.Schema.Name, bat, index)
	// index := md.LogIndex{Capacity: uint64(bat.Vecs[0].Length())}
}

// func (r *Relation) AddAttribute(_ engine.TableDef) error {
// 	return nil
// }

// func (r *Relation) DelAttribute(_ engine.TableDef) error {
// 	return nil
// }
