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

package aoedb

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	md "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Relation is a high-level abstraction provided for
// the upper layer, contains the table data, schema, and
// the segments it refers to.
type Relation struct {
	Data   iface.ITableData
	DBImpl *DB
	Meta   *md.Table
	tree   struct {
		sync.RWMutex
		Segments map[uint64]*db.Segment
	}
}

func NewRelation(impl *DB, data iface.ITableData, meta *md.Table) *Relation {
	r := &Relation{
		DBImpl: impl,
		Meta:   meta,
		Data:   data,
	}
	r.tree.Segments = make(map[uint64]*db.Segment)
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

func (r *Relation) SegmentIds() dbi.IDS {
	return dbi.IDS{Ids: r.Data.SegmentIds()}
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

func (r *Relation) Segment(id uint64, proc *process.Process) engine.Segment {
	r.tree.RLock()
	seg := r.tree.Segments[id]
	if seg != nil {
		r.tree.RUnlock()
		return seg
	}
	r.tree.RUnlock()
	r.tree.Lock()
	seg = r.tree.Segments[id]
	if seg != nil {
		r.tree.Unlock()
		return seg
	}
	seg = &db.Segment{
		Ids:  new(atomic.Value),
		Data: r.Data.StrongRefSegment(id),
	}
	r.tree.Segments[id] = seg
	r.tree.Unlock()
	return seg
}

func (r *Relation) Write(ctx *AppendCtx) error {
	return r.DBImpl.Append(ctx)
}
