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

package tpEngine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewTpRelation(n string, id uint64, cinfo string, m tpMetadata, kv driver.CubeDriver, proc *process.Process) *tpRelation {
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
