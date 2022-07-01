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

package local

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"

	//"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type localRoRelation struct {
	impl *aoedb.Relation
}

func (r *localRoRelation) Nodes(_ engine.Snapshot) engine.Nodes {
	panic("implement me")
}

func (r *localRoRelation) TableDefs(_ engine.Snapshot) []engine.TableDef {
	panic("implement me")
}

func (r *localRoRelation) AddTableDef(u uint64, def engine.TableDef, _ engine.Snapshot) error {
	panic("implement me")
}

func (r *localRoRelation) DelTableDef(u uint64, def engine.TableDef, _ engine.Snapshot) error {
	panic("implement me")
}

func (r *localRoRelation) NewReader(i int, _ extend.Extend, _ []byte, _ engine.Snapshot) []engine.Reader {
	panic("implement me")
}

func NewLocalRoRelation(impl *aoedb.Relation) *localRoRelation {
	return &localRoRelation{
		impl: impl,
	}
}

func (r *localRoRelation) Segments() []aoe.Segment {
	panic("not supported")
}

func (r *localRoRelation) ID(_ engine.Snapshot) string {
	return r.impl.ID()
}

func (r *localRoRelation) Rows() int64 {
	return r.impl.Rows()
}

func (r *localRoRelation) Size(attr string) int64 {
	return r.impl.Size(attr)
}

func (r *localRoRelation) Close(_ engine.Snapshot) {
	r.impl.Close()
}

func (r *localRoRelation) GetPrimaryKeys(_ engine.Snapshot) []*engine.Attribute {
	panic(any("implement me"))
}

func (r *localRoRelation) Truncate(_ engine.Snapshot) (uint64, error) {
	panic(any("implement me"))
}

func (r *localRoRelation) GetHideKey(_ engine.Snapshot) *engine.Attribute {
	panic(any("implement me"))
}

func (r *localRoRelation) GetPriKeyOrHideKey(_ engine.Snapshot) ([]engine.Attribute, bool) {
	return nil, false
}

func (r *localRoRelation) Index() []*engine.IndexTableDef {
	return r.impl.Index()
}
func (r *localRoRelation) CreateIndex(_ uint64, _ []engine.TableDef) error {
	panic("not supported")
}
func (r *localRoRelation) DropIndex(epoch uint64, name string) error {
	panic("not supported")
}
func (r *localRoRelation) Segment(segInfo aoe.Segment, proc *process.Process) aoe.Segment {
	id, err := strconv.ParseUint(segInfo.ID(), 10, 64)
	if err != nil {
		return nil
	}
	return r.impl.Segment(id)
}

func (r *localRoRelation) Attribute() []engine.Attribute {
	return r.impl.Attribute()
}

func (r *localRoRelation) Write(_ uint64, _ *batch.Batch, _ engine.Snapshot) error {
	panic("not supported")
}

func (r *localRoRelation) Update(_ uint64, bat *batch.Batch, _ engine.Snapshot) error {
	panic(any("implement me"))
}

func (r *localRoRelation) Delete(_ uint64, _ *vector.Vector, _ string, _ engine.Snapshot) error {
	panic(any("implement me"))
}

func (r *localRoRelation) AddAttribute(_ uint64, _ engine.TableDef) error {
	panic("not supported")
}

func (r *localRoRelation) DelAttribute(_ uint64, _ engine.TableDef) error {
	panic("not supported")
}
