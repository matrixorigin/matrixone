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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func (r *localRoRelation) CreateIndex(_ uint64, _ []engine.TableDef) error{
	panic("not supported")
}
func (r *localRoRelation) DropIndex(epoch uint64, name string) error{
	panic("not supported")
}