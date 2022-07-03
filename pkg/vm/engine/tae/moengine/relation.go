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

package moengine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

var (
	_ engine.Relation = (*baseRelation)(nil)
)

const ADDR = "localhost:20000"

func (rel *baseRelation) ID(_ engine.Snapshot) string {
	return rel.handle.GetMeta().(*catalog.TableEntry).GetSchema().Name
}

func (rel *baseRelation) Close(_ engine.Snapshot) {}

func (rel *baseRelation) Nodes(_ engine.Snapshot) (nodes engine.Nodes) {
	return rel.nodes
}

func (_ *baseRelation) Size(_ string) int64 {
	return 0
}

func (_ *baseRelation) CardinalNumber(_ string) int64 {
	return 0
}

func (_ *baseRelation) CreateIndex(_ uint64, _ []engine.TableDef) error {
	panic(any("implement me"))
}

func (_ *baseRelation) DropIndex(_ uint64, _ string) error {
	panic(any("implement me"))
}

func (_ *baseRelation) AddTableDef(u uint64, def engine.TableDef, _ engine.Snapshot) error {
	panic(any("implement me"))
}

func (_ *baseRelation) DelTableDef(u uint64, def engine.TableDef, _ engine.Snapshot) error {
	panic(any("implement me"))
}

func (rel *baseRelation) TableDefs(_ engine.Snapshot) []engine.TableDef {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	defs, _ := SchemaToDefs(schema)
	return defs
}

func (rel *baseRelation) Rows() int64 {
	return rel.handle.Rows()
}

func (rel *baseRelation) Index() []*engine.IndexTableDef {
	panic(any("implement me"))
}

func (rel *baseRelation) GetPrimaryKeys(_ engine.Snapshot) (attrs []*engine.Attribute) {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	if !schema.HasPK() {
		return
	}
	for _, def := range schema.SortKey.Defs {
		attr := new(engine.Attribute)
		attr.Name = def.Name
		attr.Type = def.Type
		attrs = append(attrs, attr)
	}
	logutil.Debugf("GetPrimaryKeys: %v", attrs[0])
	return
}

func (rel *baseRelation) Truncate(_ engine.Snapshot) (uint64, error) {
	return 0, nil
}

func (rel *baseRelation) GetHideKey(_ engine.Snapshot) *engine.Attribute {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	key := new(engine.Attribute)
	key.Name = schema.HiddenKey.Name
	key.Type = schema.HiddenKey.Type
	logutil.Debugf("GetHideKey: %v", key)
	return key
}

func (rel *baseRelation) GetPriKeyOrHideKey(_ engine.Snapshot) ([]engine.Attribute, bool) {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	attrs := make([]engine.Attribute, 1)
	attrs[0].Name = schema.HiddenKey.Name
	attrs[0].Type = schema.HiddenKey.Type
	return attrs, false
}

func (rel *baseRelation) Attribute() []engine.Attribute {
	meta := rel.handle.GetMeta().(*catalog.TableEntry)
	attrs := make([]engine.Attribute, len(meta.GetSchema().ColDefs))
	for idx, attr := range attrs {
		attr.Name = meta.GetSchema().ColDefs[idx].Name
		attr.Type = meta.GetSchema().ColDefs[idx].Type
		attrs[idx] = attr
	}
	return attrs
}

func (rel *baseRelation) Write(_ uint64, bat *batch.Batch, _ engine.Snapshot) error {
	return nil
}

func (rel *baseRelation) Update(_ uint64, data *batch.Batch, _ engine.Snapshot) error {
	return nil
}

func (rel *baseRelation) Delete(_ uint64, data *vector.Vector, col string, _ engine.Snapshot) error {
	return nil
}

func (rel *baseRelation) NewReader(num int, _ extend.Extend, _ []byte, _ engine.Snapshot) (rds []engine.Reader) {
	it := rel.handle.MakeBlockIt()
	for i := 0; i < num; i++ {
		reader := newReader(rel.handle, it)
		rds = append(rds, reader)
	}
	return
}
