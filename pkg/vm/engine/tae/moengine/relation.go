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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Relation = (*txnRelation)(nil)
)

func newRelation(h handle.Relation) *txnRelation {
	return &txnRelation{
		handle: h,
	}
}

func (rel *txnRelation) ID() string {
	return rel.handle.GetMeta().(*catalog.TableEntry).GetSchema().Name
}

func (rel *txnRelation) Close() {}

func (rel *txnRelation) Nodes() (nodes engine.Nodes) {
	return
}

func (_ *txnRelation) Size(_ string) int64 {
	return 0
}

func (_ *txnRelation) CardinalNumber(_ string) int64 {
	return 0
}

func (_ *txnRelation) CreateIndex(_ uint64, _ []engine.TableDef) error {
	panic("implement me")
}

func (_ *txnRelation) DropIndex(_ uint64, _ string) error {
	panic("implement me")
}

func (_ *txnRelation) AddTableDef(u uint64, def engine.TableDef) error {
	panic("implement me")
}

func (_ *txnRelation) DelTableDef(u uint64, def engine.TableDef) error {
	panic("implement me")
}

func (rel *txnRelation) TableDefs() []engine.TableDef {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	info := SchemaToTableInfo(schema)
	_, _, _, _, defs, _ := helper.UnTransfer(info)
	return defs
}

func (rel *txnRelation) Rows() int64 {
	return rel.handle.Rows()
}

func (_ *txnRelation) Index() []*engine.IndexTableDef {
	panic("implement me")
}

func (rel *txnRelation) GetPriKeyOrHideKey() ([]engine.Attribute, bool) {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	attrs := make([]engine.Attribute, 1)
	attrs[0].Name = schema.ColDefs[schema.PrimaryKey].Name
	attrs[0].Type = schema.ColDefs[schema.PrimaryKey].Type
	return attrs, true
}

func (rel *txnRelation) Attribute() []engine.Attribute {
	meta := rel.handle.GetMeta().(*catalog.TableEntry)
	attrs := make([]engine.Attribute, len(meta.GetSchema().ColDefs))
	for idx, attr := range attrs {
		attr.Name = meta.GetSchema().ColDefs[idx].Name
		attr.Type = meta.GetSchema().ColDefs[idx].Type
		attrs[idx] = attr
	}
	return attrs
}

func (rel *txnRelation) Write(_ uint64, bat *batch.Batch) error {
	return rel.handle.Append(bat)
}

func (rel *txnRelation) NewReader(num int, _ extend.Extend, _ []byte) (rds []engine.Reader) {
	it := rel.handle.MakeBlockIt()
	for i := 0; i < num; i++ {
		reader := newReader(rel.handle, it)
		rds = append(rds, reader)
	}
	return
}
