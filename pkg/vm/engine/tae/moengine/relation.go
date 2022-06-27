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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var (
	_ engine.Relation = (*txnRelation)(nil)
)

const ADDR = "localhost:20000"

func newRelation(h handle.Relation) *txnRelation {
	r := &txnRelation{
		handle: h,
	}
	r.nodes = append(r.nodes, engine.Node{
		Addr: ADDR,
	})
	return r
}

func (rel *txnRelation) ID(_ engine.Snapshot) string {
	return rel.handle.GetMeta().(*catalog.TableEntry).GetSchema().Name
}

func (rel *txnRelation) Close(_ engine.Snapshot) {}

func (rel *txnRelation) Nodes(_ engine.Snapshot) (nodes engine.Nodes) {
	return rel.nodes
}

func (_ *txnRelation) Size(_ string) int64 {
	return 0
}

func (_ *txnRelation) CardinalNumber(_ string) int64 {
	return 0
}

func (_ *txnRelation) CreateIndex(_ uint64, _ []engine.TableDef) error {
	panic(any("implement me"))
}

func (_ *txnRelation) DropIndex(_ uint64, _ string) error {
	panic(any("implement me"))
}

func (_ *txnRelation) AddTableDef(u uint64, def engine.TableDef, _ engine.Snapshot) error {
	panic(any("implement me"))
}

func (_ *txnRelation) DelTableDef(u uint64, def engine.TableDef, _ engine.Snapshot) error {
	panic(any("implement me"))
}

func (rel *txnRelation) TableDefs(_ engine.Snapshot) []engine.TableDef {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	defs, _ := SchemaToDefs(schema)
	return defs
}

func (rel *txnRelation) Rows() int64 {
	return rel.handle.Rows()
}

func (_ *txnRelation) Index() []*engine.IndexTableDef {
	panic(any("implement me"))
}

func (rel *txnRelation) GetPrimaryKeys(_ engine.Snapshot) (attrs []*engine.Attribute) {
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

func (rel *txnRelation) GetHideKey(_ engine.Snapshot) *engine.Attribute {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	key := new(engine.Attribute)
	key.Name = schema.HiddenKey.Name
	key.Type = schema.HiddenKey.Type
	logutil.Debugf("GetHideKey: %v", key)
	return key
}

func (rel *txnRelation) GetPriKeyOrHideKey(_ engine.Snapshot) ([]engine.Attribute, bool) {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	attrs := make([]engine.Attribute, 1)
	attrs[0].Name = schema.HiddenKey.Name
	attrs[0].Type = schema.HiddenKey.Type
	return attrs, false
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

func (rel *txnRelation) Write(_ uint64, bat *batch.Batch, _ engine.Snapshot) error {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	allNullables := schema.AllNullables()
	taeBatch := containers.NewEmptyBatch()
	defer taeBatch.Close()
	for i, vec := range bat.Vecs {
		v := MOToVector(vec, allNullables[i])
		taeBatch.AddVector(bat.Attrs[i], v)
	}
	return rel.handle.Append(taeBatch)
}

func (rel *txnRelation) Update(_ uint64, data *batch.Batch, _ engine.Snapshot) error {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	allNullables := schema.AllNullables()
	bat := containers.NewEmptyBatch()
	defer bat.Close()
	for i, vec := range data.Vecs {
		idx := catalog.GetAttrIdx(schema.AllNames(), data.Attrs[i])
		v := MOToVector(vec, allNullables[idx])
		bat.AddVector(data.Attrs[i], v)
	}
	hiddenIdx := catalog.GetAttrIdx(data.Attrs, schema.HiddenKey.Name)
	for idx := 0; idx < bat.Vecs[hiddenIdx].Length(); idx++ {
		v := bat.Vecs[hiddenIdx].Get(idx)
		for i, attr := range bat.Attrs {
			if schema.HiddenKey.Name == attr {
				continue
			}
			colIdx := schema.GetColIdx(attr)
			err := rel.handle.UpdateByHiddenKey(v, colIdx, bat.Vecs[i].Get(idx))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (rel *txnRelation) Delete(_ uint64, data *vector.Vector, col string, _ engine.Snapshot) error {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	logutil.Debugf("Delete col: %v", col)
	allNullables := schema.AllNullables()
	idx := catalog.GetAttrIdx(schema.AllNames(), col)
	vec := MOToVector(data, allNullables[idx])
	defer vec.Close()
	if schema.HiddenKey.Name == col {
		return rel.handle.DeleteByHiddenKeys(vec)
	}
	if !schema.HasPK() || schema.IsCompoundSortKey() {
		panic(any("No valid primary key found"))
	}
	if schema.SortKey.Defs[0].Name == col {
		for i := 0; i < vec.Length(); i++ {
			filter := handle.NewEQFilter(vec.Get(i))
			err := rel.handle.DeleteByFilter(filter)
			if err != nil {
				return err
			}
		}
		return nil
	}
	panic(any("Key not found"))
}

func (rel *txnRelation) NewReader(num int, _ extend.Extend, _ []byte, _ engine.Snapshot) (rds []engine.Reader) {
	it := rel.handle.MakeBlockIt()
	for i := 0; i < num; i++ {
		reader := newReader(rel.handle, it)
		rds = append(rds, reader)
	}
	return
}
