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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
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

func (rel *txnRelation) GetPrimaryKeys(_ engine.Snapshot) []*engine.Attribute {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	ColDef := schema.GetSinglePKColDef()
	if ColDef.IsPrimary() && ColDef.Name != catalog.HiddenColumnName {
		attrs := make([]*engine.Attribute, 1)
		attrs[0] = &engine.Attribute{
			Name: ColDef.Name,
			Type: ColDef.Type,
		}
		logutil.Infof("GetPrimaryKeys: %v", ColDef.Name)
		return attrs
	}
	return nil
}

func (rel *txnRelation) GetHideKey(_ engine.Snapshot) *engine.Attribute {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	ColDef := schema.HiddenKeyDef()
	if ColDef.IsHidden() && ColDef.Name == catalog.HiddenColumnName {
		logutil.Infof("GetHideKey: %v", ColDef.Name)
		return &engine.Attribute{
			Name: ColDef.Name,
			Type: ColDef.Type,
		}
	}
	return nil
}

func (rel *txnRelation) GetPriKeyOrHideKey(_ engine.Snapshot) ([]engine.Attribute, bool) {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	attrs := make([]engine.Attribute, 1)
	attrs[0].Name = schema.GetSinglePKColDef().Name
	attrs[0].Type = schema.GetSinglePKColDef().Type
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

func (rel *txnRelation) Write(_ uint64, bat *batch.Batch, _ engine.Snapshot) error {
	return rel.handle.Append(bat)
}

func (rel *txnRelation) Delete(_ uint64, vector *vector.Vector, col string, _ engine.Snapshot) error {
	schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	logutil.Debugf("Delete col: %v", col)
	if col != catalog.HiddenColumnName {
		ColDef := schema.GetSinglePKColDef()
		if ColDef.IsPrimary() && ColDef.Name == col {
			for i := 0; i < vector.Length; i++ {
				v := compute.GetValue(vector, uint32(i))
				filter := handle.NewEQFilter(v)
				id, row, err := rel.handle.GetByFilter(filter)
				if err != nil {
					return err
				}
				err = rel.handle.RangeDelete(id, row, row)
				if err != nil {
					return err
				}
			}
			return nil
		}
		panic(any("key not found"))
	}
	ColDef := schema.HiddenKeyDef()
	if ColDef.IsHidden() && ColDef.Name == catalog.HiddenColumnName && ColDef.Name == col {
		for i := 0; i < vector.Length; i++ {
			v := compute.GetValue(vector, uint32(i))
			if err := rel.handle.DeleteByHiddenKey(v); err != nil {
				return err
			}
		}
		return nil
	}
	panic(any("key not found"))
}

func (rel *txnRelation) NewReader(num int, _ extend.Extend, _ []byte, _ engine.Snapshot) (rds []engine.Reader) {
	it := rel.handle.MakeBlockIt()
	for i := 0; i < num; i++ {
		reader := newReader(rel.handle, it)
		rds = append(rds, reader)
	}
	return
}
