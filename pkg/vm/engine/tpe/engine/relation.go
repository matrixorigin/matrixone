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

package engine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
)

func (trel * TpeRelation) Rows() int64 {
	panic("implement me")
}

func (trel * TpeRelation) Size(s string) int64 {
	panic("implement me")
}

func (trel * TpeRelation) Close() {
}

func (trel * TpeRelation) ID() string {
	return trel.desc.Name
}

func (trel * TpeRelation) Nodes() engine.Nodes {
	var nds = []engine.Node{
		{
			Id:   "0",
			Addr: "localhost:20000",
		},
	}
	return nds
}

func (trel * TpeRelation) CreateIndex(epoch uint64, defs []engine.TableDef) error {
	panic("implement me")
}

func (trel * TpeRelation) DropIndex(epoch uint64, name string) error {
	panic("implement me")
}

func (trel * TpeRelation) TableDefs() []engine.TableDef {
	var defs []engine.TableDef
	var pkNames []string
	for _, attr := range trel.desc.Attributes {
		//skip hidden attribute ?
		if !attr.Is_hidden {
			if attr.Is_primarykey {
				pkNames = append(pkNames,attr.Name)
			}
			def := &engine.AttributeDef{Attr: engine.Attribute{
				Name:    attr.Name,
				Alg:     0,
				Type:    attr.TypesType,
				Default: attr.Default,
			}}
			defs = append(defs,def)
		}
	}

	if len(pkNames) != 0 {
		defs = append(defs,&engine.PrimaryIndexDef{
			Names:    pkNames,
		})
	}

	if len(trel.desc.Comment) != 0 {
		defs = append(defs,&engine.CommentDef{Comment: trel.desc.Comment})
	}
	return defs
}

func (trel * TpeRelation) Write(_ uint64, batch *batch.Batch) error {
	var attrDescs []descriptor.AttributeDesc
	for _, attr := range batch.Attrs {
		for i2, tta := range trel.desc.Attributes {
			if tta.Name == attr {
				attrDescs = append(attrDescs,trel.desc.Attributes[i2])
			}
		}
	}

	if len(attrDescs) != len(batch.Attrs) {
		return errorSomeAttributeNamesAreNotInAttributeDesc
	}

	err := trel.computeHandler.Write(trel.dbDesc, trel.desc, &trel.desc.Primary_index, attrDescs, batch)
	if err != nil {
		return err
	}
	return nil
}

func (trel * TpeRelation) AddTableDef(u uint64, def engine.TableDef) error {
	panic("implement me")
}

func (trel * TpeRelation) DelTableDef(u uint64, def engine.TableDef) error {
	panic("implement me")
}

func (trel * TpeRelation) NewReader(cnt int) []engine.Reader {
	var readers []engine.Reader = make([]engine.Reader,cnt)
	readers[0] = &TpeReader{
		dbDesc:         trel.dbDesc,
		tableDesc:      trel.desc,
		computeHandler: trel.computeHandler,
		prefix: nil,
		prefixLen: 0,
		isDumpReader: false,
	}
	for i := 1; i < cnt; i++ {
		readers[i] = &TpeReader{isDumpReader: true}
	}
	return readers
}