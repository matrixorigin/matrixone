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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestTpeRelation_Write(t *testing.T) {
	convey.Convey("table write without primary key", t, func() {
		tpe, err := NewTpeEngine(&TpeConfig{
			KvType:                    tuplecodec.KV_MEMORY,
			SerialType:                tuplecodec.ST_JSON,
			ValueLayoutSerializerType: "default",
			KVLimit:                   10000})
		convey.So(err, convey.ShouldBeNil)
		err = tpe.Create(0, "test", 0)
		convey.So(err, convey.ShouldBeNil)

		dbDesc, err := tpe.Database("test")
		convey.So(err, convey.ShouldBeNil)

		//(a,b,c)
		//(uint64,uint64,uint64)
		_, attrDefs := tuplecodec.MakeAttributes(types.T_uint64, types.T_uint64, types.T_uint64)

		attrNames := []string{
			"a", "b", "c",
		}
		var defs []engine.TableDef
		var rawDefs []*engine.AttributeDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs, def)
			rawDefs = append(rawDefs, def)
		}

		err = dbDesc.Create(0, "A", defs)
		convey.So(err, convey.ShouldBeNil)

		tableDesc, err := dbDesc.Relation("A")
		convey.So(err, convey.ShouldBeNil)

		//make data
		bat := tuplecodec.MakeBatch(10, attrNames, rawDefs)

		err = tableDesc.Write(0, bat)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("table write with primary key", t, func() {
		tpe, err := NewTpeEngine(&TpeConfig{
			KvType:                    tuplecodec.KV_MEMORY,
			SerialType:                tuplecodec.ST_JSON,
			ValueLayoutSerializerType: "default",
			KVLimit:                   10000})
		convey.So(err, convey.ShouldBeNil)
		err = tpe.Create(0, "test", 0)
		convey.So(err, convey.ShouldBeNil)

		dbDesc, err := tpe.Database("test")
		convey.So(err, convey.ShouldBeNil)

		//(a,b,c)
		//(uint64,uint64,uint64)
		//primary key (a,b)
		_, attrDefs := tuplecodec.MakeAttributes(types.T_uint64, types.T_uint64, types.T_uint64)

		attrNames := []string{
			"a", "b", "c",
		}
		var defs []engine.TableDef
		var rawDefs []*engine.AttributeDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs, def)
			rawDefs = append(rawDefs, def)
		}

		pkDef := &engine.PrimaryIndexDef{Names: []string{"a", "b"}}
		defs[0].(*engine.AttributeDef).Attr.Primary = true
		defs[1].(*engine.AttributeDef).Attr.Primary = true

		defs = append(defs, pkDef)

		err = dbDesc.Create(0, "A", defs)
		convey.So(err, convey.ShouldBeNil)

		tableDesc, err := dbDesc.Relation("A")
		convey.So(err, convey.ShouldBeNil)

		convey.So(tableDesc.ID(), convey.ShouldEqual, "A")

		convey.So(tableDesc.Nodes()[0].Addr, convey.ShouldEqual, "localhost:20000")
		convey.So(tableDesc.Nodes()[0].Id, convey.ShouldEqual, "0")

		readers := tableDesc.NewReader(10, nil, nil)
		dumpReaderCnt := 0
		for i := 0; i < 10; i++ {
			rd := readers[i].(*TpeReader)
			if rd.isDumpReader {
				dumpReaderCnt++
			}
		}
		convey.So(dumpReaderCnt, convey.ShouldEqual, 9)

		getDefs := tableDesc.TableDefs()
		for _, get := range getDefs {
			if x, ok := get.(*engine.AttributeDef); ok {
				found := false
				for _, attrDef := range attrDefs {
					if x.Attr.Name == attrDef.Attr.Name {
						found = true
						convey.So(reflect.DeepEqual(*x, *attrDef), convey.ShouldBeTrue)
					}
				}
				convey.So(found, convey.ShouldBeTrue)
			} else if y, ok := get.(*engine.PrimaryIndexDef); ok {
				convey.So(y.Names, convey.ShouldResemble, pkDef.Names)
			}
		}

		//make data
		bat := tuplecodec.MakeBatch(10, attrNames, rawDefs)

		vec0 := bat.Vecs[0].Col.([]uint64)
		vec1 := bat.Vecs[1].Col.([]uint64)

		for i := 0; i < 10; i++ {
			vec0[i] = uint64(i)
			vec1[i] = uint64(i)
		}
		err = tableDesc.Write(0, bat)
		convey.So(err, convey.ShouldBeNil)
	})
}
