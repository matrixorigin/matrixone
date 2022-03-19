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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
	"github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

func TestTpeDatabase_Create(t *testing.T) {
	convey.Convey("create table with none primary key ",t, func() {
		tpe, _ := NewTpeEngine(&TpeConfig{KVLimit: 10000})

		dbName := "test"
		err := tpe.Create(0,dbName,0)
		convey.So(err,convey.ShouldBeNil)

		dbDesc, err := tpe.Database(dbName)
		convey.So(err,convey.ShouldBeNil)

		//(a,b,c)
		//(uint64,uint64,uint64)
		_,attrDefs := tuplecodec.MakeAttributes(types.T_uint64,types.T_uint64,types.T_uint64)


		attrNames := []string{
			"a","b","c",
		}
		var defs []engine.TableDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs,def)
		}

		defs = append(defs,&engine.CommentDef{
			Comment: "A(a,b,c)",
		})

		cnt := 10
		var tableNames []string
		for i := 0; i < cnt; i++ {
			tableName := fmt.Sprintf("A%d",i)
			tableNames = append(tableNames,tableName)

			err = dbDesc.Create(0, tableName, defs)
			convey.So(err,convey.ShouldBeNil)

			table, err := dbDesc.Relation(tableName)
			convey.So(err,convey.ShouldBeNil)

			checkTable := func(table engine.Relation) {
				tableDesc,ok := table.(*TpeRelation)
				convey.So(ok,convey.ShouldBeTrue)
				convey.So(tableDesc.id,convey.ShouldEqual,3+i)
				convey.So(tableDesc.desc.ID,convey.ShouldEqual,3+i)
				convey.So(tableDesc.desc.Name,convey.ShouldEqual,tableName)

				pkAttrs := tableDesc.desc.Primary_index.Attributes
				convey.So(len(pkAttrs),convey.ShouldEqual,1)
				pkAttr := pkAttrs[0]
				convey.So(pkAttr.ID,convey.ShouldEqual,0)
				convey.So(strings.HasPrefix(pkAttr.Name,"rowid"),convey.ShouldBeTrue)
				convey.So(pkAttr.Type,convey.ShouldEqual,orderedcodec.VALUE_TYPE_UINT64)

				convey.So(len(attrDefs)+1,convey.ShouldEqual,len(tableDesc.desc.Attributes))

				attrs :=tableDesc.desc.Attributes
				for i, attr := range attrs {
					if attr.Is_primarykey {
						continue
					}
					convey.So(attr.ID,convey.ShouldEqual,i)
					convey.So(attr.Ttype,convey.ShouldEqual,orderedcodec.VALUE_TYPE_UINT64)
					convey.So(attr.TypesType.Oid,convey.ShouldEqual,types.T_uint64)
				}
			}

			checkTable(table)
		}

		wantNames := dbDesc.Relations()
		for i := 0; i < cnt; i++ {
			convey.So(wantNames[i],convey.ShouldEqual,tableNames[i])
		}

		var restNames []string
		for i := 0; i < cnt; i++ {
			if i % 2 == 0 {
				err = dbDesc.Delete(0, tableNames[i])
				convey.So(err,convey.ShouldBeNil)
			}else{
				restNames = append(restNames,tableNames[i])
			}
		}
		wantNames = dbDesc.Relations()
		convey.So(wantNames,convey.ShouldResemble,restNames)

		//recreate the dropped table
		for i := 0; i < cnt; i++ {
			if i%2 == 0 {
				err = dbDesc.Create(0,tableNames[i],defs)
				convey.So(err, convey.ShouldBeNil)
			}
		}
	})

	convey.Convey("create table with primary key ",t, func() {
		tpe, _ := NewTpeEngine(&TpeConfig{KVLimit: 10000})

		dbName := "test"
		err := tpe.Create(0,dbName,0)
		convey.So(err,convey.ShouldBeNil)

		dbDesc, err := tpe.Database(dbName)
		convey.So(err,convey.ShouldBeNil)

		//(a,b,c,primary key(a,b))
		//(uint64,uint64,uint64)
		_,attrDefs := tuplecodec.MakeAttributes(types.T_uint64,types.T_uint64,types.T_uint64)
		attrNames := []string{
			"a","b","c",
		}
		var defs []engine.TableDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs,def)
		}

		pkDefs := []*engine.PrimaryIndexDef{
			{Names: []string{
				"a","b",
			}},
		}

		defs = append(defs,pkDefs[0])

		defs = append(defs,&engine.CommentDef{
			Comment: "A(a,b,c)",
		})

		cnt := 10
		var tableNames []string
		for i := 0; i < cnt; i++ {
			tableName := fmt.Sprintf("A%d",i)
			tableNames = append(tableNames,tableName)

			err = dbDesc.Create(0, tableName, defs)
			convey.So(err,convey.ShouldBeNil)

			table, err := dbDesc.Relation(tableName)
			convey.So(err,convey.ShouldBeNil)

			checkTable := func(table engine.Relation) {
				tableDesc,ok := table.(*TpeRelation)
				convey.So(ok,convey.ShouldBeTrue)
				convey.So(tableDesc.id,convey.ShouldEqual,3+i)
				convey.So(tableDesc.desc.ID,convey.ShouldEqual,3+i)
				convey.So(tableDesc.desc.Name,convey.ShouldEqual,tableName)

				pkAttrs := tableDesc.desc.Primary_index.Attributes
				convey.So(len(pkAttrs),convey.ShouldEqual,2)
				for j, pkAttr := range pkAttrs {
					convey.So(pkAttr.ID,convey.ShouldEqual,j)
					convey.So(pkAttr.Name,convey.ShouldEqual,pkDefs[0].Names[j])
					convey.So(pkAttr.Type,convey.ShouldEqual,orderedcodec.VALUE_TYPE_UINT64)
				}

				convey.So(len(attrDefs),convey.ShouldEqual,len(tableDesc.desc.Attributes))

				attrs :=tableDesc.desc.Attributes
				for i, attr := range attrs {
					if attr.Is_primarykey {
						continue
					}
					convey.So(attr.ID,convey.ShouldEqual,i)
					convey.So(attr.Ttype,convey.ShouldEqual,orderedcodec.VALUE_TYPE_UINT64)
					convey.So(attr.TypesType.Oid,convey.ShouldEqual,types.T_uint64)
				}
			}

			checkTable(table)
		}

		wantNames := dbDesc.Relations()
		for i := 0; i < cnt; i++ {
			convey.So(wantNames[i],convey.ShouldEqual,tableNames[i])
		}

		var restNames []string
		for i := 0; i < cnt; i++ {
			if i % 2 == 0 {
				err = dbDesc.Delete(0, tableNames[i])
				convey.So(err,convey.ShouldBeNil)
			}else{
				restNames = append(restNames,tableNames[i])
			}
		}
		wantNames = dbDesc.Relations()
		convey.So(wantNames,convey.ShouldResemble,restNames)

		//recreate the dropped table
		for i := 0; i < cnt; i++ {
			if i%2 == 0 {
				err = dbDesc.Create(0,tableNames[i],defs)
				convey.So(err, convey.ShouldBeNil)
			}
		}
	})
}