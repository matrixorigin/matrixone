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
	"testing"
)

func TestTpeRelation_Write(t *testing.T) {
	convey.Convey("table write with no primary key",t, func() {
		tpe := NewTpeEngine(&TpeConfig{})
		err := tpe.Create(0, "test", 0)
		convey.So(err,convey.ShouldBeNil)

		dbDesc, err := tpe.Database("test")
		convey.So(err,convey.ShouldBeNil)

		//(a,b,c)
		//(uint64,uint64,uint64)
		_,attrDefs := tuplecodec.MakeAttributes(types.T_uint64,types.T_uint64,types.T_uint64)


		attrNames := []string{
			"a","b","c",
		}
		var defs []engine.TableDef
		var rawDefs []*engine.AttributeDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs,def)
			rawDefs = append(rawDefs,def)
		}

		err = dbDesc.Create(0,"A",defs)
		convey.So(err,convey.ShouldBeNil)

		tableDesc, err := dbDesc.Relation("A")
		convey.So(err,convey.ShouldBeNil)

		//make data
		bat := tuplecodec.MakeBatch(10,attrNames, rawDefs)

		err = tableDesc.Write(0, bat)
		convey.So(err,convey.ShouldBeNil)
	})
}