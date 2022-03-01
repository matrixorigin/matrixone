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
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestTpeEngine_Create(t *testing.T) {
	convey.Convey("create/delete/list/get database",t, func() {
		tpe := NewTpeEngine(&TpeConfig{})
		cnt := 10

		var dbNames []string
		for i := 0; i < cnt; i++ {
			dbName := fmt.Sprintf("testdb%d", i)
			dbNames = append(dbNames,dbName)
		}

		for i := 0; i < cnt; i++ {
			err := tpe.Create(0,dbNames[i],0)
			convey.So(err,convey.ShouldBeNil)
		}

		wantDbNames := tpe.Databases()
		convey.So(reflect.DeepEqual(dbNames,wantDbNames),convey.ShouldBeTrue)

		var dbNames2 []string
		for i := 0; i < cnt; i++ {
			if i % 2 != 0 {
				err := tpe.Delete(0,dbNames[i])
				convey.So(err,convey.ShouldBeNil)
			}
			if i % 2 == 0 {
				dbNames2 =append(dbNames2,dbNames[i])
			}
		}

		wantDbNames = tpe.Databases()
		convey.So(reflect.DeepEqual(dbNames2,wantDbNames),convey.ShouldBeTrue)

		db, err := tpe.Database(dbNames2[0])
		convey.So(err,convey.ShouldBeNil)

		tpeDb,ok := db.(*TpeDatabase)
		convey.So(ok,convey.ShouldBeTrue)
		convey.So(tpeDb.id,convey.ShouldEqual,3)
		convey.So(tpeDb.desc.ID,convey.ShouldEqual,3)
		convey.So(tpeDb.desc.Name,convey.ShouldEqual,dbNames2[0])

		//recreate database again
		//TODO:to fix
		/*
		for i := 0; i < cnt; i++ {
			if i%2 != 0 {
				err = tpe.Create(0,dbNames[i],0)
				convey.So(err,convey.ShouldBeNil)
			}
		}
		*/
	})

	convey.Convey("get node",t, func() {
		tpe := NewTpeEngine(&TpeConfig{})
		ni := tpe.Node("")
		convey.So(ni.Mcpu,convey.ShouldEqual,1)
	})
}