// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/smartystreets/goconvey/convey"
)

func Test_makeAutoIncrBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	convey.Convey("Test makeAutoIncrBatch succ", t, func() {
		name := "a"
		num, step := 0, 1
		bat := makeAutoIncrBatch(name, uint64(num), uint64(step), mp)
		convey.So(bat.Attrs, convey.ShouldResemble, AUTO_INCR_TABLE_COLNAME)
		convey.So(len(bat.Vecs), convey.ShouldEqual, 3)
		convey.So(bat.Vecs[1].Col, convey.ShouldResemble, []uint64{uint64(num)})
		convey.So(bat.Vecs[2].Col, convey.ShouldResemble, []uint64{uint64(step)})
	})
}

func Test_getAutoIncrTableDef(t *testing.T) {
	convey.Convey("Test getAutoIncrTableDef succ", t, func() {
		def := getAutoIncrTableDef()
		convey.So(len(def), convey.ShouldEqual, 4)
		nameAttr, ok := def[0].(*engine.AttributeDef)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(nameAttr.Attr.Name, convey.ShouldEqual, AUTO_INCR_TABLE_COLNAME[0])
		convey.So(nameAttr.Attr.Type, convey.ShouldResemble, types.T_varchar.ToType())
		convey.So(nameAttr.Attr.Primary, convey.ShouldBeTrue)

		numAttr, ok := def[1].(*engine.AttributeDef)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(numAttr.Attr.Name, convey.ShouldEqual, AUTO_INCR_TABLE_COLNAME[1])
		convey.So(numAttr.Attr.Type, convey.ShouldResemble, types.T_uint64.ToType())
		convey.So(numAttr.Attr.Primary, convey.ShouldBeFalse)

		stepAttr, ok := def[2].(*engine.AttributeDef)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(stepAttr.Attr.Name, convey.ShouldEqual, AUTO_INCR_TABLE_COLNAME[2])
		convey.So(stepAttr.Attr.Type, convey.ShouldResemble, types.T_uint64.ToType())
		convey.So(stepAttr.Attr.Primary, convey.ShouldBeFalse)

		pri, ok := def[3].(*engine.PrimaryIndexDef)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(pri.Names, convey.ShouldResemble, []string{AUTO_INCR_TABLE_COLNAME[0]})
	})
}
