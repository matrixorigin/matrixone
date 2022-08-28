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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPrepareInitialData(t *testing.T) {
	convey.Convey("mo_database", t, func() {
		sch := DefineSchemaForMoDatabase()
		data := PrepareInitialDataForMoDatabase()
		bat := FillInitialDataForMoDatabase()
		convey.So(bat, convey.ShouldNotBeNil)
		convey.So(batch.Length(bat), convey.ShouldEqual, len(data))
		convey.So(len(bat.Vecs), convey.ShouldEqual, len(data[0]))
		convey.So(len(bat.Vecs), convey.ShouldEqual, sch.Length())
		for i, attr := range sch.GetAttributes() {
			convey.So(attr.AttributeType.Eq(bat.Vecs[i].Typ), convey.ShouldBeTrue)
		}
		for i, line := range data {
			s := FormatLineInBatch(bat, i)
			convey.So(line, convey.ShouldResemble, s)
		}
	})

	convey.Convey("mo_tables", t, func() {
		sch := DefineSchemaForMoTables()
		data := PrepareInitialDataForMoTables()
		bat := FillInitialDataForMoTables()
		convey.So(bat, convey.ShouldNotBeNil)
		convey.So(batch.Length(bat), convey.ShouldEqual, len(data))
		convey.So(len(bat.Vecs), convey.ShouldEqual, len(data[0]))
		convey.So(len(bat.Vecs), convey.ShouldEqual, sch.Length())
		for i, attr := range sch.GetAttributes() {
			convey.So(attr.AttributeType.Eq(bat.Vecs[i].Typ), convey.ShouldBeTrue)
		}
		for i, line := range data {
			s := FormatLineInBatch(bat, i)
			convey.So(line, convey.ShouldResemble, s)
		}
	})

	convey.Convey("mo_columns", t, func() {
		sch := DefineSchemaForMoColumns()
		data := PrepareInitialDataForMoColumns()
		bat := FillInitialDataForMoColumns()
		convey.So(bat, convey.ShouldNotBeNil)
		convey.So(batch.Length(bat), convey.ShouldEqual, len(data))
		convey.So(len(bat.Vecs), convey.ShouldEqual, len(data[0]))
		convey.So(len(bat.Vecs), convey.ShouldEqual, sch.Length())
		for i, attr := range sch.GetAttributes() {
			convey.So(attr.AttributeType.Eq(bat.Vecs[i].Typ), convey.ShouldBeTrue)
		}
		for i, line := range data {
			s := FormatLineInBatch(bat, i)
			convey.So(line, convey.ShouldResemble, s)
		}
	})

	convey.Convey("mo_global_variables", t, func() {
		sch := DefineSchemaForMoGlobalVariables()
		data := PrepareInitialDataForMoGlobalVariables()
		bat := FillInitialDataForMoGlobalVariables()
		convey.So(bat, convey.ShouldNotBeNil)
		convey.So(batch.Length(bat), convey.ShouldEqual, len(data))
		convey.So(len(bat.Vecs), convey.ShouldEqual, len(data[0]))
		convey.So(len(bat.Vecs), convey.ShouldEqual, sch.Length())
		for i, attr := range sch.GetAttributes() {
			convey.So(attr.AttributeType.Eq(bat.Vecs[i].Typ), convey.ShouldBeTrue)
		}
		for i, line := range data {
			s := FormatLineInBatch(bat, i)
			convey.So(line, convey.ShouldResemble, s)
		}
	})

	convey.Convey("mo_user", t, func() {
		sch := DefineSchemaForMoUser()
		data := PrepareInitialDataForMoUser()
		bat := FillInitialDataForMoUser()
		convey.So(bat, convey.ShouldNotBeNil)
		convey.So(batch.Length(bat), convey.ShouldEqual, len(data))
		convey.So(len(bat.Vecs), convey.ShouldEqual, len(data[0]))
		convey.So(len(bat.Vecs), convey.ShouldEqual, sch.Length())
		for i, attr := range sch.GetAttributes() {
			convey.So(attr.AttributeType.Eq(bat.Vecs[i].Typ), convey.ShouldBeTrue)
		}
		for i, line := range data {
			s := FormatLineInBatch(bat, i)
			convey.So(line, convey.ShouldResemble, s)
		}
	})
}
