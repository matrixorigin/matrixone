// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"fmt"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestScope(t *testing.T) {
	convey.Convey("test scope", t, func() {
		wanted := make(map[Scope]string)
		for i := ScopeGlobal; i <= ScopeResetPersist; i++ {
			wanted[i] = i.String()
		}

		convey.So(wanted[ScopeBoth], convey.ShouldEqual, ScopeBoth.String())
	})
}

func TestSystemVariable(t *testing.T) {
	convey.Convey("all", t, func() {
		bt := SystemVariableBoolType{}
		it := SystemVariableIntType{}
		ut := SystemVariableUintType{}
		dt := SystemVariableDoubleType{}
		et := SystemVariableEnumType{}
		sett := SystemVariableSetType{}
		st := SystemVariableStringType{}
		nt := SystemVariableNullType{}
		svs := []SystemVariableType{
			bt,
			it,
			ut,
			dt,
			et,
			sett,
			st,
			nt,
		}

		for _, sv := range svs {
			fmt.Sprintln(sv.String(), sv.Type(), sv.MysqlType(), sv.Zero())
		}

		btrt, err := nt.Convert(nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(btrt, convey.ShouldBeNil)

		_, err = nt.Convert("string")
		convey.So(err, convey.ShouldNotBeNil)

		_, err = bt.Convert(0)
		convey.So(err, convey.ShouldBeNil)

		_, err = bt.Convert(1)
		convey.So(err, convey.ShouldBeNil)

	})
}
