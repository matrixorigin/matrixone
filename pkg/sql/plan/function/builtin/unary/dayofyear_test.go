// Copyright 2022 Matrix Origin
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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

func TestDayOfYear(t *testing.T) {
	convey.Convey("RightCase", t, func() {
		type kase struct {
			s    string
			want uint16
		}

		kases := []kase{
			{
				s:    "1999-04-05",
				want: 95,
			},
			{
				s:    "2004-04-03",
				want: 94,
			},
		}

		var intStrs []string
		var outUint16 []uint16
		for _, k := range kases {
			intStrs = append(intStrs, k.s)
			outUint16 = append(outUint16, k.want)
		}

		inVector := testutil.MakeDateVector(intStrs, nil)
		wantVec := testutil.MakeUint16Vector(outUint16, nil)
		proc := testutil.NewProc()
		res, err := DayOfYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("ScalarCase", t, func() {
		type kase struct {
			s    string
			want uint16
		}

		k := kase{
			s:    "2004-09-03",
			want: 247,
		}

		inVector := testutil.MakeScalarDate(k.s, 10)
		wantVec := testutil.MakeScalarUint16(k.want, 10)
		proc := testutil.NewProc()
		res, err := DayOfYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("NullCase", t, func() {
		inVector := testutil.MakeScalarNull(types.T_date, 10)
		wantVec := testutil.MakeScalarNull(types.T_uint16, 10)
		proc := testutil.NewProc()
		res, err := DayOfYear([]*vector.Vector{inVector}, proc)
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(wantVec, res)
		convey.So(compare, convey.ShouldBeTrue)
	})

}
