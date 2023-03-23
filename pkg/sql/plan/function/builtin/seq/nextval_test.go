// Copyright 2021 - 2022 Matrix Origin
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

package seq

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestMakePosIncr(t *testing.T) {
	convey.Convey("Test01", t, func() {
		cases := []struct {
			incr int64
			res  int64
		}{
			{
				incr: 20,
				res:  20,
			},
			{
				incr: -30,
				res:  30,
			},
		}

		var expects []int64
		var input []int64
		var res []int64
		for _, c := range cases {
			expects = append(expects, c.res)
			input = append(input, c.incr)
		}

		for _, v := range input {
			r := makePosIncr[int64](v)
			res = append(res, r)
		}
		compare := true
		for i := range expects {
			if expects[i] != res[i] {
				compare = false
			}
		}
		convey.So(compare, convey.ShouldBeTrue)
	})
}
