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

package multi

import (
	"errors"
	"log"
	"math"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func Test_Pi(t *testing.T) {
	convey.Convey("Test Pi function succ", t, func() {
		vec, err := Pi(nil, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(vec.Nsp.Np, convey.ShouldBeNil)
		data, ok := vec.Col.([]float64)
		if !ok {
			log.Fatal(errors.New("the Pi function return value type is not []float64"))
		}
		convey.So(data[0], convey.ShouldEqual, math.Pi)

	})
}
