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

package orderedcodec

import (
	"bytes"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestOrderedDecoder_DecodeKey(t *testing.T) {
	convey.Convey("decodeKey",t, func() {
		od := &OrderedDecoder{}
		kases := [][]byte{
			nil,
		}

		d,di,e := od.DecodeKey(kases[0])
		convey.So(e,convey.ShouldBeError)
		convey.So(d,convey.ShouldEqual,kases[0])
		convey.So(di,convey.ShouldBeNil)
	})
}

func TestOrderedDecoder_IsNull(t *testing.T) {
	convey.Convey("isNull",t, func() {
		od := &OrderedDecoder{}

		kases := [][]byte{
			nil,
			[]byte{1},
			[]byte{0},
			[]byte{0,1,2},
		}

		d,di,e := od.IsNull(kases[0])
		convey.So(e,convey.ShouldBeError)
		convey.So(bytes.Equal(d,kases[0]),convey.ShouldBeTrue)
		convey.So(di,convey.ShouldBeNil)

		d,di,e = od.IsNull(kases[1])
		convey.So(e,convey.ShouldBeError)
		convey.So(bytes.Equal(d,kases[1]),convey.ShouldBeTrue)
		convey.So(di,convey.ShouldBeNil)

		d,di,e = od.IsNull(kases[2])
		convey.So(e,convey.ShouldBeNil)
		convey.So(bytes.Equal(d,kases[2][1:]),convey.ShouldBeTrue)
		convey.So(di.value,convey.ShouldBeNil)
		convey.So(di.valueType,convey.ShouldEqual,VALUE_TYPE_NULL)

		d,di,e = od.IsNull(kases[3])
		convey.So(e,convey.ShouldBeNil)
		convey.So(bytes.Equal(d,kases[3][1:]),convey.ShouldBeTrue)
		convey.So(di.value,convey.ShouldBeNil)
		convey.So(di.valueType,convey.ShouldEqual,VALUE_TYPE_NULL)
	})
}
