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
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestOrderedEncoder_EncodeKey(t *testing.T) {
	convey.Convey("encodeKey null",t, func() {
		oe := &OrderedEncoder{}
		d,_ := oe.EncodeKey([]byte{},nil)
		convey.So(d[len(d) - 1],convey.ShouldEqual,nullEncoding)
	})
}

func TestOrderedEncoder_EncodeNull(t *testing.T) {
	convey.Convey("encodeNUll",t, func() {
		oe := &OrderedEncoder{}
		kases := [][]byte{
			nil,
			[]byte{},
			[]byte{0x0,0x1},
		}
		for _, k := range kases {
			d,_ := oe.EncodeNull(k)
			convey.So(d[len(d) - 1],convey.ShouldEqual,nullEncoding)
		}
	})
}