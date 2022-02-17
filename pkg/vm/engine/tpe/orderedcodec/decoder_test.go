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
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/goconvey/convey"
	"math"
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
		convey.So(di.Value,convey.ShouldBeNil)
		convey.So(di.ValueType,convey.ShouldEqual,VALUE_TYPE_NULL)

		d,di,e = od.IsNull(kases[3])
		convey.So(e,convey.ShouldBeNil)
		convey.So(bytes.Equal(d,kases[3][1:]),convey.ShouldBeTrue)
		convey.So(di.Value,convey.ShouldBeNil)
		convey.So(di.ValueType,convey.ShouldEqual,VALUE_TYPE_NULL)
	})
}

func TestOrderedDecoder_DecodeUint64(t *testing.T) {
	type args struct {
		want uint64
		value []byte
	}
	convey.Convey("decodeUint64",t, func() {
		od := &OrderedDecoder{}

		kases := []args{
			{0, []byte{136}},
			{1, []byte{137}},
			{encodingPrefixForSplit, []byte{245}},
			{encodingPrefixForSplit+1, []byte{246,110}},
			{0xff,[]byte{246,255}},
			{0xff+1,[]byte{247,1,0}},
			{0xffff,[]byte{247,255,255}},
			{0xffff+1,[]byte{248,1,0,0}},
			{0xffffff,[]byte{248,255,255,255}},
			{0xffffff+1,[]byte{249,1,0,0,0}},
			{0xffffffff,[]byte{249,255,255,255,255}},
			{0xffffffff+1,[]byte{250,1,0,0,0,0}},
			{0xffffffffff,[]byte{250,255,255,255,255,255}},
			{0xffffffffff+1,[]byte{251,1,0,0,0,0,0}},
			{0xffffffffffff,[]byte{251,255,255,255,255,255,255}},
			{0xffffffffffff+1,[]byte{252,1,0,0,0,0,0,0}},
			{0xffffffffffffff,[]byte{252,255,255,255,255,255,255,255}},
			{0xffffffffffffff+1,[]byte{253,1,0,0,0,0,0,0,0}},
			{math.MaxUint64-1,[]byte{253,255,255,255,255,255,255,255,254}},
			{math.MaxUint64,[]byte{253,255,255,255,255,255,255,255,255}},
		}

		for _, kase := range kases {
			d,di, _ := od.DecodeUint64(kase.value)
			convey.So(d,convey.ShouldBeEmpty)
			convey.So(di.Value,convey.ShouldEqual,kase.want)
		}
	})

	convey.Convey("decodeUint64",t, func() {
		od := &OrderedDecoder{}

		d,di,e := od.DecodeUint64(nil)
		convey.So(e,convey.ShouldBeError)
		convey.So(d,convey.ShouldBeNil)
		convey.So(di,convey.ShouldBeNil)

		d,di,e = od.DecodeUint64([]byte{254})
		convey.So(e,convey.ShouldBeError)
		convey.So(d,convey.ShouldBeNil)
		convey.So(di,convey.ShouldBeNil)

		d,di,e = od.DecodeUint64([]byte{255})
		convey.So(e,convey.ShouldBeError)
		convey.So(d,convey.ShouldBeNil)
		convey.So(di,convey.ShouldBeNil)

		d,di,e = od.DecodeUint64([]byte{253})
		convey.So(e,convey.ShouldBeError)
		convey.So(d,convey.ShouldBeNil)
		convey.So(di,convey.ShouldBeNil)
	})
}

func TestOrderedDecoder_DecodeBytes(t *testing.T) {
	type args struct {
		want []byte
		value []byte
	}

	convey.Convey("encodeBytes",t, func() {
		od := &OrderedDecoder{}

		tag := encodingPrefixForBytes
		fb := byteEscapedToFirstByte
		sb := byteEscapedToSecondByte
		btbe := byteToBeEscaped
		bfbe := byteForBytesEnding

		kases := []args{
			{[]byte{0},[]byte{tag,fb,sb,btbe,bfbe}},
			{[]byte{0,1},[]byte{tag,fb,sb,1,btbe,bfbe}},
			{[]byte{0xff,0,1},[]byte{tag,0xff,fb,sb,1,btbe,bfbe}},
			{[]byte{0,0},[]byte{tag,fb,sb,fb,sb,btbe,bfbe}},
			{[]byte{0,0,0},[]byte{tag,fb,sb,fb,sb,fb,sb,btbe,bfbe}},
			{[]byte("matrix"),[]byte{tag,'m','a','t','r','i','x',btbe,bfbe}},
		}
		for _, kase := range kases {
			d,di,e := od.DecodeBytes(kase.value)
			convey.So(e,convey.ShouldBeNil)
			convey.So(d,convey.ShouldBeEmpty)
			convey.So(di.Value,should.Resemble,kase.want)
		}
	})
}

func TestOrderedDecoder_DecodeString(t *testing.T) {
	type args struct {
		want string
		value []byte
	}

	convey.Convey("encodeBytes",t, func() {
		od := &OrderedDecoder{}

		tag := encodingPrefixForBytes
		fb := byteEscapedToFirstByte
		sb := byteEscapedToSecondByte
		btbe := byteToBeEscaped
		bfbe := byteForBytesEnding

		kases := []args{
			{"\x00",[]byte{tag,fb,sb,btbe,bfbe}},
			{"\x00\x01",[]byte{tag,fb,sb,1,btbe,bfbe}},
			{"\xff\x00\x01",[]byte{tag,0xff,fb,sb,1,btbe,bfbe}},
			{"\x00\x00",[]byte{tag,fb,sb,fb,sb,btbe,bfbe}},
			{"\x00\x00\x00",[]byte{tag,fb,sb,fb,sb,fb,sb,btbe,bfbe}},
			{"matrix",[]byte{tag,'m','a','t','r','i','x',btbe,bfbe}},
		}
		for _, kase := range kases {
			d,di,e := od.DecodeString(kase.value)
			convey.So(e,convey.ShouldBeNil)
			convey.So(d,convey.ShouldBeEmpty)
			convey.So(di.Value,should.Resemble,kase.want)
		}
	})
}