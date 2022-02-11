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
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/goconvey/convey"
	"math"
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

func TestOrderedEncoder_EncodeUint64(t *testing.T) {
	type args struct {
		value uint64
		want []byte
	}
	convey.Convey("encodeUint64",t, func() {
		oe := &OrderedEncoder{}

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
			d,_ := oe.EncodeUint64(nil,kase.value)
			convey.So(d,should.Resemble,kase.want)
		}
	})
}

func TestOrderedEncoder_EncodeBytes(t *testing.T) {
	type args struct {
		value []byte
		want []byte
	}

	convey.Convey("encodeBytes",t, func() {
		oe := &OrderedEncoder{}

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
			d,_ := oe.EncodeBytes(nil,kase.value)
			convey.So(d,should.Resemble,kase.want)
		}
	})
}