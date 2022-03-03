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
	"math"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/goconvey/convey"
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

func TestOrderedEncoder_EncodeString(t *testing.T) {
	type args struct {
		value string
		want []byte
	}

	convey.Convey("encodeString",t, func() {
		oe := &OrderedEncoder{}

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
			d,_ := oe.EncodeString(nil,kase.value)
			convey.So(d,should.Resemble,kase.want)
		}
	})
}

func TestOrderedEncoder_EncodeBool(t *testing.T) {
	type args struct {
		value bool
		want []byte
	}

	convey.Convey("encodeBool",t, func() {
		oe := &OrderedEncoder{}

		kases := []args{
			{false, []byte{encodingPrefixForIntegerZero}},
			{true, []byte{encodingPrefixForIntegerZero + 1}},
		}
		for _, kase := range kases {
			d, _ := oe.EncodeBool(nil, kase.value)
			convey.So(d, convey.ShouldResemble, kase.want)
		}
	})
}

func TestOrderedEncoder_EncodeInt8(t *testing.T) {
	type args struct {
		value int8
		want []byte
	}

	convey.Convey("encodeInt8",t, func() {
		oe := &OrderedEncoder{}

		kases := []args{
			{0, []byte{encodingPrefixForIntegerZero}},
			{1, []byte{encodingPrefixForIntegerZero + 1}},
			{-1, []byte{encodingPrefixForIntegerMinimum + 7, 255}},
			{-0x80, []byte{encodingPrefixForIntegerMinimum + 7, 128}},
			{0x7f, []byte{encodingPrefixForIntMax - 7, 127}},
		}
		for _, kase := range kases {
			d, _ := oe.EncodeInt8(nil, kase.value)
			convey.So(d, convey.ShouldResemble, kase.want)
		}
	})
}

func TestOrderedEncoder_EncodeInt16(t *testing.T) {
	type args struct {
		value int16
		want []byte
	}

	convey.Convey("encodeInt16",t, func() {
		oe := &OrderedEncoder{}

		kases := []args{
			{0, []byte{encodingPrefixForIntegerZero}},
			{1, []byte{encodingPrefixForIntegerZero + 1}},
			{-1, []byte{encodingPrefixForIntegerMinimum + 7, 255}},
			{-0xff + 1, []byte{encodingPrefixForIntegerMinimum + 7, 2}},
			{-0xff, []byte{encodingPrefixForIntegerMinimum + 7, 1}},
			{-0xff - 1, []byte{encodingPrefixForIntegerMinimum + 6, 255, 0}},
			{-0x7fff + 1, []byte{encodingPrefixForIntegerMinimum + 6, 128, 2}},
			{-0x7fff, []byte{encodingPrefixForIntegerMinimum + 6, 128, 1}},
			{-0x7fff - 1, []byte{encodingPrefixForIntegerMinimum + 6, 128, 0}},
			{0xff - 1, []byte{encodingPrefixForIntMax - 7, 254}},
			{0xff, []byte{encodingPrefixForIntMax - 7, 255}},
			{0xff + 1, []byte{encodingPrefixForIntMax - 6, 1, 0}},
			{0x7fff - 1, []byte{encodingPrefixForIntMax - 6, 127, 254}},
			{0x7fff, []byte{encodingPrefixForIntMax - 6, 127, 255}},
		}
		for _, kase := range kases {
			d, _ := oe.EncodeInt16(nil, kase.value)
			convey.So(d, convey.ShouldResemble, kase.want)
		}
	})
}

func TestOrderedEncoder_EncodeInt32(t *testing.T) {
	type args struct {
		value int32
		want []byte
	}

	convey.Convey("encodeInt32",t, func() {
		oe := &OrderedEncoder{}

		kases := []args{
			{-0xffff + 1, []byte{encodingPrefixForIntegerMinimum + 6, 0, 2}},
			{-0xffff, []byte{encodingPrefixForIntegerMinimum + 6, 0, 1}},
			{-0xffff - 1, []byte{encodingPrefixForIntegerMinimum + 5, 255, 0, 0}},
			{-0xffffff + 1, []byte{encodingPrefixForIntegerMinimum + 5, 0, 0, 2}},
			{-0xffffff, []byte{encodingPrefixForIntegerMinimum + 5, 0, 0, 1}},
			{-0xffffff - 1, []byte{encodingPrefixForIntegerMinimum + 4, 255, 0, 0, 0}},
			{-0x7fffffff + 1, []byte{encodingPrefixForIntegerMinimum + 4, 128, 0, 0, 2}},
			{-0x7fffffff, []byte{encodingPrefixForIntegerMinimum + 4, 128, 0, 0, 1}},
			{-0x7fffffff - 1, []byte{encodingPrefixForIntegerMinimum + 4, 128, 0, 0, 0}},
			{0xffff - 1, []byte{encodingPrefixForIntMax - 6, 255, 254}},
			{0xffff, []byte{encodingPrefixForIntMax - 6, 255, 255}},
			{0xffff + 1, []byte{encodingPrefixForIntMax - 5, 1, 0, 0}},
			{0xffffff - 1, []byte{encodingPrefixForIntMax - 5, 255, 255, 254}},
			{0xffffff, []byte{encodingPrefixForIntMax - 5, 255, 255, 255}},
			{0xffffff + 1, []byte{encodingPrefixForIntMax - 4, 1, 0, 0, 0}},
			{0x7fffffff - 1, []byte{encodingPrefixForIntMax - 4, 127, 255, 255, 254}},
			{0x7fffffff, []byte{encodingPrefixForIntMax - 4, 127, 255, 255, 255}},
		}
		for _, kase := range kases {
			d, _ := oe.EncodeInt32(nil, kase.value)
			convey.So(d, convey.ShouldResemble, kase.want)
		}
	})
}

func TestOrderedEncoder_EncodeInt64(t *testing.T) {
	type args struct {
		value int64
		want []byte
	}

	convey.Convey("encodeInt64",t, func() {
		oe := &OrderedEncoder{}

		kases := []args{
			{math.MinInt64, []byte{0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{math.MinInt64 + 1, []byte{0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
			{-1 << 8, []byte{0x86, 0xff, 0x00}},
			{-1, []byte{0x87, 0xff}},
			{0, []byte{0x88}},
			{1, []byte{0x89}},
			{109, []byte{0xf5}},
			{112, []byte{0xf6, 0x70}},
			{1 << 8, []byte{0xf7, 0x01, 0x00}},
			{math.MaxInt64, []byte{0xfd, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		}
		for _, kase := range kases {
			d, _ := oe.EncodeInt64(nil, kase.value)
			convey.So(d, convey.ShouldResemble, kase.want)
		}
	})
}

func TestOrderedEncoder_EncodeFloat32(t *testing.T) {
	type args struct {
		value float32
		want []byte
	}
	convey.Convey("encodeFloat32",t, func() {
		oe := &OrderedEncoder{}

		kases := []args{
			{float32(math.NaN()), []byte{encodingfloatNaN}},
			{0, []byte{encodingfloatZero}},
			{1, []byte{encodingfloatPos, 63, 240, 0, 0, 0, 0, 0, 0}},
			{-1, []byte{encodingfloatNeg, 64, 15, 255, 255, 255, 255, 255, 255}},
		}
		for _, kase := range kases {
			d, _ := oe.EncodeFloat32(nil, kase.value)
			convey.So(d, convey.ShouldResemble, kase.want)
		}
	})
}


func TestOrderedEncoder_EncodeFloat64(t *testing.T) {
	type args struct {
		value float64
		want []byte
	}
	convey.Convey("encodeFloat64",t, func() {
		oe := &OrderedEncoder{}

		kases := []args{
			{math.NaN(), []byte{0x02}},
			{math.Inf(-1), []byte{0x03, 0x00, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{-math.MaxFloat64, []byte{0x03, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{-1e308, []byte{0x03, 0x00, 0x1e, 0x33, 0x0c, 0x7a, 0x14, 0x37, 0x5f}},
			{-10000.0, []byte{0x03, 0x3f, 0x3c, 0x77, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{-9999.0, []byte{0x03, 0x3f, 0x3c, 0x78, 0x7f, 0xff, 0xff, 0xff, 0xff}},
			{-100.0, []byte{0x03, 0x3f, 0xa6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{-99.0, []byte{0x03, 0x3f, 0xa7, 0x3f, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{-1.0, []byte{0x03, 0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{-0.00123, []byte{0x03, 0x40, 0xab, 0xd9, 0x01, 0x8e, 0x75, 0x79, 0x28}},
			{-1e-307, []byte{0x03, 0x7f, 0xce, 0x05, 0xe7, 0xd3, 0xbf, 0x39, 0xf2}},
			{-math.SmallestNonzeroFloat64, []byte{0x03, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
			{math.Copysign(0, -1), []byte{0x04}},
			{0, []byte{0x04}},
			{math.SmallestNonzeroFloat64, []byte{0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
			{1e-307, []byte{0x05, 0x00, 0x31, 0xfa, 0x18, 0x2c, 0x40, 0xc6, 0x0d}},
			{0.00123, []byte{0x05, 0x3f, 0x54, 0x26, 0xfe, 0x71, 0x8a, 0x86, 0xd7}},
			{0.0123, []byte{0x05, 0x3f, 0x89, 0x30, 0xbe, 0x0d, 0xed, 0x28, 0x8d}},
			{0.123, []byte{0x05, 0x3f, 0xbf, 0x7c, 0xed, 0x91, 0x68, 0x72, 0xb0}},
			{1.0, []byte{0x05, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{10.0, []byte{0x05, 0x40, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{12.345, []byte{0x05, 0x40, 0x28, 0xb0, 0xa3, 0xd7, 0x0a, 0x3d, 0x71}},
			{99.0, []byte{0x05, 0x40, 0x58, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{99.0001, []byte{0x05, 0x40, 0x58, 0xc0, 0x01, 0xa3, 0x6e, 0x2e, 0xb2}},
			{99.01, []byte{0x05, 0x40, 0x58, 0xc0, 0xa3, 0xd7, 0x0a, 0x3d, 0x71}},
			{100.0, []byte{0x05, 0x40, 0x59, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{100.01, []byte{0x05, 0x40, 0x59, 0x00, 0xa3, 0xd7, 0x0a, 0x3d, 0x71}},
			{100.1, []byte{0x05, 0x40, 0x59, 0x06, 0x66, 0x66, 0x66, 0x66, 0x66}},
			{1234, []byte{0x05, 0x40, 0x93, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{1234.5, []byte{0x05, 0x40, 0x93, 0x4a, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{9999, []byte{0x05, 0x40, 0xc3, 0x87, 0x80, 0x00, 0x00, 0x00, 0x00}},
			{9999.000001, []byte{0x05, 0x40, 0xc3, 0x87, 0x80, 0x00, 0x08, 0x63, 0x7c}},
			{9999.000009, []byte{0x05, 0x40, 0xc3, 0x87, 0x80, 0x00, 0x4b, 0x7f, 0x5a}},
			{9999.00001, []byte{0x05, 0x40, 0xc3, 0x87, 0x80, 0x00, 0x53, 0xe2, 0xd6}},
			{9999.00009, []byte{0x05, 0x40, 0xc3, 0x87, 0x80, 0x02, 0xf2, 0xf9, 0x87}},
			{9999.000099, []byte{0x05, 0x40, 0xc3, 0x87, 0x80, 0x03, 0x3e, 0x78, 0xe2}},
			{9999.0001, []byte{0x05, 0x40, 0xc3, 0x87, 0x80, 0x03, 0x46, 0xdc, 0x5d}},
			{9999.001, []byte{0x05, 0x40, 0xc3, 0x87, 0x80, 0x20, 0xc4, 0x9b, 0xa6}},
			{9999.01, []byte{0x05, 0x40, 0xc3, 0x87, 0x81, 0x47, 0xae, 0x14, 0x7b}},
			{9999.1, []byte{0x05, 0x40, 0xc3, 0x87, 0x8c, 0xcc, 0xcc, 0xcc, 0xcd}},
			{10000, []byte{0x05, 0x40, 0xc3, 0x88, 0x00, 0x00, 0x00, 0x00, 0x00}},
			{10001, []byte{0x05, 0x40, 0xc3, 0x88, 0x80, 0x00, 0x00, 0x00, 0x00}},
			{12345, []byte{0x05, 0x40, 0xc8, 0x1c, 0x80, 0x00, 0x00, 0x00, 0x00}},
			{123450, []byte{0x05, 0x40, 0xfe, 0x23, 0xa0, 0x00, 0x00, 0x00, 0x00}},
			{1e308, []byte{0x05, 0x7f, 0xe1, 0xcc, 0xf3, 0x85, 0xeb, 0xc8, 0xa0}},
			{math.MaxFloat64, []byte{0x05, 0x7f, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{math.Inf(1), []byte{0x05, 0x7f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		}
		for _, kase := range kases {
			d, _ := oe.EncodeFloat64(nil, kase.value)
			convey.So(d, convey.ShouldResemble, kase.want)
		}
	})
}