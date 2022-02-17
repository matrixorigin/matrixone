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

package tuplecodec

import (
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestTupleKey_Less(t *testing.T) {
	convey.Convey("key less",t, func() {
		type args struct {
			a TupleKey
			b TupleKey
			want bool
		}

		kases := []args {
			{nil,[]byte{},false},
			{nil,[]byte{0},true},
			{TupleKey(""),TupleKey("a"),true},
			{TupleKey("a"),TupleKey("b"),true},
			{TupleKey("a\x00"),TupleKey("a"),false},
			{TupleKey("a\x00"),TupleKey("a\x01"),true},
			{[]byte{0,1},[]byte{1,0},true},
			{[]byte{1,0},[]byte{0,1},false},
			{[]byte{1,0},[]byte{1,0},false},
		}

		for _, kase := range kases {
			ret := kase.a.Less(kase.b)
			convey.So(ret,convey.ShouldEqual,kase.want)
		}
	})
}

func TestTupleKey_Compare(t *testing.T) {
	convey.Convey("key compare",t, func() {
		type args struct {
			a TupleKey
			b TupleKey
			want int
		}
		kases := []args {
			{nil,[]byte{},0},
			{nil,[]byte{0},-1},
			{TupleKey(""),TupleKey("a"),-1},
			{TupleKey("a"),TupleKey("b"),-1},
			{TupleKey("a\x00"),TupleKey("a"),1},
			{TupleKey("a\x00"),TupleKey("a\x01"),-1},
			{[]byte{0,1},[]byte{1,0},-1},
			{[]byte{1,0},[]byte{0,1},1},
			{[]byte{1,0},[]byte{1,0},0},
		}

		for _, kase := range kases {
			ret := kase.a.Compare(kase.b)
			convey.So(ret,convey.ShouldEqual,kase.want)
		}
	})
}

func TestTupleKey_Equal(t *testing.T) {
	convey.Convey("key equal",t, func() {
		type args struct {
			a TupleKey
			b TupleKey
			want bool
		}
		kases := []args {
			{nil,[]byte{},true},
			{nil,[]byte{0},false},
			{TupleKey(""),TupleKey("a"),false},
			{TupleKey("a"),TupleKey("b"),false},
			{TupleKey("a\x00"),TupleKey("a"),false},
			{TupleKey("a\x00"),TupleKey("a\x01"),false},
			{[]byte{0,1},[]byte{1,0},false},
			{[]byte{1,0},[]byte{0,1},false},
			{[]byte{1,0},[]byte{1,0},true},
		}

		for _, kase := range kases {
			ret := kase.a.Equal(kase.b)
			convey.So(ret,convey.ShouldEqual,kase.want)

			ret = kase.b.Equal(kase.b)
			convey.So(ret,convey.ShouldBeTrue)
		}
	})
}

func TestSuccessorOfKey(t *testing.T) {
	convey.Convey("successor of key",t, func() {
		type args struct {
			key TupleKey
			want TupleKey
		}
		kases := []args {
			{nil,[]byte{0}},
			{[]byte{},[]byte{0}},
			{[]byte{0},[]byte{0,0}},
			{[]byte{0xff},[]byte{0xff,0}},
			{[]byte{0xff,0xff},[]byte{0xff,0xff,0}},
			{TupleKey("abc"),TupleKey("abc\x00")},
			{TupleKey("abc\x00"),TupleKey("abc\x00\x00")},
		}

		for _, kase := range kases {
			ret := SuccessorOfKey(kase.key)
			convey.So(ret,convey.ShouldResemble,kase.want)

			convey.So(ret.Equal(kase.want),convey.ShouldBeTrue)
		}
	})
}

func TestSuccessorOfPrefix(t *testing.T) {
	convey.Convey("successor of prefix",t, func() {
		type args struct {
			key TupleKey
			want TupleKey
		}
		kases := []args {
			{nil,TupleKey{0xff}},
			{TupleKey{},TupleKey{0xff}},
			{[]byte{0},[]byte{1}},
			{[]byte{0,0},[]byte{0,1}},
			{[]byte{0,0xff},[]byte{1}},
			{[]byte{0,0xff,0xff},[]byte{1}},
			{[]byte{0xff},[]byte{0xff}},
			{[]byte{0xff,0xfe},[]byte{0xff,0xff}},
			{[]byte{0xff,0xff},[]byte{0xff,0xff}},
			{[]byte{0xff,0xff,0xff},[]byte{0xff,0xff,0xff}},
		}

		for _, kase := range kases {
			ret := SuccessorOfPrefix(kase.key)
			convey.So(ret,convey.ShouldResemble,kase.want)
		}
	})
}

func TestTupleKey_IsPredecessor(t *testing.T) {
	convey.Convey("is predecessor",t, func() {
		type args struct {
			key TupleKey
			another TupleKey
			want bool
		}
		kases := []args {
			{nil,[]byte{0},true},
			{nil,[]byte{0,0},false},
			{[]byte{},[]byte{0},true},
			{[]byte{},[]byte{0,0},false},
			{nil,nil,false},
			{[]byte{0},[]byte{0,0},true},
			{[]byte{0},[]byte{0,0,0},false},
			{[]byte{0xff},[]byte{0xff,0},true},
			{[]byte{0xff},[]byte{0xff,0,0},false},
			{[]byte{0xff,0xff},[]byte{0xff,0xff,0},true},
			{[]byte{0xff,0xff},[]byte{0xff,0xff,0,0},false},
			{TupleKey("abc"),TupleKey("abc\x00"),true},
			{TupleKey("abc"),TupleKey("abc\x00\x00"),false},
			{TupleKey("abc\x00"),TupleKey("abc\x00\x00"),true},
		}

		for _, kase := range kases {
			ret := kase.key.IsPredecessor(kase.another)
			convey.So(ret,convey.ShouldEqual,kase.want)
		}
	})
}

func TestRange_IsValid(t *testing.T) {
	convey.Convey("is valid",t, func() {
		type args struct {
			rg Range
			want bool
		}

		kases := []args{
			{Range{TupleKey("x"),nil},true},
			{Range{TupleKey("x"),TupleKey("y")},true},
			{Range{TupleKey(""),TupleKey("")},false},
			{Range{TupleKey(""),TupleKey("z")},true},
			{Range{TupleKey("x"),TupleKey("x")},false},
			{Range{TupleKey("zzzzzz"),TupleKey("x")},false},
		}

		for _, kase := range kases {
			convey.So(kase.rg.IsValid(),convey.ShouldEqual,kase.want)
		}
	})
}

func TestRange_Contain(t *testing.T) {
	convey.Convey("contain",t, func() {
		rg := Range{
			startKey: TupleKey("x"),
			endKey:   TupleKey("y"),
		}

		type args struct {
			key TupleKey
			want bool
		}

		kases := []args{
			{TupleKey("a"),false},
			{TupleKey("x"),true},
			{TupleKey("xxx"),true},
			{TupleKey("y"),false},
			{TupleKey("z"),false},
			{SuccessorOfKey(TupleKey("y")),false},
		}

		for _, kase := range kases {
			convey.So(rg.Contain(kase.key),convey.ShouldEqual,kase.want)
		}
	})
}

var gen = func(a,b string) Range {
	return Range{
		startKey: TupleKey(a),
		endKey:   TupleKey(b),
	}
}

func TestRange_Equal(t *testing.T) {


	convey.Convey("range equal",t, func() {
		type args struct {
			a Range
			b Range
			want bool
		}

		kases := []args {
			{
				gen("x","y"),
				gen("x","y"),
				true,
			},
			{
				gen("x","y"),
				gen("x","z"),
				false,
			},
			{
				gen("x",""),
				gen("x",""),
				true,
			},
			{
				gen("x",""),
				gen("","x"),
				false,
			},
		}

		for _, kase := range kases {
			convey.So(kase.a.Equal(kase.b),convey.ShouldEqual,kase.want)
		}
	})
}

func TestRange_Merge(t *testing.T) {
	convey.Convey("range merge",t, func() {
		type args struct {
			a Range
			b Range
			want Range
		}

		a := gen("a","")
		d := gen("d","")
		ac := gen("a","c")
		ad := gen("a","d")
		adSucc := gen("a","d")
		adSucc.endKey = SuccessorOfKey(adSucc.endKey)
		bd := gen("b","d")
		bdSucc := gen("b","d")
		bdSucc.endKey = SuccessorOfKey(bdSucc.endKey)
		ca := gen("c","a")
		db := gen("d","b")
		empty := gen("","")

		kases := []args{
			{a,a,a},
			{a,d,adSucc},
			{a,bd,ad},
			{bd,a,ad},
			{d,bd,bdSucc},
			{bd,d,bdSucc},
			{a,ac,ac},
			{ac,a,ac},
			{ac,ac,ac},
			{ac,bd,ad},
			{bd,ac,ad},
			{ac,db,empty},
			{db,ac,empty},
			{bd,ca,empty},
			{ca,bd,empty},
		}

		for _, kase := range kases {
			convey.So(kase.a.Merge(kase.b).Equal(kase.want),convey.ShouldBeTrue)
		}
	})
}

func TestRange_Overlap(t *testing.T) {
	convey.Convey("overlap",t, func() {
		type args struct {
			a Range
			b Range
			want bool
		}
		a := gen("a","")
		d := gen("d","")
		ac := gen("a","c")
		bd := gen("b","d")
		ca := gen("c","a")
		db := gen("d","b")

		kases := []args{
			{a,a,true},
			{a,d,false},
			{a,bd,false},
			{bd,a,false},
			{d,bd,false},
			{bd,d,false},
			{a,ac,true},
			{ac,a,true},
			{ac,ac,true},
			{ac,bd,true},
			{bd,ac,true},
			{ac,db,false},
			{db,ac,false},
			{bd,ca,false},
			{ca,bd,false},
		}
		for _, kase := range kases {
			ret := kase.a.Overlap(kase.b)
			convey.So(ret,convey.ShouldEqual,kase.want)
		}
	})
}

func TestRange_Intersect(t *testing.T) {
	convey.Convey("intersect",t, func() {
		type args struct {
			a Range
			b Range
			want Range
		}
		a := gen("a","")
		d := gen("d","")
		ac := gen("a","c")
		ad := gen("a","d")
		bc := gen("b","c")
		bd := gen("b","d")
		cd := gen("c","d")
		ca := gen("c","a")
		db := gen("d","b")
		empty := Range{}

		kases := []args{
			{a,a,a},
			{a,ac,a},
			{ac,a,a},
			{ac,ac,ac},

			{ac,ad,ac},
			{ad,ac,ac},
			{ac,bc,bc},
			{bc,ac,bc},

			{ac,bd,bc},
			{bd,ac,bc},
			{ad,bc,bc},
			{bc,ad,bc},

			{a,d,empty},
			{a,bd,empty},
			{bd,a,empty},
			{d,bd,empty},

			{bd,d,empty},
			{ac,cd,empty},
			{cd,ac,empty},
			{ac,db,empty},

			{db,ac,empty},
			{bd,ca,empty},
			{ca,bd,empty},
		}

		for _, kase := range kases {
			ret := kase.a.Intersect(kase.b)
			convey.So(reflect.DeepEqual(ret,kase.want),convey.ShouldBeTrue)
		}
	})
}

func TestRange_ContainRange(t *testing.T) {
	convey.Convey("contain range",t, func() {
		rg := gen("a","b")

		type args struct {
			a Range
			want bool
		}

		a := gen("a","")
		ab := gen("a","b")
		aa := gen("aa","")
		aa_b := gen("aa","b")
		aa_bb := gen("aa","bb")
		a_aa := gen("a","aa")
		x := gen("x","")
		xa := gen("x","a")
		b := gen("b","")
		ba := gen("b","a")
		bb := gen("b","bb")
		c := gen("c","")
		c0 := gen("0","9")
		cb := gen("0","b")
		cbb := gen("0","bb")

		kases := []args{
			{a,true},
			{aa,true},
			{x,false},
			{b,false},
			{c,false},
			{ab,true},
			{a_aa,true},
			{aa_b,true},
			{c0,false},
			{xa,false},
			{bb,false},
			{cb,false},
			{cbb,false},
			{aa_bb,false},
			{ba,false},
		}

		for _, kase := range kases {
			ret := rg.ContainRange(kase.a)
			convey.So(ret,convey.ShouldEqual,kase.want)
		}
	})
}