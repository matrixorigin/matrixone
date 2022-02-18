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

package kv

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestMemoryKV_Set(t *testing.T) {
	convey.Convey("set",t, func() {
		kv := NewMemoryKV()

		type args struct {
			key tuplecodec.TupleKey
			value tuplecodec.TupleValue
		}

		genkv := func(a,b string) args {
			return args{
				tuplecodec.TupleKey(a),
				tuplecodec.TupleValue(b),
				}
		}

		kases := []args{
			genkv("a","b"),
			genkv("b","b"),
			genkv("c","b"),
			genkv("c","c"),
		}

		for _, kase := range kases {
			err := kv.Set(kase.key,kase.value)
			convey.So(err,convey.ShouldBeNil)

			want, err := kv.Get(kase.key)
			convey.So(err,convey.ShouldBeNil)
			convey.So(want,convey.ShouldResemble,kase.value)
		}
	})
}

func TestMemoryKV_SetBatch(t *testing.T) {
	convey.Convey("set batch",t, func() {
		kv := NewMemoryKV()

		type args struct {
			key tuplecodec.TupleKey
			value tuplecodec.TupleValue
		}

		genkv := func(a,b string) args {
			return args{
				tuplecodec.TupleKey(a),
				tuplecodec.TupleValue(b),
			}
		}

		kases := []args{
			genkv("a","b"),
			genkv("b","b"),
			genkv("c","b"),
			genkv("c","c"),
		}

		want := []tuplecodec.TupleValue {
			genkv("a","b").value,
			genkv("b","b").value,
			genkv("c","c").value,
			genkv("c","c").value,
		}

		var keys []tuplecodec.TupleKey
		var values []tuplecodec.TupleValue
		for _, kase := range kases {
			keys = append(keys,kase.key)
			values = append(values,kase.value)
		}

		errs := kv.SetBatch(keys, values)
		for _, err := range errs {
			convey.So(err,convey.ShouldBeNil)
		}

		gets, err2 := kv.GetBatch(keys)
		convey.So(err2,convey.ShouldBeNil)
		convey.So(len(gets),convey.ShouldEqual,len(want))

		for i, get := range gets {
			convey.So(get,convey.ShouldResemble,want[i])
		}
	})
}

func TestMemoryKV_DedupSet(t *testing.T) {
	convey.Convey("dedup set",t, func() {
		kv := NewMemoryKV()

		type args struct {
			key tuplecodec.TupleKey
			value tuplecodec.TupleValue
			want bool
		}

		genkv := func(a,b string,c bool) args {
			return args{
				tuplecodec.TupleKey(a),
				tuplecodec.TupleValue(b),
				c,
			}
		}

		kases := []args{
			genkv("a","b",true),
			genkv("b","b",true),
			genkv("c","b",true),
			genkv("c","c",false),
		}

		for _, kase := range kases {
			err := kv.DedupSet(kase.key,kase.value)
			if kase.want {
				convey.So(err,convey.ShouldBeNil)

				want, err := kv.Get(kase.key)
				convey.So(err,convey.ShouldBeNil)
				convey.So(want,convey.ShouldResemble,kase.value)
			}else{
				convey.So(err,convey.ShouldBeError)
			}
		}
	})
}

func TestMemoryKV_DedupSetBatch(t *testing.T) {
	convey.Convey("dedup set batch",t, func() {
		kv := NewMemoryKV()

		type args struct {
			key tuplecodec.TupleKey
			value tuplecodec.TupleValue
			want bool
		}

		genkv := func(a,b string,w bool) args {
			return args{
				tuplecodec.TupleKey(a),
				tuplecodec.TupleValue(b),
				w,
			}
		}

		kases := []args{
			genkv("a","b",true),
			genkv("b","b",true),
			genkv("c","b",true),
			genkv("c","c",false),
		}

		want := []tuplecodec.TupleValue {
			genkv("a","b",true).value,
			genkv("b","b",true).value,
			genkv("c","b",true).value,
			genkv("c","b",true).value,
		}

		var keys []tuplecodec.TupleKey
		var values []tuplecodec.TupleValue
		for _, kase := range kases {
			keys = append(keys,kase.key)
			values = append(values,kase.value)
		}

		errs := kv.DedupSetBatch(keys, values)
		for i, err := range errs {
			if kases[i].want {
				convey.So(err,convey.ShouldBeNil)
			}else{
				convey.So(err,convey.ShouldBeError)
			}
		}

		gets, err2 := kv.GetBatch(keys)
		convey.So(err2,convey.ShouldBeNil)
		convey.So(len(gets),convey.ShouldEqual,len(want))

		for i, get := range gets {
			convey.So(get,convey.ShouldResemble,want[i])
		}
	})
}

func TestMemoryKV_GetRange(t *testing.T) {
	convey.Convey("get range",t, func() {
		kv := NewMemoryKV()

		type args struct {
			key tuplecodec.TupleKey
			value tuplecodec.TupleValue
		}

		genkv := func(a,b string) args {
			return args{
				tuplecodec.TupleKey(a),
				tuplecodec.TupleValue(b),
			}
		}

		kases := []args{
			genkv("a","a"),
			genkv("b","b"),
			genkv("c","c"),
		}

		var keys []tuplecodec.TupleKey
		var values []tuplecodec.TupleValue
		for _, kase := range kases {
			keys = append(keys,kase.key)
			values = append(values,kase.value)
		}

		errs := kv.SetBatch(keys, values)
		for _, err := range errs {
			convey.So(err,convey.ShouldBeNil)
		}

		type args2 struct {
			start tuplecodec.TupleKey
			end tuplecodec.TupleKey
			want []tuplecodec.TupleValue
		}

		gen := func(a,b string,w ...tuplecodec.TupleValue) args2 {
			return args2{
				start: tuplecodec.TupleKey(a),
				end:   tuplecodec.TupleKey(b),
				want:  w,
			}
		}

		kases2 := []args2{
			gen("a","c",
				tuplecodec.TupleValue("a"),
				tuplecodec.TupleValue("b"),
				tuplecodec.TupleValue("c")),
			gen("a","b",
				tuplecodec.TupleValue("a"),
				tuplecodec.TupleValue("b")),
			gen("a","a"),
			gen("b","c",
				tuplecodec.TupleValue("b"),
				tuplecodec.TupleValue("c")),
			gen("b","e",
				tuplecodec.TupleValue("b"),
				tuplecodec.TupleValue("c")),
		}

		for _, k2 := range kases2 {
			values, err := kv.GetRange(k2.start, k2.end)
			convey.So(err,convey.ShouldBeNil)
			if len(k2.want) == 0 {
				convey.So(values,convey.ShouldBeEmpty)
			}else{
				for i := 0; i < len(values); i++ {
					convey.So(reflect.DeepEqual(values[i],k2.want[i]),
						convey.ShouldBeTrue)
				}
			}
		}
	})
}

func TestMemoryKV_GetRangeWithLimit(t *testing.T) {
	convey.Convey("get with prefix",t, func() {
		prefix := "abc"
		cnt := 20

		kv := NewMemoryKV()

		type args struct {
			key tuplecodec.TupleKey
			value tuplecodec.TupleValue
		}

		var kases []args
		for i := 0 ; i < cnt; i++ {
			key := tuplecodec.TupleKey(prefix + fmt.Sprintf("%20d",i))
			value := tuplecodec.TupleValue(fmt.Sprintf("v%d",i))

			kases = append(kases,args{
				key:   key,
				value: value,
			})
			err := kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		_, values, err := kv.GetRangeWithLimit(tuplecodec.TupleKey(prefix),uint64(cnt))
		convey.So(err,convey.ShouldBeNil)

		for i, kase := range kases {
			convey.So(values[i],convey.ShouldResemble,kase.value)
		}

		step := 10
		last := tuplecodec.TupleKey(prefix)
		for i := 0; i < cnt; i += step {
			keys, values, err := kv.GetRangeWithLimit(last, uint64(step))
			convey.So(err,convey.ShouldBeNil)

			for j := i; j < i+step; j++ {
				convey.So(values[j - i],convey.ShouldResemble,kases[j].value)
			}

			last = tuplecodec.SuccessorOfKey(keys[len(keys) - 1])
		}
	})
}

func TestMemoryKV_GetWithPrefix(t *testing.T) {
	convey.Convey("get with prefix",t, func() {
		prefix := "abc"
		cnt := 100

		kv := NewMemoryKV()

		type args struct {
			key tuplecodec.TupleKey
			value tuplecodec.TupleValue
		}

		var kases []args
		for i := 0 ; i < cnt; i++ {
			key := tuplecodec.TupleKey(prefix + fmt.Sprintf("%20d",i))
			value := tuplecodec.TupleValue(fmt.Sprintf("v%d",i))

			kases = append(kases,args{
				key:   key,
				value: value,
			})
			err := kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		_, values, err := kv.GetWithPrefix(tuplecodec.TupleKey(prefix),uint64(cnt))
		convey.So(err,convey.ShouldBeNil)

		for i, kase := range kases {
			convey.So(values[i],convey.ShouldResemble,kase.value)
		}

		step := 10
		last := tuplecodec.TupleKey(prefix)
		for i := 0; i < cnt; i += step {
			keys, values, err := kv.GetWithPrefix(last, uint64(step))
			convey.So(err,convey.ShouldBeNil)

			for j := i; j < i+step; j++ {
				convey.So(values[j - i],convey.ShouldResemble,kases[j].value)
			}

			last = tuplecodec.SuccessorOfKey(keys[len(keys) - 1])
		}
	})
}