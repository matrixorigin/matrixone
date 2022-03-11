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
	"bytes"
	"fmt"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestMemoryKV_NextID(t *testing.T) {
	convey.Convey("next id",t, func() {
		kv := NewMemoryKV()

		typ := "xxxxx"
		for i := 0; i < 100; i++ {
			id, err := kv.NextID(typ)
			convey.So(err,convey.ShouldBeNil)
			convey.So(id,convey.ShouldEqual,uint64(i)+UserTableIDOffset)
		}
	})
}

func TestMemoryKV_Set(t *testing.T) {
	convey.Convey("set",t, func() {
		kv := NewMemoryKV()

		type args struct {
			key   TupleKey
			value TupleValue
		}

		genkv := func(a,b string) args {
			return args{
				TupleKey(a),
				TupleValue(b),
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
			key   TupleKey
			value TupleValue
		}

		genkv := func(a,b string) args {
			return args{
				TupleKey(a),
				TupleValue(b),
			}
		}

		kases := []args{
			genkv("a","b"),
			genkv("b","b"),
			genkv("c","b"),
			genkv("c","c"),
		}

		want := []TupleValue{
			genkv("a","b").value,
			genkv("b","b").value,
			genkv("c","c").value,
			genkv("c","c").value,
		}

		var keys []TupleKey
		var values []TupleValue
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
			key   TupleKey
			value TupleValue
			want  bool
		}

		genkv := func(a,b string,c bool) args {
			return args{
				TupleKey(a),
				TupleValue(b),
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
			key   TupleKey
			value TupleValue
			want  bool
		}

		genkv := func(a,b string,w bool) args {
			return args{
				TupleKey(a),
				TupleValue(b),
				w,
			}
		}

		kases := []args{
			genkv("a","b",true),
			genkv("b","b",true),
			genkv("c","b",true),
			genkv("c","c",false),
		}

		want := []TupleValue{
			genkv("a","b",true).value,
			genkv("b","b",true).value,
			genkv("c","b",true).value,
			genkv("c","b",true).value,
		}

		var keys []TupleKey
		var values []TupleValue
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
			key   TupleKey
			value TupleValue
		}

		genkv := func(a,b string) args {
			return args{
				TupleKey(a),
				TupleValue(b),
			}
		}

		kases := []args{
			genkv("a","a"),
			genkv("b","b"),
			genkv("c","c"),
		}

		var keys []TupleKey
		var values []TupleValue
		for _, kase := range kases {
			keys = append(keys,kase.key)
			values = append(values,kase.value)
		}

		errs := kv.SetBatch(keys, values)
		for _, err := range errs {
			convey.So(err,convey.ShouldBeNil)
		}

		type args2 struct {
			start TupleKey
			end   TupleKey
			want  []TupleValue
		}

		gen := func(a,b string,w ...TupleValue) args2 {
			return args2{
				start: TupleKey(a),
				end:   TupleKey(b),
				want:  w,
			}
		}

		kases2 := []args2{
			gen("a","c",
				TupleValue("a"),
				TupleValue("b"),
				TupleValue("c")),
			gen("a","b",
				TupleValue("a"),
				TupleValue("b")),
			gen("a","a"),
			gen("b","c",
				TupleValue("b"),
				TupleValue("c")),
			gen("b","e",
				TupleValue("b"),
				TupleValue("c")),
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
	convey.Convey("get range",t, func() {
		prefix := "abc"
		cnt := 20

		kv := NewMemoryKV()

		type args struct {
			key   TupleKey
			value TupleValue
		}

		var kases []args
		for i := 0 ; i < cnt; i++ {
			key := TupleKey(prefix + fmt.Sprintf("%20d",i))
			value := TupleValue(fmt.Sprintf("v%d",i))

			kases = append(kases,args{
				key:   key,
				value: value,
			})
			err := kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		_, values, _, _, err := kv.GetRangeWithLimit(TupleKey(prefix), nil, uint64(cnt))
		convey.So(err,convey.ShouldBeNil)

		for i, kase := range kases {
			convey.So(values[i],convey.ShouldResemble,kase.value)
		}

		step := 10
		last := TupleKey(prefix)
		for i := 0; i < cnt; i += step {
			keys, values, _, _, err := kv.GetRangeWithLimit(last, nil, uint64(step))
			convey.So(err,convey.ShouldBeNil)

			for j := i; j < i+step; j++ {
				convey.So(values[j - i],convey.ShouldResemble,kases[j].value)
			}

			last = SuccessorOfKey(keys[len(keys) - 1])
		}
	})
}

func TestMemoryKV_GetWithPrefix(t *testing.T) {
	convey.Convey("get with prefix",t, func() {
		prefix := "abc"
		cnt := 20

		kv := NewMemoryKV()

		type args struct {
			key   TupleKey
			value TupleValue
		}

		var kases []args
		for i := 0 ; i < cnt; i++ {
			key := TupleKey(prefix + fmt.Sprintf("%20d",i))
			value := TupleValue(fmt.Sprintf("v%d",i))

			kases = append(kases,args{
				key:   key,
				value: value,
			})
			err := kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		_, values, _, _, err := kv.GetWithPrefix(TupleKey(prefix), len(prefix), uint64(cnt))
		convey.So(err,convey.ShouldBeNil)

		for i, kase := range kases {
			convey.So(values[i],convey.ShouldResemble,kase.value)
		}

		step := 10
		last := TupleKey(prefix)
		prefixLen := len(prefix)
		for i := 0; i < cnt; i += step {
			keys, values, _, _, err := kv.GetWithPrefix(last, prefixLen , uint64(step))
			convey.So(err,convey.ShouldBeNil)

			for j := i; j < i+step; j++ {
				convey.So(values[j - i],convey.ShouldResemble,kases[j].value)
			}

			last = SuccessorOfKey(keys[len(keys) - 1])
		}
	})
}

func TestMemoryKV_DeletePrefix(t *testing.T) {
	convey.Convey("delete prefix",t, func() {
		kv := NewMemoryKV()
		cnt := 10

		genData := func(cnt int,handler KVHandler,prefix string) ([]TupleKey,[]TupleValue) {
			var keys []TupleKey
			var values []TupleValue
			for i := 0; i < cnt; i++ {
				key := fmt.Sprintf("%s%d",prefix,i)
				value := fmt.Sprintf("v%d",i)
				keys = append(keys,[]byte(key))
				values = append(values,[]byte(value))
				err := handler.Set([]byte(key), []byte(value))
				convey.So(err,convey.ShouldBeNil)
			}

			return keys, values
		}

		wKeys1, wValues1 := genData(cnt,kv,"abc")
		err := kv.SetBatch(wKeys1,wValues1)
		for _, e := range err {
			convey.So(e,convey.ShouldBeNil)
		}

		wKeys2, wValues2 := genData(cnt,kv,"cde")
		err = kv.SetBatch(wKeys2,wValues2)
		for _, e := range err {
			convey.So(e,convey.ShouldBeNil)
		}

		err2 := kv.DeleteWithPrefix([]byte("abc"))
		convey.So(err2,convey.ShouldBeNil)

		resKeys, resValues, _, _, err3 := kv.GetWithPrefix(TupleKey("cde"), len("cde"), uint64(cnt))
		convey.So(err3,convey.ShouldBeNil)

		for i, key := range resKeys {
			for j, wKey := range wKeys2 {
				if bytes.Equal(key,wKey) {
					convey.So(resValues[i],
						convey.ShouldResemble,wValues2[j])
				}else {
					break
				}
			}
		}
	})
}