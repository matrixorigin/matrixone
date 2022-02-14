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
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestTupleKeyEncoder_EncodeTenantPrefix(t *testing.T) {
	convey.Convey("encode tenant prefix",t, func() {
		convey.So(func() {
			_ = NewTupleKeyEncoder(0)
		},convey.ShouldPanic)
	})

	convey.Convey("encode tenant prefix -- nil",t, func() {
		tke := NewTupleKeyEncoder(SystemTenantID)
		convey.So(tke.GetTenantPrefix(),convey.ShouldBeNil)
	})

	convey.Convey("encode tenant prefix -- not nil",t, func() {
		tke := NewTupleKeyEncoder(2)

		t,_ := tke.oe.EncodeUint64(nil,2)
		convey.So(tke.GetTenantPrefix()[0],convey.ShouldEqual,t[0])
	})
}

func TestTupleKeyEncoder_EncodeDatabasePrefix(t *testing.T) {

	convey.Convey("encode database",t, func() {
		tke := NewTupleKeyEncoder(SystemTenantID)

		for _, kase := range kases {
			tk,_ := tke.EncodeDatabasePrefix(nil,kase.value)
			convey.So(bytes.Equal(tk,kase.want),should.BeTrue)
		}
	})

	convey.Convey("encode database",t, func() {
		tke := NewTupleKeyEncoder(2)
		prefix,_ := tke.oe.EncodeUint64(nil,2)

		for _, kase := range kases {
			tk,_ := tke.EncodeDatabasePrefix(nil,kase.value)
			t := append(prefix,kase.want...)
			convey.So(bytes.Equal(tk,t),should.BeTrue)
		}
	})
}

func TestTupleKeyEncoder_EncodeTablePrefix(t *testing.T) {
	type args struct {
		value uint64
		want []byte
	}

	convey.Convey("encode table",t, func() {
		tke := NewTupleKeyEncoder(SystemTenantID)
		prefix,_ := tke.oe.EncodeUint64(nil,2)

		for _, kase := range kases {
			tk,_ := tke.EncodeTablePrefix(nil,2,kase.value)
			t := append(prefix,kase.want...)
			convey.So(bytes.Equal(tk,t),should.BeTrue)
		}
	})

	convey.Convey("encode table",t, func() {
		tke := NewTupleKeyEncoder(2)
		te,_ := tke.oe.EncodeUint64(nil,2)
		de,_ := tke.oe.EncodeUint64(nil,3)
		prefix := append(te,de...)

		for _, kase := range kases {
			tk,_ := tke.EncodeTablePrefix(nil,3,kase.value)
			t := append(prefix,kase.want...)
			convey.So(bytes.Equal(tk,t),should.BeTrue)
		}
	})
}

func TestTupleKeyEncoder_EncodeIndexPrefix(t *testing.T) {
	type args struct {
		value uint64
		want []byte
	}

	convey.Convey("encode index",t, func() {
		tke := NewTupleKeyEncoder(SystemTenantID)
		de,_ := tke.oe.EncodeUint64(nil,3)
		te,_ := tke.oe.EncodeUint64(nil,4)
		prefix := append(de,te...)

		for _, kase := range kases {
			tk,_ := tke.EncodeIndexPrefix(nil,3,4,kase.value)
			t := append(prefix,kase.want...)
			convey.So(bytes.Equal(tk,t),should.BeTrue)
		}
	})

	convey.Convey("encode table",t, func() {
		tke := NewTupleKeyEncoder(2)
		te,_ := tke.oe.EncodeUint64(nil,2)
		de,_ := tke.oe.EncodeUint64(nil,3)
		tae,_ := tke.oe.EncodeUint64(nil,4)
		prefix := append(te,de...)
		prefix = append(prefix,tae...)

		for _, kase := range kases {
			tk,_ := tke.EncodeIndexPrefix(nil,3,4,kase.value)
			t := append(prefix,kase.want...)
			convey.So(bytes.Equal(tk,t),should.BeTrue)
		}
	})
}