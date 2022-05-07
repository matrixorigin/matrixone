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
	"encoding/binary"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	mock_tuplecodec "github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec/test"
	"github.com/smartystreets/goconvey/convey"
)

type args struct {
	value uint64
	want []byte
}

var kases = []args{
	{0, []byte{136}},
	{1, []byte{137}},
	{109, []byte{245}},
	{109+1, []byte{246,110}},
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

func TestTupleKeyDecoder_SkipTenantPrefix(t *testing.T) {
	convey.Convey("skip tenant",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		//tke := tch.GetEncoder()
		tkd := tch.GetDecoder()

		r,err := tkd.SkipTenantPrefix(nil)
		convey.So(err,convey.ShouldBeNil)
		convey.So(r,convey.ShouldBeNil)
	})

	convey.Convey("skip tenant 2",t, func() {
		tch := NewTupleCodecHandler(2)
		tke := tch.GetEncoder()
		tkd := tch.GetDecoder()

		tkb,err := tke.EncodeTenantPrefix(nil,2)
		convey.So(err,convey.ShouldBeNil)

		r,err2 := tkd.SkipTenantPrefix(tkb)
		convey.So(err2,convey.ShouldBeNil)
		convey.So(r,convey.ShouldBeEmpty)

		tkb2,err := tke.EncodeTenantPrefix(nil,3)
		convey.So(err,convey.ShouldBeNil)

		r2,err3 := tkd.SkipTenantPrefix(tkb2)
		convey.So(err3,convey.ShouldBeError)
		convey.So(r2,convey.ShouldBeNil)
	})
}

func TestTupleKeyDecoder_DecodeDatabasePrefix(t *testing.T) {
	convey.Convey("decode database",t, func() {
		tch := NewTupleCodecHandler(2)
		tke := tch.GetEncoder()
		tkd := tch.GetDecoder()

		for _, kase := range kases {
			tkbt,err := tke.EncodeDatabasePrefix(nil,kase.value)
			convey.So(err,convey.ShouldBeNil)

			tk,di,err2 := tkd.DecodeDatabasePrefix(tkbt)
			convey.So(err2,convey.ShouldBeNil)
			convey.So(tk,convey.ShouldBeEmpty)
			convey.So(di.Value.(uint64),convey.ShouldEqual,kase.value)
		}
	})
}

func TestTupleKeyDecoder_DecodeTablePrefix(t *testing.T) {
	convey.Convey("decode table",t, func() {
		tch := NewTupleCodecHandler(2)
		tke := tch.GetEncoder()
		tkd := tch.GetDecoder()

		for _, kase := range kases {
			tkbt, err := tke.EncodeTablePrefix(nil, kase.value, kase.value)
			convey.So(err, convey.ShouldBeNil)

			tk, dis, err2 := tkd.DecodeTablePrefix(tkbt)
			convey.So(err2, convey.ShouldBeNil)
			convey.So(tk, convey.ShouldBeEmpty)
			convey.So(dis[0].Value.(uint64), convey.ShouldEqual, kase.value)
			convey.So(dis[1].Value.(uint64), convey.ShouldEqual, kase.value)
		}
	})
}

func TestTupleKeyDecoder_DecodeIndexPrefix(t *testing.T) {
	convey.Convey("decode index",t, func() {
		tch := NewTupleCodecHandler(2)
		tke := tch.GetEncoder()
		tkd := tch.GetDecoder()

		for _, kase := range kases {
			tkbt, err := tke.EncodeIndexPrefix(nil, kase.value,kase.value,kase.value)
			convey.So(err, convey.ShouldBeNil)

			tk, dis, err2 := tkd.DecodeIndexPrefix(tkbt)
			convey.So(err2, convey.ShouldBeNil)
			convey.So(tk, convey.ShouldBeEmpty)
			convey.So(dis[0].Value.(uint64), convey.ShouldEqual, kase.value)
			convey.So(dis[1].Value.(uint64), convey.ShouldEqual, kase.value)
			convey.So(dis[2].Value.(uint64), convey.ShouldEqual, kase.value)
		}
	})
}

func TestTupleKeyDecoder_DecodePrimaryIndexKey(t *testing.T) {
	type args struct {
		id uint32
		value interface{}
		valueType orderedcodec.ValueType
		want []byte
	}

	convey.Convey("decode primary index key 1",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		tke := tch.GetEncoder()
		tkd := tch.GetDecoder()

		kases := []args{
			{0,uint64(0),orderedcodec.VALUE_TYPE_UINT64,[]byte{136}},
			{1,"abc",orderedcodec.VALUE_TYPE_STRING,[]byte{18,'a','b','c',0,1,}},
			{2,[]byte{1,2,3},orderedcodec.VALUE_TYPE_BYTES,[]byte{18,1,2,3,0,1,}},
		}

		id := descriptor.IndexDesc{ID: PrimaryIndexID}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tuple := mock_tuplecodec.NewMockTuple(ctrl)
		want := []byte{}
		for _, kase := range kases {
			id.Attributes = append(id.Attributes,
				descriptor.IndexDesc_Attribute{ID: kase.id,Type: kase.valueType})
			tuple.EXPECT().GetValue(uint32(kase.id)).Return(kase.value,nil)
			want = append(want,kase.want...)
		}

		key, _, err := tke.EncodePrimaryIndexKey(nil,&id,0,tuple)
		convey.So(err,convey.ShouldBeNil)
		convey.So(bytes.Equal(key,want),convey.ShouldBeTrue)

		rest, dis, err := tkd.DecodePrimaryIndexKey(key,&id)
		convey.So(err,convey.ShouldBeNil)
		convey.So(rest,convey.ShouldBeEmpty)
		for i, kase := range kases {
			convey.So(reflect.DeepEqual(dis[i].Value,kase.value),convey.ShouldBeTrue)
		}
	})
}

func TestTupleKeyDecoder_DecodePrimaryIndexValue(t *testing.T) {
	type args struct {
		id uint32
		value interface{}
		valueType byte
		want []byte
	}
	convey.Convey("primary index value 1",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		tke := tch.GetEncoder()
		tkd := tch.GetDecoder()

		kases := []args{
			{0,uint64(0),SERIAL_TYPE_UINT64,[]byte{}},
			{1,"abc",SERIAL_TYPE_STRING,[]byte{}},
			{2,[]byte{1,2,3},SERIAL_TYPE_BYTES,[]byte{}},
			{3,nil,SERIAL_TYPE_NULL,[]byte{}},
		}

		id := descriptor.IndexDesc{ID: PrimaryIndexID}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tuple := mock_tuplecodec.NewMockTuple(ctrl)
		serial := &DefaultValueSerializer{}

		tuple.EXPECT().GetAttributeCount().Return(uint32(len(kases)),nil)
		want := []byte{}
		for _, kase := range kases {
			id.Attributes = append(id.Attributes,
				descriptor.IndexDesc_Attribute{ID: kase.id})
			tuple.EXPECT().GetValue(uint32(kase.id)).Return(kase.value,nil)
			m,err :=json.Marshal(kase.value)
			convey.So(err,convey.ShouldBeNil)

			//encode data len
			var lbuf [binary.MaxVarintLen64]byte
			bytesWritten := binary.PutVarint(lbuf[:], int64(len(m)))
			want = append(want,kase.valueType)
			want = append(want,lbuf[:bytesWritten]...)
			want = append(want,m...)
		}

		key, _, err := tke.EncodePrimaryIndexValue(nil,&id,0,tuple,serial)
		convey.So(err,convey.ShouldBeNil)
		convey.So(bytes.Equal(key,want),convey.ShouldBeTrue)

		rest, dis, err := tkd.DecodePrimaryIndexValue(key,&id,0,serial)
		convey.So(err,convey.ShouldBeNil)
		convey.So(rest,convey.ShouldBeEmpty)
		for i, kase := range kases {
			convey.So(reflect.DeepEqual(dis[i].Value,kase.value),convey.ShouldBeTrue)
		}
	})
}