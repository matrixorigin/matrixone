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
	"encoding/json"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	mock_tuplecodec "github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec/test"
	"github.com/smartystreets/goconvey/convey"
	"math"
	"reflect"
	"testing"
)

func TestDescriptorHandlerImpl_LoadRelationDescByName(t *testing.T) {
	convey.Convey("load table desc by name",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		//save test data
		var prefix []byte
		prefix,_ = tch.tke.EncodeIndexPrefix(prefix,
			InternalDatabaseID,
			InternalDescriptorTableID,
			uint64(PrimaryIndexID))

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func(a uint64,b uint64,c string,d []byte) Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(4),nil)
			tuple.EXPECT().GetValue(uint32(0)).Return(a,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(1)).Return(b,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(2)).Return(c,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(3)).Return(d,nil).AnyTimes()
			return tuple
		}

		type args struct {
			parentID uint64
			id uint64
			name string
			desc *descriptor.RelationDesc
		}

		make_tableDesc := func(desc *descriptor.RelationDesc,id uint32,name string) *descriptor.RelationDesc {
			d := new(descriptor.RelationDesc)
			*d = *desc
			d.ID = id
			d.Name = name
			return d
		}

		make_args := func(cnt int,base int) []args {
			var kases []args
			for i := 0; i < cnt; i++ {
				id := base + i + 1
				name := fmt.Sprintf("test%d",id)
				kases = append(kases,args{
					parentID: InternalDatabaseID,
					id:       uint64(id),
					name:     name,
					desc:     make_tableDesc(InternalDescriptorTableDesc,uint32(id),name),
				})
			}
			return kases
		}

		kases := make_args(10,0)

		for _, kase := range kases {
			descBytes, err := json.Marshal(*kase.desc)
			convey.So(err,convey.ShouldBeNil)

			tmpPrefix := make([]byte,len(prefix))
			copy(tmpPrefix,prefix)

			tuple := make_tuple(kase.parentID,kase.id,kase.name,descBytes)
			key, _, err := tch.tke.EncodePrimaryIndexKey(tmpPrefix,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
			)
			convey.So(err,convey.ShouldBeNil)

			value, _, err := tch.tke.EncodePrimaryIndexValue(nil,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
				serial)
			convey.So(err,convey.ShouldBeNil)

			err = kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		kv.PrintKeys()

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		for _, kase := range kases {
			desc, err := dhi.LoadRelationDescByName(InternalDatabaseID,kase.name)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)

			_, err = dhi.LoadRelationDescByName(InternalDatabaseID+1,kase.name)
			convey.So(err,convey.ShouldBeError)
		}

		notFoundKases := make_args(10,20)
		for _, kase := range notFoundKases {
			_, err := dhi.LoadRelationDescByName(InternalDatabaseID,kase.name)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestDescriptorHandlerImpl_LoadRelationDescByID(t *testing.T) {
	convey.Convey("load table desc by id",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		//save test data
		var prefix []byte
		prefix,_ = tch.tke.EncodeIndexPrefix(prefix,
			InternalDatabaseID,
			InternalDescriptorTableID,
			uint64(PrimaryIndexID))

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func(a uint64,b uint64,c string,d []byte) Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(4),nil)
			tuple.EXPECT().GetValue(uint32(0)).Return(a,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(1)).Return(b,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(2)).Return(c,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(3)).Return(d,nil).AnyTimes()
			return tuple
		}

		type args struct {
			parentID uint64
			id uint64
			name string
			desc *descriptor.RelationDesc
		}

		make_tableDesc := func(desc *descriptor.RelationDesc,id uint32,name string) *descriptor.RelationDesc {
			d := new(descriptor.RelationDesc)
			*d = *desc
			d.ID = id
			d.Name = name
			return d
		}

		make_args := func(cnt int,base int) []args {
			var kases []args
			for i := 0; i < cnt; i++ {
				id := base + i + 1
				name := fmt.Sprintf("test%d",id)
				kases = append(kases,args{
					parentID: InternalDatabaseID,
					id:       uint64(id),
					name:     name,
					desc:     make_tableDesc(InternalDescriptorTableDesc,uint32(id),name),
				})
			}
			return kases
		}

		kases := make_args(10,0)

		for _, kase := range kases {
			descBytes, err := json.Marshal(*kase.desc)
			convey.So(err,convey.ShouldBeNil)

			tmpPrefix := make([]byte,len(prefix))
			copy(tmpPrefix,prefix)

			tuple := make_tuple(kase.parentID,kase.id,kase.name,descBytes)
			key, _, err := tch.tke.EncodePrimaryIndexKey(tmpPrefix,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
			)
			convey.So(err,convey.ShouldBeNil)

			value, _, err := tch.tke.EncodePrimaryIndexValue(nil,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
				serial)
			convey.So(err,convey.ShouldBeNil)

			err = kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		kv.PrintKeys()

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		for _, kase := range kases {
			desc, err := dhi.LoadRelationDescByID(InternalDatabaseID,kase.id)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)

			_, err = dhi.LoadRelationDescByID(InternalDatabaseID+1,kase.id)
			convey.So(err,convey.ShouldBeError)
		}

		notFoundKases := make_args(10,20)
		for _, kase := range notFoundKases {
			_, err := dhi.LoadRelationDescByID(InternalDatabaseID,kase.id)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestDescriptorHandlerImpl_StoreRelationDescByName(t *testing.T) {
	convey.Convey("save table desc by name",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		//save test data
		var prefix []byte
		prefix,_ = tch.tke.EncodeIndexPrefix(prefix,
			InternalDatabaseID,
			InternalDescriptorTableID,
			uint64(PrimaryIndexID))

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func(a uint64,b uint64,c string,d []byte) Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(4),nil)
			tuple.EXPECT().GetValue(uint32(0)).Return(a,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(1)).Return(b,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(2)).Return(c,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(3)).Return(d,nil).AnyTimes()
			return tuple
		}

		type args struct {
			parentID uint64
			id uint64
			name string
			desc *descriptor.RelationDesc
		}

		make_tableDesc := func(desc *descriptor.RelationDesc,id uint32,name string) *descriptor.RelationDesc {
			d := new(descriptor.RelationDesc)
			*d = *desc
			d.ID = id
			d.Name = name
			return d
		}

		make_args := func(cnt int,base int) []args {
			var kases []args
			for i := 0; i < cnt; i++ {
				id := base + i + 1
				name := fmt.Sprintf("test%d",id)
				kases = append(kases,args{
					parentID: InternalDatabaseID,
					id:       uint64(id),
					name:     name,
					desc:     make_tableDesc(InternalDescriptorTableDesc,uint32(id),name),
				})
			}
			return kases
		}

		kases := make_args(10,0)

		for _, kase := range kases {
			descBytes, err := json.Marshal(*kase.desc)
			convey.So(err,convey.ShouldBeNil)

			tmpPrefix := make([]byte,len(prefix))
			copy(tmpPrefix,prefix)

			tuple := make_tuple(kase.parentID,kase.id,kase.name,descBytes)
			key, _, err := tch.tke.EncodePrimaryIndexKey(tmpPrefix,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
			)
			convey.So(err,convey.ShouldBeNil)

			value, _, err := tch.tke.EncodePrimaryIndexValue(nil,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
				serial)
			convey.So(err,convey.ShouldBeNil)

			err = kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		kv.PrintKeys()

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		for _, kase := range kases {
			err := dhi.StoreRelationDescByName(InternalDatabaseID,kase.name,kase.desc)
			convey.So(err,convey.ShouldBeNil)

			desc, err := dhi.LoadRelationDescByName(InternalDatabaseID,kase.name)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)
		}

		notFoundKases := make_args(10,20)
		for _, kase := range notFoundKases {
			err := dhi.StoreRelationDescByName(InternalDatabaseID,kase.name,kase.desc)
			convey.So(err,convey.ShouldBeNil)

			desc, err := dhi.LoadRelationDescByName(InternalDatabaseID,kase.name)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)
		}
	})
}

func TestDescriptorHandlerImpl_StoreRelationDescByID(t *testing.T) {
	convey.Convey("save table desc by id",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		//save test data
		var prefix []byte
		prefix,_ = tch.tke.EncodeIndexPrefix(prefix,
			InternalDatabaseID,
			InternalDescriptorTableID,
			uint64(PrimaryIndexID))

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func(a uint64,b uint64,c string,d []byte) Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(4),nil)
			tuple.EXPECT().GetValue(uint32(0)).Return(a,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(1)).Return(b,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(2)).Return(c,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(3)).Return(d,nil).AnyTimes()
			return tuple
		}

		type args struct {
			parentID uint64
			id uint64
			name string
			desc *descriptor.RelationDesc
		}

		make_tableDesc := func(desc *descriptor.RelationDesc,id uint32,name string) *descriptor.RelationDesc {
			d := new(descriptor.RelationDesc)
			*d = *desc
			d.ID = id
			d.Name = name
			return d
		}

		make_args := func(cnt int,base int) []args {
			var kases []args
			for i := 0; i < cnt; i++ {
				id := base + i + 1
				name := fmt.Sprintf("test%d",id)
				kases = append(kases,args{
					parentID: InternalDatabaseID,
					id:       uint64(id),
					name:     name,
					desc:     make_tableDesc(InternalDescriptorTableDesc,uint32(id),name),
				})
			}
			return kases
		}

		kases := make_args(10,0)

		for _, kase := range kases {
			descBytes, err := json.Marshal(*kase.desc)
			convey.So(err,convey.ShouldBeNil)

			tmpPrefix := make([]byte,len(prefix))
			copy(tmpPrefix,prefix)

			tuple := make_tuple(kase.parentID,kase.id,kase.name,descBytes)
			key, _, err := tch.tke.EncodePrimaryIndexKey(tmpPrefix,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
			)
			convey.So(err,convey.ShouldBeNil)

			value, _, err := tch.tke.EncodePrimaryIndexValue(nil,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
				serial)
			convey.So(err,convey.ShouldBeNil)

			err = kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		kv.PrintKeys()

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		for _, kase := range kases {
			err := dhi.StoreRelationDescByID(InternalDatabaseID,kase.id,kase.desc)
			convey.So(err,convey.ShouldBeNil)

			desc, err := dhi.LoadRelationDescByID(InternalDatabaseID,kase.id)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)
		}

		notFoundKases := make_args(10,20)
		for _, kase := range notFoundKases {
			err := dhi.StoreRelationDescByID(InternalDatabaseID,kase.id,kase.desc)
			convey.So(err,convey.ShouldBeNil)

			desc, err := dhi.LoadRelationDescByID(InternalDatabaseID,kase.id)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)
		}
	})
}

func TestDescriptorHandlerImpl_LoadDatabaseDescByName(t *testing.T) {
	convey.Convey("load database desc by name",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		//save test data
		var prefix []byte
		prefix,_ = tch.tke.EncodeIndexPrefix(prefix,
			InternalDatabaseID,
			InternalDescriptorTableID,
			uint64(PrimaryIndexID))

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func(a uint64,b uint64,c string,d []byte) Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(4),nil)
			tuple.EXPECT().GetValue(uint32(0)).Return(a,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(1)).Return(b,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(2)).Return(c,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(3)).Return(d,nil).AnyTimes()
			return tuple
		}

		type args struct {
			parentID uint64
			id uint64
			name string
			desc *descriptor.DatabaseDesc
		}

		make_dbDesc := func(desc *descriptor.DatabaseDesc,id uint32,name string) *descriptor.DatabaseDesc {
			d := new(descriptor.DatabaseDesc)
			*d = *desc
			d.ID = id
			d.Name = name
			return d
		}

		make_args := func(cnt int,base int) []args {
			var kases []args
			for i := 0; i < cnt; i++ {
				id := base + i + 1
				name := fmt.Sprintf("database%d",id)
				kases = append(kases,args{
					parentID: math.MaxUint64,
					id:       uint64(id),
					name:     name,
					desc:     make_dbDesc(InternalDatabaseDesc,uint32(id),name),
				})
			}
			return kases
		}

		kases := make_args(10,0)

		for _, kase := range kases {
			descBytes, err := json.Marshal(*kase.desc)
			convey.So(err,convey.ShouldBeNil)

			tmpPrefix := make([]byte,len(prefix))
			copy(tmpPrefix,prefix)

			tuple := make_tuple(kase.parentID,kase.id,kase.name,descBytes)
			key, _, err := tch.tke.EncodePrimaryIndexKey(tmpPrefix,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
			)
			convey.So(err,convey.ShouldBeNil)

			value, _, err := tch.tke.EncodePrimaryIndexValue(nil,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
				serial)
			convey.So(err,convey.ShouldBeNil)

			err = kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		kv.PrintKeys()

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		for _, kase := range kases {
			desc, err := dhi.LoadDatabaseDescByName(kase.name)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)

			_, err = dhi.LoadDatabaseDescByName("err"+kase.name)
			convey.So(err,convey.ShouldBeError)
		}

		notFoundKases := make_args(10,20)
		for _, kase := range notFoundKases {
			_, err := dhi.LoadDatabaseDescByName(kase.name)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestDescriptorHandlerImpl_LoadDatabaseDescByID(t *testing.T) {
	convey.Convey("load database desc by id",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		//save test data
		var prefix []byte
		prefix,_ = tch.tke.EncodeIndexPrefix(prefix,
			InternalDatabaseID,
			InternalDescriptorTableID,
			uint64(PrimaryIndexID))

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func(a uint64,b uint64,c string,d []byte) Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(4),nil)
			tuple.EXPECT().GetValue(uint32(0)).Return(a,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(1)).Return(b,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(2)).Return(c,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(3)).Return(d,nil).AnyTimes()
			return tuple
		}

		type args struct {
			parentID uint64
			id uint64
			name string
			desc *descriptor.DatabaseDesc
		}

		make_dbDesc := func(desc *descriptor.DatabaseDesc,id uint32,name string) *descriptor.DatabaseDesc {
			d := new(descriptor.DatabaseDesc)
			*d = *desc
			d.ID = id
			d.Name = name
			return d
		}

		make_args := func(cnt int,base int) []args {
			var kases []args
			for i := 0; i < cnt; i++ {
				id := base + i + 1
				name := fmt.Sprintf("database%d",id)
				kases = append(kases,args{
					parentID: math.MaxUint64,
					id:       uint64(id),
					name:     name,
					desc:     make_dbDesc(InternalDatabaseDesc,uint32(id),name),
				})
			}
			return kases
		}

		kases := make_args(10,0)

		for _, kase := range kases {
			descBytes, err := json.Marshal(*kase.desc)
			convey.So(err,convey.ShouldBeNil)

			tmpPrefix := make([]byte,len(prefix))
			copy(tmpPrefix,prefix)

			tuple := make_tuple(kase.parentID,kase.id,kase.name,descBytes)
			key, _, err := tch.tke.EncodePrimaryIndexKey(tmpPrefix,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
			)
			convey.So(err,convey.ShouldBeNil)

			value, _, err := tch.tke.EncodePrimaryIndexValue(nil,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
				serial)
			convey.So(err,convey.ShouldBeNil)

			err = kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		kv.PrintKeys()

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		for _, kase := range kases {
			desc, err := dhi.LoadDatabaseDescByID(kase.id)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)

			_, err = dhi.LoadDatabaseDescByID(kase.id*100)
			convey.So(err,convey.ShouldBeError)
		}

		notFoundKases := make_args(10,20)
		for _, kase := range notFoundKases {
			_, err := dhi.LoadDatabaseDescByID(kase.id)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestDescriptorHandlerImpl_StoreDatabaseDescByName(t *testing.T) {
	convey.Convey("load database desc by name",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		//save test data
		var prefix []byte
		prefix,_ = tch.tke.EncodeIndexPrefix(prefix,
			InternalDatabaseID,
			InternalDescriptorTableID,
			uint64(PrimaryIndexID))

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func(a uint64,b uint64,c string,d []byte) Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(4),nil)
			tuple.EXPECT().GetValue(uint32(0)).Return(a,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(1)).Return(b,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(2)).Return(c,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(3)).Return(d,nil).AnyTimes()
			return tuple
		}

		type args struct {
			parentID uint64
			id uint64
			name string
			desc *descriptor.DatabaseDesc
		}

		make_dbDesc := func(desc *descriptor.DatabaseDesc,id uint32,name string) *descriptor.DatabaseDesc {
			d := new(descriptor.DatabaseDesc)
			*d = *desc
			d.ID = id
			d.Name = name
			return d
		}

		make_args := func(cnt int,base int) []args {
			var kases []args
			for i := 0; i < cnt; i++ {
				id := base + i + 1
				name := fmt.Sprintf("database%d",id)
				kases = append(kases,args{
					parentID: math.MaxUint64,
					id:       uint64(id),
					name:     name,
					desc:     make_dbDesc(InternalDatabaseDesc,uint32(id),name),
				})
			}
			return kases
		}

		kases := make_args(10,0)

		for _, kase := range kases {
			descBytes, err := json.Marshal(*kase.desc)
			convey.So(err,convey.ShouldBeNil)

			tmpPrefix := make([]byte,len(prefix))
			copy(tmpPrefix,prefix)

			tuple := make_tuple(kase.parentID,kase.id,kase.name,descBytes)
			key, _, err := tch.tke.EncodePrimaryIndexKey(tmpPrefix,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
			)
			convey.So(err,convey.ShouldBeNil)

			value, _, err := tch.tke.EncodePrimaryIndexValue(nil,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
				serial)
			convey.So(err,convey.ShouldBeNil)

			err = kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		kv.PrintKeys()

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		for _, kase := range kases {
			err := dhi.StoreDatabaseDescByName(kase.name,kase.desc)
			convey.So(err,convey.ShouldBeNil)

			desc, err := dhi.LoadDatabaseDescByName(kase.name)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)

			err = dhi.StoreDatabaseDescByName("err"+kase.name,kase.desc)
			convey.So(err,convey.ShouldBeNil)
		}

		notFoundKases := make_args(10,20)
		for _, kase := range notFoundKases {
			_, err := dhi.LoadDatabaseDescByName(kase.name)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestDescriptorHandlerImpl_StoreDatabaseDescByID(t *testing.T) {
	convey.Convey("load database desc by id",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		//save test data
		var prefix []byte
		prefix,_ = tch.tke.EncodeIndexPrefix(prefix,
			InternalDatabaseID,
			InternalDescriptorTableID,
			uint64(PrimaryIndexID))

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func(a uint64,b uint64,c string,d []byte) Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(4),nil)
			tuple.EXPECT().GetValue(uint32(0)).Return(a,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(1)).Return(b,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(2)).Return(c,nil).AnyTimes()
			tuple.EXPECT().GetValue(uint32(3)).Return(d,nil).AnyTimes()
			return tuple
		}

		type args struct {
			parentID uint64
			id uint64
			name string
			desc *descriptor.DatabaseDesc
		}

		make_dbDesc := func(desc *descriptor.DatabaseDesc,id uint32,name string) *descriptor.DatabaseDesc {
			d := new(descriptor.DatabaseDesc)
			*d = *desc
			d.ID = id
			d.Name = name
			return d
		}

		make_args := func(cnt int,base int) []args {
			var kases []args
			for i := 0; i < cnt; i++ {
				id := base + i + 1
				name := fmt.Sprintf("database%d",id)
				kases = append(kases,args{
					parentID: math.MaxUint64,
					id:       uint64(id),
					name:     name,
					desc:     make_dbDesc(InternalDatabaseDesc,uint32(id),name),
				})
			}
			return kases
		}

		kases := make_args(10,0)

		for _, kase := range kases {
			descBytes, err := json.Marshal(*kase.desc)
			convey.So(err,convey.ShouldBeNil)

			tmpPrefix := make([]byte,len(prefix))
			copy(tmpPrefix,prefix)

			tuple := make_tuple(kase.parentID,kase.id,kase.name,descBytes)
			key, _, err := tch.tke.EncodePrimaryIndexKey(tmpPrefix,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
			)
			convey.So(err,convey.ShouldBeNil)

			value, _, err := tch.tke.EncodePrimaryIndexValue(nil,
				&InternalDescriptorTableDesc.Primary_index,
				0,
				tuple,
				serial)
			convey.So(err,convey.ShouldBeNil)

			err = kv.Set(key, value)
			convey.So(err,convey.ShouldBeNil)
		}

		kv.PrintKeys()

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		for _, kase := range kases {
			err := dhi.StoreDatabaseDescByID(kase.id,kase.desc)
			convey.So(err,convey.ShouldBeNil)

			desc, err := dhi.LoadDatabaseDescByID(kase.id)
			convey.So(reflect.DeepEqual(*desc,*kase.desc),convey.ShouldBeTrue)
		}

		notFoundKases := make_args(10,20)
		for _, kase := range notFoundKases {
			_, err := dhi.LoadDatabaseDescByID(kase.id)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestDescriptorHandlerImpl_StoreRelationDescIntoAsyncGC(t *testing.T) {
	convey.Convey("store relation desc into asyncgc",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		make_relation_desc := func(from *descriptor.RelationDesc,name string,id uint32) *descriptor.RelationDesc {
			desc := new(descriptor.RelationDesc)
			*desc = *from
			desc.ID = id
			desc.Name = name
			return desc
		}

		var wantGCItems []descriptor.EpochGCItem

		cnt := uint64(2)

		for epoch := uint64(0); epoch < cnt; epoch++ {
			offset := 100 * epoch
			for dbId := offset + uint64(0); dbId < offset + cnt; dbId++ {
				for tableId := uint32(0); uint64(tableId) < cnt; tableId++ {
					wantGCItems = append(wantGCItems,descriptor.EpochGCItem{
						Epoch:   epoch,
						DbID:    dbId,
						TableID: uint64(tableId),
					})

					tableName := fmt.Sprintf("table%d",tableId)
					desc := make_relation_desc(InternalDescriptorTableDesc,tableName,tableId)
					err := dhi.StoreRelationDescIntoAsyncGC(epoch, dbId, desc)
					convey.So(err,convey.ShouldBeNil)
				}
			}
		}

		gcItems, err := dhi.ListRelationDescFromAsyncGC(cnt)
		convey.So(err,convey.ShouldBeNil)
		convey.So(len(gcItems),convey.ShouldEqual,len(wantGCItems))
		for i, item := range gcItems {
			convey.So(item,convey.ShouldResemble,wantGCItems[i])
		}
	})
}