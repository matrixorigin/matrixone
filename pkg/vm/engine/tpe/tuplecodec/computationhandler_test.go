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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestComputationHandlerImpl_CreateDatabase(t *testing.T) {
	convey.Convey("create database",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)
		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)
		chi := NewComputationHandlerImpl(dhi, kv, tch, &DefaultValueSerializer{}, nil)

		for i := 0; i < 20; i++ {
			dbName := fmt.Sprintf("test%d",i)
			dbID, err := chi.CreateDatabase(0,dbName,0)
			convey.So(err,convey.ShouldBeNil)

			desc, err := dhi.LoadDatabaseDescByID(dbID)
			convey.So(err,convey.ShouldBeNil)
			convey.So(desc.ID,convey.ShouldEqual,dbID)
			convey.So(desc.Name,convey.ShouldEqual,dbName)

			_, err = chi.CreateDatabase(0,dbName,0)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestComputationHandlerImpl_CreateTable(t *testing.T) {
	convey.Convey("create table",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)
		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)
		chi := NewComputationHandlerImpl(dhi, kv, tch, &DefaultValueSerializer{}, nil)

		dbID, err := chi.CreateDatabase(0,"test",0)
		convey.So(err,convey.ShouldBeNil)

		for i := 0; i < 10; i++ {
			table := &descriptor.RelationDesc{}
			*table = *internalDescriptorTableDesc

			tableName := fmt.Sprintf("A%d",i)
			table.Name = tableName
			tableID, err := chi.CreateTable(0,dbID,table)
			convey.So(err,convey.ShouldBeNil)

			get, err := chi.dh.LoadRelationDescByID(dbID,tableID)
			convey.So(err,convey.ShouldBeNil)
			convey.So(get.Name,convey.ShouldEqual,tableName)
			convey.So(get.ID,convey.ShouldEqual,tableID)

			_, err = chi.CreateTable(0,dbID,table)
			convey.So(err,convey.ShouldBeError)
		}

	})
}

func TestComputationHandlerImpl_DropTable(t *testing.T) {
	convey.Convey("drop table",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)
		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)
		chi := NewComputationHandlerImpl(dhi, kv, tch, &DefaultValueSerializer{}, nil)

		dbID, err := chi.CreateDatabase(0,"test",0)
		convey.So(err,convey.ShouldBeNil)

		for i := 0; i < 10; i++ {
			table := &descriptor.RelationDesc{}
			*table = *internalDescriptorTableDesc

			tableName := fmt.Sprintf("A%d",i)
			table.Name = tableName
			tableID, err := chi.CreateTable(0,dbID,table)
			convey.So(err,convey.ShouldBeNil)

			get, err := chi.dh.LoadRelationDescByID(dbID,tableID)
			convey.So(err,convey.ShouldBeNil)
			convey.So(get.Name,convey.ShouldEqual,tableName)
			convey.So(get.ID,convey.ShouldEqual,tableID)

			_, err = chi.DropTable(0,dbID,tableName)
			convey.So(err,convey.ShouldBeNil)

			_, err = chi.DropTable(0,dbID,tableName)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestComputationHandlerImpl_ListTables(t *testing.T) {
	convey.Convey("list tables",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)
		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)
		chi := NewComputationHandlerImpl(dhi, kv, tch, &DefaultValueSerializer{}, nil)

		dbID, err := chi.CreateDatabase(0,"test",0)
		convey.So(err,convey.ShouldBeNil)

		var tables []*descriptor.RelationDesc
		for i := 0; i < 10; i++ {
			table := &descriptor.RelationDesc{}
			*table = *internalDescriptorTableDesc

			tableName := fmt.Sprintf("A%d",i)
			table.Name = tableName

			tables = append(tables,table)

			tableID, err := chi.CreateTable(0,dbID,table)
			convey.So(err,convey.ShouldBeNil)

			get, err := chi.dh.LoadRelationDescByID(dbID,tableID)
			convey.So(err,convey.ShouldBeNil)
			convey.So(get.Name,convey.ShouldEqual,tableName)
			convey.So(get.ID,convey.ShouldEqual,tableID)

		}

		wantTables, err := chi.ListTables(dbID)
		convey.So(err,convey.ShouldBeNil)

		for i := 0; i < 10; i++ {
			convey.So(reflect.DeepEqual(*wantTables[i],*tables[i]),convey.ShouldBeTrue)
		}

		for i := 0; i < 10; i++ {
			tableName := fmt.Sprintf("A%d",i)

			if i & 2 == 0 {
				_, err = chi.DropTable(0,dbID,tableName)
				convey.So(err,convey.ShouldBeNil)
			}
		}

		wantTables, err = chi.ListTables(dbID)
		convey.So(err,convey.ShouldBeNil)

		for i := 0; i < 10; i++ {
			tableName := fmt.Sprintf("A%d",i)

			get, err := chi.dh.LoadRelationDescByName(dbID,tableName)
			if i & 2 == 0 {
				convey.So(err,convey.ShouldBeError)
			}else{
				convey.So(err,convey.ShouldBeNil)
				convey.So(get.Name,convey.ShouldEqual,tableName)
			}
		}
	})
}

func TestComputationHandlerImpl_DropDatabase(t *testing.T) {
	convey.Convey("drop database",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)
		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)
		chi := NewComputationHandlerImpl(dhi, kv, tch, &DefaultValueSerializer{}, nil)

		var dbIDs []uint64
		for i := 0; i < 3; i++ {
			dbName := fmt.Sprintf("test%d",i)

			dbID, err := chi.CreateDatabase(0,dbName,0)
			convey.So(err,convey.ShouldBeNil)

			dbIDs = append(dbIDs,dbID)

			var tables []*descriptor.RelationDesc
			for j := 0; j < 10; j++ {
				table := &descriptor.RelationDesc{}
				*table = *internalDescriptorTableDesc

				tableName := fmt.Sprintf("A%d",j)
				table.Name = tableName

				tables = append(tables,table)

				tableID, err := chi.CreateTable(0,dbID,table)
				convey.So(err,convey.ShouldBeNil)

				get, err := chi.dh.LoadRelationDescByID(dbID,tableID)
				convey.So(err,convey.ShouldBeNil)
				convey.So(get.Name,convey.ShouldEqual,tableName)
				convey.So(get.ID,convey.ShouldEqual,tableID)
			}

			wantTables, err := chi.ListTables(dbID)
			convey.So(err,convey.ShouldBeNil)

			for j := 0; j < 10; j++ {
				convey.So(reflect.DeepEqual(*wantTables[j],*tables[j]),convey.ShouldBeTrue)
			}
		}

		for i := 0; i < 3; i++ {
			dbName := fmt.Sprintf("test%d",i)

			err := chi.DropDatabase(0, dbName)
			convey.So(err,convey.ShouldBeNil)

			_, err = chi.dh.LoadDatabaseDescByID(dbIDs[i])
			convey.So(err,convey.ShouldBeError)

			_,err = chi.ListTables(dbIDs[i])
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestComputationHandlerImpl_GetDatabase(t *testing.T) {
	convey.Convey("get database",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)
		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)
		chi := NewComputationHandlerImpl(dhi, kv, tch, &DefaultValueSerializer{}, nil)

		var dbIDs []uint64
		for i := 0; i < 3; i++ {
			dbName := fmt.Sprintf("test%d",i)

			dbID, err := chi.CreateDatabase(0,dbName,0)
			convey.So(err,convey.ShouldBeNil)

			dbIDs = append(dbIDs,dbID)

			for j := 0; j < 10; j++ {
				table := &descriptor.RelationDesc{}
				*table = *internalDescriptorTableDesc

				tableName := fmt.Sprintf("A%d",j)
				table.Name = tableName

				tableID, err := chi.CreateTable(0,dbID,table)
				convey.So(err,convey.ShouldBeNil)

				get, err := chi.dh.LoadRelationDescByID(dbID,tableID)
				convey.So(err,convey.ShouldBeNil)
				convey.So(get.Name,convey.ShouldEqual,tableName)
				convey.So(get.ID,convey.ShouldEqual,tableID)
			}

			dbDesc, err := chi.GetDatabase(dbName)
			convey.So(err,convey.ShouldBeNil)
			convey.So(dbDesc.ID,convey.ShouldEqual,dbID)
		}

		for i := 0; i < 3; i++ {
			dbName := fmt.Sprintf("test%d",i)

			err := chi.DropDatabase(0, dbName)
			convey.So(err,convey.ShouldBeNil)

			_,err = chi.ListTables(dbIDs[i])
			convey.So(err,convey.ShouldBeError)

			_, err = chi.GetDatabase(dbName)
			convey.So(err,convey.ShouldBeError)
		}
	})
}

func TestComputationHandlerImpl_ListDatabases(t *testing.T) {
	convey.Convey("list databases",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)
		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)
		chi := NewComputationHandlerImpl(dhi, kv, tch, &DefaultValueSerializer{}, nil)

		var dbIDs []uint64
		for i := 0; i < 10; i++ {
			dbName := fmt.Sprintf("test%d",i)

			dbID, err := chi.CreateDatabase(0,dbName,0)
			convey.So(err,convey.ShouldBeNil)

			dbIDs = append(dbIDs,dbID)

			for j := 0; j < 10; j++ {
				table := &descriptor.RelationDesc{}
				*table = *internalDescriptorTableDesc

				tableName := fmt.Sprintf("A%d",j)
				table.Name = tableName

				tableID, err := chi.CreateTable(0,dbID,table)
				convey.So(err,convey.ShouldBeNil)

				get, err := chi.dh.LoadRelationDescByID(dbID,tableID)
				convey.So(err,convey.ShouldBeNil)
				convey.So(get.Name,convey.ShouldEqual,tableName)
				convey.So(get.ID,convey.ShouldEqual,tableID)
			}

			dbDesc, err := chi.GetDatabase(dbName)
			convey.So(err,convey.ShouldBeNil)
			convey.So(dbDesc.ID,convey.ShouldEqual,dbID)
		}

		for i := 0; i < 10; i++ {
			dbName := fmt.Sprintf("test%d",i)

			if i % 2 == 0{
				err := chi.DropDatabase(0, dbName)
				convey.So(err,convey.ShouldBeNil)
			}
		}

		dbDescs, err := chi.ListDatabases()
		convey.So(err,convey.ShouldBeNil)
		for _, desc := range dbDescs {
			convey.So(desc.Is_deleted,convey.ShouldBeFalse)
		}
	})
}

func TestComputationHandlerImpl_GetTable(t *testing.T) {
	convey.Convey("get table",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)
		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)
		chi := NewComputationHandlerImpl(dhi, kv, tch, &DefaultValueSerializer{}, nil)

		dbID, err := chi.CreateDatabase(0,"test",0)
		convey.So(err,convey.ShouldBeNil)

		for i := 0; i < 10; i++ {
			table := &descriptor.RelationDesc{}
			*table = *internalDescriptorTableDesc

			tableName := fmt.Sprintf("A%d",i)
			table.Name = tableName

			_, err := chi.CreateTable(0,dbID,table)
			convey.So(err,convey.ShouldBeNil)

			getTable, err := chi.GetTable(dbID, tableName)
			convey.So(err,convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(*table,*getTable),convey.ShouldBeTrue)

		}

		for i := 0; i < 10; i++ {
			tableName := fmt.Sprintf("A%d",i)

			if i & 2 == 0 {
				_, err = chi.DropTable(0,dbID,tableName)
				convey.So(err,convey.ShouldBeNil)
			}
		}

		for i := 0; i < 10; i++ {
			tableName := fmt.Sprintf("A%d",i)

			_, err := chi.GetTable(dbID, tableName)
			if i & 2 == 0 {
				convey.So(err,convey.ShouldBeError)
			}else{
				convey.So(err,convey.ShouldBeNil)
			}
		}
	})
}