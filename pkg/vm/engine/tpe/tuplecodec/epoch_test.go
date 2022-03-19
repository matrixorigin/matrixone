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
	"testing"
)

func TestEpochHandler_SetEpoch(t *testing.T) {
	convey.Convey("set/get",t, func() {
		eh := NewEpochHandler(nil,nil,nil)

		for i := 0; i < 100; i++ {
			eh.SetEpoch(uint64(i))
			convey.So(uint64(i),convey.ShouldEqual, eh.GetEpoch())
		}
	})
}

func TestEpochHandler_RemoveDeletedTable(t *testing.T) {
	convey.Convey("remove deleted table",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}
		kvLimit := uint64(2)

		dhi := NewDescriptorHandlerImpl(tch,kv,serial,kvLimit)

		eh := NewEpochHandler(tch,dhi,kv)

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
			for dbId := UserTableIDOffset + offset + uint64(0); dbId < UserTableIDOffset + offset + cnt; dbId++ {
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

		gcCnt, err := eh.RemoveDeletedTable(cnt / 2 - 1)
		convey.So(err,convey.ShouldBeNil)
		convey.So(uint64(gcCnt),convey.ShouldEqual,cnt*cnt*cnt / 2)

		gcItems, err = dhi.ListRelationDescFromAsyncGC(cnt)
		convey.So(err,convey.ShouldBeNil)
		convey.So(len(gcItems),convey.ShouldEqual,cnt*cnt*cnt / 2)
	})
}