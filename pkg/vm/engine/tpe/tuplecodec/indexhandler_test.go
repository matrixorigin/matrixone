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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestIndexHandlerImpl_WriteIntoIndex(t *testing.T) {
	convey.Convey("write into index",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}

		ihi := &IndexHandlerImpl{
			tch:        tch,
			dbDesc:     InternalDatabaseDesc,
			kv:         kv,
			serializer: serial,
		}

		names,attrs := MakeAttributes(types.T_uint64,types.T_uint64,types.T_char,types.T_char)

		cnt := 10

		bat := MakeBatch(cnt,names,attrs)

		lines := randomLines(cnt,names,attrs)

		fillBatch(lines,bat)

		writeStates := make([]AttributeStateForWrite,4)
		for i, attrDesc := range InternalDescriptorTableDesc.Attributes {
			writeStates[i].AttrDesc = attrDesc
			writeStates[i].NeedGenerated = false
			writeStates[i].PositionInBatch = i
		}

		writeCtx := &WriteContext{
			DbDesc:          InternalDatabaseDesc,
			TableDesc:       InternalDescriptorTableDesc,
			IndexDesc:       &InternalDescriptorTableDesc.Primary_index,
			AttributeStates: writeStates,
			BatchAttrs:      InternalDescriptorTableDesc.Attributes,
			callback:        callbackPackage{},
			NodeID:          0,
		}
		err := ihi.WriteIntoIndex(writeCtx, bat)
		convey.So(err,convey.ShouldBeNil)

		err = ihi.WriteIntoTable(InternalDescriptorTableDesc, writeCtx, bat)
		convey.So(err,convey.ShouldBeError)
	})
}

func TestIndexHandlerImpl_ReadFromIndex(t *testing.T) {
	convey.Convey("read from index",t, func() {
		//make table
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}

		ihi := &IndexHandlerImpl{
			tch:        tch,
			dbDesc:     InternalDatabaseDesc,
			kv:         kv,
			kvLimit:    uint64(10),
			serializer: serial,
			rcc:        &RowColumnConverterImpl{},
		}

		names,attrs := MakeAttributes(types.T_uint64,types.T_uint64,types.T_char,types.T_char)

		cnt := 10

		bat := MakeBatch(cnt,names,attrs)

		lines := randomLines(cnt,names,attrs)

		fillBatch(lines,bat)

		writeStates := make([]AttributeStateForWrite,4)
		for i, attrDesc := range InternalDescriptorTableDesc.Attributes {
			writeStates[i].AttrDesc = attrDesc
			writeStates[i].NeedGenerated = false
			writeStates[i].PositionInBatch = i
		}

		writeCtx := &WriteContext{
			DbDesc:          InternalDatabaseDesc,
			TableDesc:       InternalDescriptorTableDesc,
			IndexDesc:       &InternalDescriptorTableDesc.Primary_index,
			AttributeStates: writeStates,
			BatchAttrs:      InternalDescriptorTableDesc.Attributes,
			callback:        callbackPackage{},
			NodeID:          0,
		}

		err := ihi.WriteIntoIndex(writeCtx, bat)
		convey.So(err,convey.ShouldBeNil)

		wantAttr := []*descriptor.AttributeDesc{
			&InternalDescriptorTableDesc.Attributes[InternalDescriptorTable_parentID_ID],
			&InternalDescriptorTableDesc.Attributes[InternalDescriptorTable_id_ID],
			&InternalDescriptorTableDesc.Attributes[InternalDescriptorTable_desc_ID],
		}

		readCtx := &ReadContext{
			DbDesc:                   InternalDatabaseDesc,
			TableDesc:                InternalDescriptorTableDesc,
			IndexDesc:                &InternalDescriptorTableDesc.Primary_index,
			ReadAttributeDescs:       wantAttr,
			SingleReaderContext:SingleReaderContext{
				CompleteInAllShards:      false,
				PrefixForScanKey:         nil,
				LengthOfPrefixForScanKey: 0,
			},
		}
		//var bat2 *batch.Batch
		var readcnt int

		for  {
			//read table
			_, readcnt, err = ihi.ReadFromIndex(readCtx)
			convey.So(err,convey.ShouldBeNil)
			if readcnt == 0 {//done
				break
			}
		}
	})
}