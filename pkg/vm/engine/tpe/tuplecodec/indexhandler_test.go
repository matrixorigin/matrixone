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
	"testing"
)

func TestIndexHandlerImpl_WriteIntoIndex(t *testing.T) {
	/*
	TODO:to fix
	convey.Convey("write into index",t, func() {
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}

		ihi := &IndexHandlerImpl{
			tch:        tch,
			dbDesc:     internalDatabaseDesc,
			kv:         kv,
			serializer: serial,
		}

		names,attrs := MakeAttributes(types.T_uint64,types.T_uint64,types.T_char,types.T_char)

		cnt := 10

		bat := MakeBatch(cnt,names,attrs)

		lines := randomLines(cnt,names,attrs)

		fillBatch(lines,bat)


		writeCtx := &WriteContext{
			AttributeStates: []AttributeStateForWrite{

			},
			BatchAttrs:      nil,
			callback:        callbackPackage{},
			NodeID:          0,
		}
		err := ihi.WriteIntoIndex(internalDatabaseDesc, internalDescriptorTableDesc, &internalDescriptorTableDesc.Primary_index, nil, writeCtx, bat)
		convey.So(err,convey.ShouldBeNil)

		err = ihi.WriteIntoTable(internalDescriptorTableDesc, bat)
		convey.So(err,convey.ShouldBeNil)
	})
	*/
}

func TestIndexHandlerImpl_ReadFromIndex(t *testing.T) {
	/*
	TODO:to fix
	convey.Convey("read from index",t, func() {
		//make table
		tch := NewTupleCodecHandler(SystemTenantID)
		kv := NewMemoryKV()
		serial := &DefaultValueSerializer{}

		ihi := &IndexHandlerImpl{
			tch:        tch,
			dbDesc:     internalDatabaseDesc,
			kv:         kv,
			kvLimit: uint64(10),
			serializer: serial,
			rcc: &RowColumnConverterImpl{},
		}

		names,attrs := MakeAttributes(types.T_uint64,types.T_uint64,types.T_char,types.T_char)

		cnt := 10

		bat := MakeBatch(cnt,names,attrs)

		lines := randomLines(cnt,names,attrs)

		fillBatch(lines,bat)

		err := ihi.WriteIntoIndex(internalDatabaseDesc, internalDescriptorTableDesc, &internalDescriptorTableDesc.Primary_index, nil, nil, bat)
		convey.So(err,convey.ShouldBeNil)

		wantAttr := []*descriptor.AttributeDesc{
			&internalDescriptorTableDesc.Attributes[InternalDescriptorTableID_parentID_ID],
			&internalDescriptorTableDesc.Attributes[InternalDescriptorTableID_id_ID],
			&internalDescriptorTableDesc.Attributes[InternalDescriptorTableID_desc_ID],
		}

		var prefix []byte
		var prefixLen int
		//var bat2 *batch.Batch
		var readcnt int

		for  {
			//read table
			_, readcnt, prefix, prefixLen, err = ihi.ReadFromIndex(internalDatabaseDesc,
				internalDescriptorTableDesc,
				&internalDescriptorTableDesc.Primary_index,
				wantAttr,prefix,prefixLen)
			convey.So(err,convey.ShouldBeNil)
			if readcnt == 0 {//done
				break
			}
		}
	})
	*/
}