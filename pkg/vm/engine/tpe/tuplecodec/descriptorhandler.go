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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

var (
	errorTypeInValueNotEqualToTypeInAttribute = errors.New("the type in the value is not equal to the value in the attribute")
	errorDoNotFindTheDesc = errors.New("do not find the descriptor")
	errorDecodeDescriptorFailed = errors.New("decode the descriptor failed")
	errorDescriptorSavedIsNotTheWanted = errors.New("the descriptor saved is not the wanted one")
)
/*
Internal descriptor table for schema management.
Attribute  PrimaryKey
---------------------
parentID        Y
ID              Y
Name            N
DescriptorBytes N
 */

var _ descriptor.DescriptorHandler = &DescriptorHandlerImpl{}

type DescriptorHandlerImpl struct {
	codecHandler *TupleCodecHandler
	kvHandler    KVHandler
	serializer ValueSerializer
	kvLimit uint64
}

func NewDescriptorHandlerImpl(codec*TupleCodecHandler,
		kv KVHandler,
		vs ValueSerializer,
		limit uint64) *DescriptorHandlerImpl {
	return &DescriptorHandlerImpl{
		codecHandler: codec,
		kvHandler: kv,
		serializer: vs,
		kvLimit: limit,
	}
}

// decodeValue decodes the data into (parentID,ID,Name,Bytes)
func (dhi *DescriptorHandlerImpl) decodeValue(data []byte) ([]*orderedcodec.DecodedItem,error) {
	attrCnt := len(internalDescriptorTableDesc.Attributes)
	dis := make([]*orderedcodec.DecodedItem,0,attrCnt)
	for j := 0; j < attrCnt; j++ {
		rest, di, err := dhi.serializer.DeserializeValue(data)
		if err != nil {
			return nil, err
		}
		dis = append(dis,di)
		data = rest
	}
	return dis,nil
}

// unmarshalRelationDesc decodes the bytes into the relationDesc
func (dhi *DescriptorHandlerImpl) unmarshalRelationDesc(data []byte) (*descriptor.RelationDesc,error) {
	tableDesc := &descriptor.RelationDesc{}
	err := json.Unmarshal(data, tableDesc)
	if err != nil {
		return nil, errorDecodeDescriptorFailed
	}
	return tableDesc,nil
}

func (dhi *DescriptorHandlerImpl) makePrefixWithParentID(dbID uint64, tableID uint64, indexID uint64, parentID uint64) (TupleKey, *orderedcodec.EncodedItem) {
	tke := dhi.codecHandler.GetEncoder()

	//make prefix
	var prefix TupleKey
	// tenantID,dbID,tableID,indexID
	prefix,_ = tke.EncodeIndexPrefix(prefix,
		dbID,
		tableID,
		indexID)

	// append parentID
	prefix,_ = tke.oe.EncodeUint64(prefix,parentID)
	return prefix,nil
}

func (dhi *DescriptorHandlerImpl) makePrefixWithParentIDAndTableID(dbID uint64, tableID uint64, indexID uint64, parentID uint64,ID uint64)(TupleKey, *orderedcodec.EncodedItem){
	prefix,_ := dhi.makePrefixWithParentID(dbID,tableID,indexID,parentID)
	// append tableID
	prefix,_ = dhi.codecHandler.GetEncoder().oe.EncodeUint64(prefix,tableID)
	return prefix,nil
}

func (dhi *DescriptorHandlerImpl) LoadRelationDescByName(parentID uint64, name string) (*descriptor.RelationDesc, error) {
	/*
	1,make prefix (tenantID,dbID,tableID,indexID,parentID)
	2,get keys with the prefix
	3,decode the ID and the Name and find the desired name
	 */

	//make prefix
	var prefix TupleKey
	// append parentID
	prefix,_ = dhi.makePrefixWithParentID(InternalDatabaseID,
		InternalDescriptorTableID,
		uint64(PrimaryIndexID),
		parentID)

	//get keys with the prefix
	prefixLen := len(prefix)
	for {
		keys, values, err := dhi.kvHandler.GetWithPrefix(prefix,prefixLen,dhi.kvLimit)
		if err != nil {
			return nil, err
		}

		if len(keys) == 0 {
			break
		}

		for i := 0; i < len(keys); i++ {
			//decode the name which is in the value
			data := values[i]
			//decode the data into (parentID,ID,Name,Bytes)
			dis,err := dhi.decodeValue(data)
			if err != nil {
				return nil, err
			}

			//get the name and the desc
			nameAttr := internalDescriptorTableDesc.Attributes[InternalDescriptorTableID_name_ID]
			descAttr := internalDescriptorTableDesc.Attributes[InternalDescriptorTableID_desc_ID]
			nameDI := dis[InternalDescriptorTableID_name_ID]
			descDI := dis[InternalDescriptorTableID_desc_ID]
			if !(nameDI.IsValueType(nameAttr.Ttype) ||
					descDI.IsValueType(descAttr.Ttype)){
				return nil,errorTypeInValueNotEqualToTypeInAttribute
			}

			if nameInValue,ok := nameDI.Value.(string); ok {
				//check the name
				if name == nameInValue {//get it
					//deserialize the desc
					if bytesInValue,ok2 := descDI.Value.([]byte); ok2 {
						tableDesc,err := dhi.unmarshalRelationDesc(bytesInValue)
						if err != nil {
							return nil, err
						}
						return tableDesc,nil
					}
				}
			}
		}

		//get the next prefix
		prefix = SuccessorOfKey(keys[len(keys) - 1])
	}

	return nil, errorDoNotFindTheDesc
}

func (dhi *DescriptorHandlerImpl) LoadRelationDescByID(parentID uint64, tableID uint64) (*descriptor.RelationDesc, error) {
	/*
		1,make prefix (tenantID,dbID,tableID,indexID,parentID,tableID)
		2,get keys with the key
		3,decode the ID and the Name and find the desired name
	*/
	//make the prefix
	var key TupleKey
	key,_ = dhi.makePrefixWithParentIDAndTableID(InternalDatabaseID,
		InternalDescriptorTableID,
		uint64(PrimaryIndexID),
		parentID,
		tableID)

	value, err := dhi.kvHandler.Get(key)
	if err != nil {
		return nil, err
	}

	dis,err := dhi.decodeValue(value)
	if err != nil {
		return nil, err
	}
	descAttr := internalDescriptorTableDesc.Attributes[InternalDescriptorTableID_desc_ID]
	descDI := dis[InternalDescriptorTableID_desc_ID]
	if !descDI.IsValueType(descAttr.Ttype) {
		return nil,errorTypeInValueNotEqualToTypeInAttribute
	}

	//deserialize the desc
	if bytesInValue,ok := descDI.Value.([]byte); ok {
		tableDesc,err := dhi.unmarshalRelationDesc(bytesInValue)
		if err != nil {
			return nil, err
		}
		return tableDesc,nil
	}
	return nil,errorDoNotFindTheDesc
}

func (dhi *DescriptorHandlerImpl) StoreRelationDescByName(parentID uint64, name string, tableDesc *descriptor.RelationDesc) error {
	desc, err := dhi.LoadRelationDescByName(parentID,name)
	if err != nil {
		return err
	}
	if desc.Name != name ||
			desc.ID != tableDesc.ID ||
			desc.Name != tableDesc.Name {
		return errorDescriptorSavedIsNotTheWanted
	}
	//get the id, update it and save the descriptor
	//TODO:
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) StoreRelationDescByID(parentID uint64, tableID uint64, table *descriptor.RelationDesc) error {

	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) LoadDatabaseDescByName(parentID uint64, name string) (*descriptor.DatabaseDesc, error) {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) LoadDatabaseDescByID(dbID uint64) (*descriptor.DatabaseDesc, error) {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) StoreDatabaseDescByName(parentID uint64, name string, db *descriptor.DatabaseDesc) error {
	panic("implement me")
}

func (dhi *DescriptorHandlerImpl) StoreDatabaseDescByID(dbID uint64, db *descriptor.DatabaseDesc) error {
	panic("implement me")
}
