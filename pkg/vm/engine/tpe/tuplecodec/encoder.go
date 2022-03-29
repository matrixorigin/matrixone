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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

var (
	errorWrongTenantID = errors.New("wrong tenant id")
	errorPrimaryIndexIDIsNotOne = errors.New("primary index id is not one")
	errorPrimaryIndexAttributesHaveNull  = errors.New("primary index attributes have null")
	errorUnknownValueType = errors.New("unknown value type")
	errorWrongValueType = errors.New("wrong value type")
	errorNoEnoughBytes = errors.New("there is no enough bytes")
	errorVarintOverflow = errors.New("varint is overflow")
	errorNotJsonNumber = errors.New("not json number")
)

func NewTupleKeyEncoder(tenantID uint64) *TupleKeyEncoder {
	oe := orderedcodec.NewOrderedEncoder()
	tke := &TupleKeyEncoder{oe : oe}
	tp,_ := tke.EncodeTenantPrefix(nil,tenantID)
	tke.oe = oe
	tke.tenantPrefix = &tp
	return tke
}

// EncodeTenantPrefix encodes the tenant prefix
func (tke *TupleKeyEncoder) EncodeTenantPrefix(prefix TupleKey, tenantID uint64) (TupleKey,*orderedcodec.EncodedItem) {
	if tenantID < SystemTenantID {
		panic(errorWrongTenantID)
	}
	if tenantID == SystemTenantID {
		return nil,nil
	}

	pb, _ := tke.oe.EncodeUint64(prefix,tenantID)
	return pb,nil
}

func (tke *TupleKeyEncoder) GetTenantPrefix() TupleKey {
	return *tke.tenantPrefix
}

// EncodeDatabasePrefix encodes the database prefix
func (tke *TupleKeyEncoder) EncodeDatabasePrefix(prefix TupleKey,dbID uint64) (TupleKey,*orderedcodec.EncodedItem) {
	pre := append(prefix,tke.GetTenantPrefix()...)
	dbPrefix,_ := tke.oe.EncodeUint64(pre,dbID)
	return dbPrefix,nil
}

// EncodeTablePrefix encodes the table prefix
func (tke *TupleKeyEncoder) EncodeTablePrefix(prefix TupleKey,dbID uint64,tableID uint64) (TupleKey,*orderedcodec.EncodedItem) {
	dbPre,_ := tke.EncodeDatabasePrefix(prefix,dbID)
	dbPrefix,_ := tke.oe.EncodeUint64(dbPre,tableID)
	return dbPrefix,nil
}

// EncodeIndexPrefix encodes the index prefix
func (tke *TupleKeyEncoder) EncodeIndexPrefix(prefix TupleKey,dbID uint64,tableID, indexID uint64) (TupleKey,*orderedcodec.EncodedItem) {
	tablePre,_ := tke.EncodeTablePrefix(prefix,dbID,tableID)
	indexPrefix,_ := tke.oe.EncodeUint64(tablePre,indexID)
	return indexPrefix,nil
}

//EncodePrimaryIndexKey encodes the tuple into bytes.
//The prefix has the tenantID,dbID,tableID,IndexID.
func (tke *TupleKeyEncoder) EncodePrimaryIndexKey(prefix TupleKey,
		index *descriptor.IndexDesc,
		columnGroupID uint64,
		tuple Tuple)(TupleKey, *orderedcodec.EncodedItem,error) {
	if index.ID != PrimaryIndexID {
		return nil,nil,errorPrimaryIndexIDIsNotOne
	}

	/*
	fields => bytes
	1. Get fields value from tuple
	2. Encoding fields
	 */
	//index attributes
	key := prefix
	for _, attr := range index.Attributes {
		value, err := tuple.GetValue(attr.ID)
		if err != nil {
			return nil, nil, err
		}

		//check if has the null
		if value == nil {
			return nil, nil, errorPrimaryIndexAttributesHaveNull
		}

		key,_ = tke.oe.EncodeKey(key,value)
	}
	return key, nil, nil
}

//EncodePrimaryIndexValue encodes the tuple into bytes
func (tke *TupleKeyEncoder) EncodePrimaryIndexValue(prefix TupleValue,
		index *descriptor.IndexDesc,
		columnGroupID uint64,
		tuple Tuple,
		serializer ValueSerializer)(TupleValue, *orderedcodec.EncodedItem,error) {
	if index.ID != PrimaryIndexID {
		return nil,nil,errorPrimaryIndexIDIsNotOne
	}
	//just encoding into the json
	cnt, err := tuple.GetAttributeCount()
	if err != nil {
		return nil, nil, err
	}
	out := prefix
	for i := uint32(0); i < cnt; i++ {
		value, err := tuple.GetValue(i)
		if err != nil {
			return nil, nil, err
		}
		//serial value
		serialized,_, err := serializer.SerializeValue(out,value)
		if err != nil {
			return nil, nil, err
		}
		out = serialized
	}
	return out, nil, nil
}