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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"sort"
)

var (
	errorRowIndexDifferentInKeyAndValue = errors.New("the rowIndexForkey != rowIndexForValue")
	errorWriteContextIsInvalid = errors.New("the write context is invalid")
)

var _ index.IndexHandler = &IndexHandlerImpl{}

type callbackPackage struct {
	index *descriptor.IndexDesc
	prefix TupleKey
}

type IndexHandlerImpl struct {
	tch *TupleCodecHandler
	dbDesc *descriptor.DatabaseDesc
	kv KVHandler
	kvLimit uint64
	serializer ValueSerializer
	rcc RowColumnConverter
}

func NewIndexHandlerImpl(tch *TupleCodecHandler,
		db *descriptor.DatabaseDesc,
		kv KVHandler,
		kvLimit uint64,
		serial ValueSerializer,
		rcc RowColumnConverter) *IndexHandlerImpl {
	return &IndexHandlerImpl{
		tch:        tch,
		dbDesc:     db,
		kv:         kv,
		kvLimit:    kvLimit,
		serializer: serial,
		rcc:        rcc,
	}
}

func (ihi * IndexHandlerImpl) ReadFromIndex(db *descriptor.DatabaseDesc, table *descriptor.RelationDesc, index *descriptor.IndexDesc, attrs []*descriptor.AttributeDesc, prefix []byte, prefixLen int) (*batch.Batch, int, []byte, int, error) {
	//check just need the index key
	indexAttrIDs := descriptor.ExtractIndexAttributeIDsSorted(index.Attributes)
	amForKey :=&AttributeMap{}
	amForValue := &AttributeMap{}
	justNeedKey := true
	for i,attr := range attrs {
		pos := sort.Search(len(indexAttrIDs), func(i int) bool {
			return indexAttrIDs[i] >= attr.ID
		})
		//id is not in the index key
		if pos >= len(indexAttrIDs) || indexAttrIDs[pos] != attr.ID {
			justNeedKey = false
			amForValue.Append(int(attr.ID),i)
		}else{
			//id in the key
			amForKey.Append(int(attr.ID),i)
		}
	}

	//1.encode prefix (tenantID,dbID,tableID,indexID)
	tke := ihi.tch.GetEncoder()
	tkd := ihi.tch.GetDecoder()

	if prefix == nil {
		prefix,_ = tke.EncodeIndexPrefix(prefix, uint64(db.ID), uint64(table.ID), uint64(index.ID))
		prefixLen = len(prefix)
	}

	//prepare the batch
	names,attrdefs := ConvertAttributeDescIntoTypesType(attrs)
	bat := MakeBatch(int(ihi.kvLimit),names,attrdefs)

	rowIndexForKey := 0
	rowIndexForValue := 0
	readFinished := false

	//2.prefix read data from kv
	//get keys with the prefix
	var lastKey []byte
	for {
		keys, values, err := ihi.kv.GetWithPrefix(prefix,prefixLen,ihi.kvLimit)
		if err != nil {
			return nil, 0, nil, 0, err
		}

		if len(keys) == 0 {
			readFinished = true
			break
		}

		//1.decode index key
		//2.get fields wanted
		for i := 0; i < len(keys); i++ {
			indexKey := keys[i][prefixLen:]
			_, dis, err := tkd.DecodePrimaryIndexKey(indexKey, index)
			if err != nil {
				return nil, 0, nil, 0, err
			}

			//pick wanted fields and save them in the batch
			err = ihi.rcc.FillBatchFromDecodedIndexKey(index,
				0, dis, amForKey, bat, rowIndexForKey)
			if err != nil {
				return nil, 0, nil, 0, err
			}

			lastKey = keys[i]
			rowIndexForKey++
			if rowIndexForKey >= int(ihi.kvLimit) {
				break
			}
		}

		//skip decoding the value
		if !justNeedKey {
			//need to update prefix
			//decode index value
			for i := 0; i < len(keys); i++ {
				//decode the name which is in the value
				data := values[i]
				_,dis,err := tkd.DecodePrimaryIndexValue(data,
					index,0,ihi.serializer)
				if err != nil {
					return nil, 0, nil, 0, err
				}

				//pick wanted fields and save them in the batch
				err = ihi.rcc.FillBatchFromDecodedIndexValue(index,
					0, dis,amForValue, bat, rowIndexForValue)
				if err != nil {
					return nil, 0, nil, 0, err
				}
				rowIndexForValue++
				if rowIndexForValue >= int(ihi.kvLimit) {
					if rowIndexForKey != rowIndexForValue {
						return nil, 0, nil, 0, errorRowIndexDifferentInKeyAndValue
					}
					break
				}
			}
		}

		//get the next prefix
		prefix = SuccessorOfKey(lastKey)
	}

	if !justNeedKey && rowIndexForKey != rowIndexForValue {
		return nil, 0, nil, 0, errorRowIndexDifferentInKeyAndValue
	}

	TruncateBatch(bat,int(ihi.kvLimit),rowIndexForKey)

	if readFinished {
		if rowIndexForKey == 0 {
			//there are no data read in this call.
			//it means there is no data any more.
			//reset the batch to the null to notify the
			//computation engine will not read data again.
			bat = nil
		}
	}
	return bat, rowIndexForKey, prefix, prefixLen, nil
}

func (ihi * IndexHandlerImpl) WriteIntoTable(table *descriptor.RelationDesc, bat *batch.Batch) error {
	return ihi.WriteIntoIndex(ihi.dbDesc, table, &table.Primary_index, nil, nil, bat)
}

//encodePrimaryIndexKey encodes the tuple into bytes.
//The prefix has the tenantID,dbID,tableID,IndexID.
func (ihi * IndexHandlerImpl) encodePrimaryIndexKey(prefix TupleKey,
	index *descriptor.IndexDesc,
	columnGroupID uint64,writeCtx *WriteContext,
	tuple Tuple)(TupleKey, *orderedcodec.EncodedItem,error) {
	if index.ID != PrimaryIndexID {
		return nil,nil,errorPrimaryIndexIDIsNotOne
	}
	tke := ihi.tch.GetEncoder()
	/*
		fields => bytes
		1. Get fields value from tuple
		2. Encoding fields
	*/
	//index attributes
	key := prefix
	var value interface{}
	var err error
	for _, attr := range index.Attributes {
		writeState := writeCtx.AttributeStates[attr.ID]
		//the logic for implicit primary key or default expr
		if writeState.NeedGenerated {
			if writeState.AttrDesc.Default.Exist {//default expr
				if writeState.AttrDesc.Default.IsNull {
					return nil, nil, errorPrimaryIndexAttributesHaveNull
				}
				value = writeState.AttrDesc.Default.Value
			}else{
				value = GetRowID(writeCtx.NodeID)
			}
		}else{
			posInBatch := writeState.PositionInBatch
			value, err = tuple.GetValue(uint32(posInBatch))
			if err != nil {
				return nil, nil, err
			}
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
func (ihi *IndexHandlerImpl) encodePrimaryIndexValue(prefix TupleValue,
	index *descriptor.IndexDesc,
	columnGroupID uint64,
	writeCtx * WriteContext,
	tuple Tuple,
	serializer ValueSerializer)(TupleValue, *orderedcodec.EncodedItem,error) {
	if index.ID != PrimaryIndexID {
		return nil,nil,errorPrimaryIndexIDIsNotOne
	}
	//fill value into the row from the tuple
	//for i, state := range writeCtx.AttributeStates {
		//TODO:implicit primary key
	//}

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

func (ihi * IndexHandlerImpl) callbackForEncodeRowInBatch(callbackCtx interface{}, tuple Tuple) error {
	writeCtx := callbackCtx.(*WriteContext)

	key, _, err := ihi.encodePrimaryIndexKey(writeCtx.callback.prefix,
		writeCtx.callback.index,0,writeCtx,tuple)
	if err != nil {
		return err
	}

	value, _, err := ihi.encodePrimaryIndexValue(nil,
		writeCtx.callback.index, 0, writeCtx,tuple, ihi.serializer)
	if err != nil {
		return err
	}

	//write key,value into the kv storage
	//failed if the key has existed
	err = ihi.kv.DedupSet(key,value)
	if err != nil {
		return err
	}
	return nil
}

func (ihi * IndexHandlerImpl) WriteIntoIndex(db *descriptor.DatabaseDesc, table *descriptor.RelationDesc, index *descriptor.IndexDesc, attrs []descriptor.AttributeDesc, writeCtx interface{}, bat *batch.Batch) error {
	//1.encode prefix (tenantID,dbID,tableID,indexID)
	tke := ihi.tch.GetEncoder()
	var prefix TupleKey
	prefix,_ = tke.EncodeIndexPrefix(prefix, uint64(db.ID), uint64(table.ID), uint64(index.ID))

	indexWriteCtx,ok := writeCtx.(*WriteContext)
	if !ok {
		return errorWriteContextIsInvalid
	}

	indexWriteCtx.callback = callbackPackage{
		index:  index,
		prefix: prefix,
	}

	//2.encode every row in the batch
	ba := NewBatchAdapter(bat)
	err := ba.ForEach(indexWriteCtx, ihi.callbackForEncodeRowInBatch)
	if err != nil {
		return err
	}
	return nil
}

func (ihi * IndexHandlerImpl) DeleteFromTable(table *descriptor.RelationDesc, bat *batch.Batch) error {
	panic("implement me")
}

func (ihi * IndexHandlerImpl) DeleteFromIndex(index *descriptor.IndexDesc, attrs []descriptor.AttributeDesc, bat *batch.Batch) error {
	panic("implement me")
}

