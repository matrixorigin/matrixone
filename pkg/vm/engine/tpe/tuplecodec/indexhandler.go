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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

var (
	errorRowIndexDifferentInKeyAndValue = errors.New("the rowIndexForkey != rowIndexForValue")
	errorWriteContextIsInvalid = errors.New("the write context is invalid")
	errorReadContextIsInvalid = errors.New("the read context is invalid")
)

var _ index.IndexHandler = &IndexHandlerImpl{}

type callbackPackage struct {
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

func (ihi * IndexHandlerImpl) parallelReader(indexReadCtx *ReadContext) (*batch.Batch, int, error) {
	if indexReadCtx.CompleteInShard {
		return nil, 0, nil
	}

	//check if we need the index key only.
	//Attributes we want are in the index key only.
	indexAttrIDs := descriptor.ExtractIndexAttributeIDs(indexReadCtx.IndexDesc.Attributes)
	amForKey :=&AttributeMap{}
	amForValue := &AttributeMap{}
	needKeyOnly := true
	for i,attr := range indexReadCtx.ReadAttributeDescs {
		if _,exist := indexAttrIDs[attr.ID]; exist {
			//id in the key
			amForKey.Append(int(attr.ID),i)
		}else{
			//id is not in the index key
			//then find it in the value
			needKeyOnly = false
			amForValue.Append(int(attr.ID),i)
		}
	}

	//1.encode prefix (tenantID,dbID,tableID,indexID)
	tke := ihi.tch.GetEncoder()
	tkd := ihi.tch.GetDecoder()

	if indexReadCtx.PrefixForScanKey == nil {
		indexReadCtx.PrefixForScanKey,_ = tke.EncodeIndexPrefix(nil, uint64(indexReadCtx.DbDesc.ID),
			uint64(indexReadCtx.TableDesc.ID),
			uint64(indexReadCtx.IndexDesc.ID))
	}

	if indexReadCtx.ShardNextScanKey == nil {
		indexReadCtx.ShardNextScanKey = indexReadCtx.ShardStartKey
	}

	//nextScanKey will not have
	if bytes.HasPrefix(indexReadCtx.ShardNextScanKey,indexReadCtx.PrefixForScanKey) {
		return nil, 0, nil
	}

	//prepare the batch
	names,attrdefs := ConvertAttributeDescIntoTypesType(indexReadCtx.ReadAttributeDescs)
	bat := MakeBatch(int(ihi.kvLimit),names,attrdefs)

	rowRead := 0
	readFinished := false

	cpuf := BeginCpuProfile("cpu_profile")

	//2.prefix read data from kv
	//get keys with the prefix
	for rowRead < int(ihi.kvLimit) {
		needRead := int(ihi.kvLimit) - rowRead
		keys, values, complete, nextScanKey, err := ihi.kv.GetRangeWithLimit(indexReadCtx.ShardNextScanKey,
			indexReadCtx.ShardEndKey, uint64(needRead))
		if err != nil {
			return nil, 0, err
		}

		rowRead += len(keys)

		//1.decode index key
		//2.get fields wanted
		for i := 0; i < len(keys); i++ {
			indexKey := keys[i][indexReadCtx.LengthOfPrefixForScanKey:]
			_, dis, err := tkd.DecodePrimaryIndexKey(indexKey, indexReadCtx.IndexDesc)
			if err != nil {
				return nil, 0, err
			}

			//pick wanted fields and save them in the batch
			err = ihi.rcc.FillBatchFromDecodedIndexKey(indexReadCtx.IndexDesc,
				0, dis, amForKey, bat, i)
			if err != nil {
				return nil, 0, err
			}
		}

		//skip decoding the value
		if !needKeyOnly {
			//need to update prefix
			//decode index value
			for i := 0; i < len(keys); i++ {
				//decode the name which is in the value
				data := values[i]
				_,dis,err := tkd.DecodePrimaryIndexValue(data,
					indexReadCtx.IndexDesc,0,ihi.serializer)
				if err != nil {
					return nil, 0, err
				}

				//pick wanted fields and save them in the batch
				err = ihi.rcc.FillBatchFromDecodedIndexValue(indexReadCtx.IndexDesc,
					0, dis,amForValue, bat, i)
				if err != nil {
					return nil, 0, err
				}
			}
		}

		//get the next prefix
		indexReadCtx.ShardNextScanKey = nextScanKey
		if complete {
			indexReadCtx.CompleteInShard = true
			readFinished = true
			break
		}
	}

	TruncateBatch(bat,int(ihi.kvLimit),rowRead)

	if readFinished {
		if rowRead == 0 {
			//there are no data read in this call.
			//it means there is no data any more.
			//reset the batch to the null to notify the
			//computation engine will not read data again.
			bat = nil
		}
	}

	EndCpuProfile(cpuf)
	return bat, rowRead, nil
}

func (ihi * IndexHandlerImpl) ReadFromIndex(readCtx interface{}) (*batch.Batch, int, error) {
	indexReadCtx,ok := readCtx.(*ReadContext)
	if !ok {
		return nil, 0, errorReadContextIsInvalid
	}

	if indexReadCtx.ParallelReader {
		return ihi.parallelReader(indexReadCtx)
	}

	//check if we need the index key only.
	//Attributes we want are in the index key only.
	indexAttrIDs := descriptor.ExtractIndexAttributeIDs(indexReadCtx.IndexDesc.Attributes)
	amForKey :=&AttributeMap{}
	amForValue := &AttributeMap{}
	needKeyOnly := true
	for i,attr := range indexReadCtx.ReadAttributeDescs {
		if _,exist := indexAttrIDs[attr.ID]; exist {
			//id in the key
			amForKey.Append(int(attr.ID),i)
		}else{
			//id is not in the index key
			//then find it in the value
			needKeyOnly = false
			amForValue.Append(int(attr.ID),i)
		}
	}

	//1.encode prefix (tenantID,dbID,tableID,indexID)
	tke := ihi.tch.GetEncoder()
	tkd := ihi.tch.GetDecoder()

	if indexReadCtx.CompleteInAllShards {
		return nil, 0, nil
	}else if !indexReadCtx.CompleteInAllShards &&
			indexReadCtx.PrefixForScanKey == nil {
		indexReadCtx.PrefixForScanKey,_ = tke.EncodeIndexPrefix(indexReadCtx.PrefixForScanKey, uint64(indexReadCtx.DbDesc.ID),
			uint64(indexReadCtx.TableDesc.ID),
			uint64(indexReadCtx.IndexDesc.ID))
		indexReadCtx.LengthOfPrefixForScanKey = len(indexReadCtx.PrefixForScanKey)
	}

	//prepare the batch
	names,attrdefs := ConvertAttributeDescIntoTypesType(indexReadCtx.ReadAttributeDescs)
	bat := MakeBatch(int(ihi.kvLimit),names,attrdefs)

	rowRead := 0
	readFinished := false

	cpuf := BeginCpuProfile("cpu_profile")

	//2.prefix read data from kv
	//get keys with the prefix
	for rowRead < int(ihi.kvLimit) {
		needRead := int(ihi.kvLimit) - rowRead
		keys, values, complete, nextScanKey, err := ihi.kv.GetWithPrefix(indexReadCtx.PrefixForScanKey,indexReadCtx.LengthOfPrefixForScanKey, uint64(needRead))
		if err != nil {
			return nil, 0, err
		}

		rowRead += len(keys)

		//1.decode index key
		//2.get fields wanted
		for i := 0; i < len(keys); i++ {
			indexKey := keys[i][indexReadCtx.LengthOfPrefixForScanKey:]
			_, dis, err := tkd.DecodePrimaryIndexKey(indexKey, indexReadCtx.IndexDesc)
			if err != nil {
				return nil, 0, err
			}

			//pick wanted fields and save them in the batch
			err = ihi.rcc.FillBatchFromDecodedIndexKey(indexReadCtx.IndexDesc,
				0, dis, amForKey, bat, i)
			if err != nil {
				return nil, 0, err
			}
		}

		//skip decoding the value
		if !needKeyOnly {
			//need to update prefix
			//decode index value
			for i := 0; i < len(keys); i++ {
				//decode the name which is in the value
				data := values[i]
				_,dis,err := tkd.DecodePrimaryIndexValue(data,
					indexReadCtx.IndexDesc,0,ihi.serializer)
				if err != nil {
					return nil, 0, err
				}

				//pick wanted fields and save them in the batch
				err = ihi.rcc.FillBatchFromDecodedIndexValue(indexReadCtx.IndexDesc,
					0, dis,amForValue, bat, i)
				if err != nil {
					return nil, 0, err
				}
			}
		}

		//get the next prefix
		indexReadCtx.PrefixForScanKey = nextScanKey
		if complete {
			indexReadCtx.CompleteInAllShards = true
			readFinished = true
			break
		}
	}

	TruncateBatch(bat,int(ihi.kvLimit),rowRead)

	if readFinished {
		if rowRead == 0 {
			//there are no data read in this call.
			//it means there is no data any more.
			//reset the batch to the null to notify the
			//computation engine will not read data again.
			bat = nil
		}
	}

	EndCpuProfile(cpuf)
	return bat, rowRead, nil
}

func (ihi * IndexHandlerImpl) WriteIntoTable(table *descriptor.RelationDesc, writeCtx interface{}, bat *batch.Batch) error {
	return ihi.WriteIntoIndex(writeCtx, bat)
}

//encodePrimaryIndexKey encodes the tuple into bytes.
//The prefix has the tenantID,dbID,tableID,IndexID.
func (ihi * IndexHandlerImpl) encodePrimaryIndexKey(columnGroupID uint64, writeCtx *WriteContext, tuple Tuple) (TupleKey, *orderedcodec.EncodedItem, error) {
	if writeCtx.IndexDesc.ID != PrimaryIndexID {
		return nil,nil,errorPrimaryIndexIDIsNotOne
	}
	tke := ihi.tch.GetEncoder()
	/*
		fields => bytes
		1. Get fields value from tuple
		2. Encoding fields
	*/
	//index attributes
	//allocate space for the key
	key := make(TupleKey,len(writeCtx.callback.prefix))
	copy(key,writeCtx.callback.prefix)
	var value interface{}
	var err error
	for _, attr := range writeCtx.IndexDesc.Attributes {
		writeState := &writeCtx.AttributeStates[attr.ID]
		//the logic for implicit primary key or default expr
		if writeState.NeedGenerated {
			if writeState.AttrDesc.Default.Exist {//default expr
				if writeState.AttrDesc.Default.IsNull {
					return nil, nil, errorPrimaryIndexAttributesHaveNull
				}
				value = writeState.AttrDesc.Default.Value
			}else{
				value = GetRowID(writeCtx.NodeID)
				//store the implicit primary key
				writeState.ImplicitPrimaryKey = value
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
func (ihi *IndexHandlerImpl) encodePrimaryIndexValue(columnGroupID uint64, writeCtx *WriteContext, tuple Tuple, serializer ValueSerializer) (TupleValue, *orderedcodec.EncodedItem, error) {
	if writeCtx.IndexDesc.ID != PrimaryIndexID {
		return nil,nil,errorPrimaryIndexIDIsNotOne
	}

	var out TupleValue
	var value interface{}
	var err error
	var serialized []byte

	//fill value into the row from the tuple
	for _, state := range writeCtx.AttributeStates {
		//the logic for implicit primary key or default expr
		if state.NeedGenerated {
			if state.AttrDesc.Default.Exist {//default expr
				value = state.AttrDesc.Default.Value
			}else{
				//get the implicit primary key
				value = state.ImplicitPrimaryKey
			}
		}else{
			value, err = tuple.GetValue(uint32(state.PositionInBatch))
			if err != nil {
				return nil, nil, err
			}
		}
		//serial value
		serialized,_, err = serializer.SerializeValue(out,value)
		if err != nil {
			return nil, nil, err
		}
		out = serialized
	}

	return out, nil, nil
}

func (ihi * IndexHandlerImpl) callbackForEncodeTupleInBatch(callbackCtx interface{}, tuple Tuple) error {
	writeCtx := callbackCtx.(*WriteContext)

	key, _, err := ihi.encodePrimaryIndexKey(0, writeCtx, tuple)
	if err != nil {
		return err
	}

	value, _, err := ihi.encodePrimaryIndexValue(0, writeCtx, tuple, ihi.serializer)
	if err != nil {
		return err
	}

	writeCtx.keys = append(writeCtx.keys,key)
	writeCtx.values = append(writeCtx.values,value)
	return nil
}

func (ihi * IndexHandlerImpl) WriteIntoIndex(writeCtx interface{}, bat *batch.Batch) error {
	indexWriteCtx,ok := writeCtx.(*WriteContext)
	if !ok {
		return errorWriteContextIsInvalid
	}

	defer func() {
		indexWriteCtx.resetWriteCache()
	}()

	//1.encode prefix (tenantID,dbID,tableID,indexID)
	tke := ihi.tch.GetEncoder()
	var prefix TupleKey
	prefix,_ = tke.EncodeIndexPrefix(prefix,
		uint64(indexWriteCtx.DbDesc.ID),
		uint64(indexWriteCtx.TableDesc.ID),
		uint64(indexWriteCtx.IndexDesc.ID))

	indexWriteCtx.callback = callbackPackage{
		prefix: prefix,
	}

	//2.encode every row in the batch
	ba := NewBatchAdapter(bat)
	err := ba.ForEachTuple(indexWriteCtx, ihi.callbackForEncodeTupleInBatch)
	if err != nil {
		return err
	}

	err = ihi.kv.DedupSetBatch(indexWriteCtx.keys,indexWriteCtx.values)
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

