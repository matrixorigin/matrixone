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
	"sort"
)

var (
	errorRowIndexDifferentInKeyAndValue = errors.New("the rowIndexForkey != rowIndexForValue")
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
	bat := makeBatch(int(ihi.kvLimit),names,attrdefs)

	rowIndexForKey := 0
	rowIndexForValue := 0

	//2.prefix read data from kv
	//get keys with the prefix
	var lastKey []byte
	for {
		keys, values, err := ihi.kv.GetWithPrefix(prefix,prefixLen,ihi.kvLimit)
		if err != nil {
			return nil, 0, nil, 0, err
		}

		if len(keys) == 0 {
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

	return bat, rowIndexForKey, prefix, prefixLen, nil
}

func (ihi * IndexHandlerImpl) WriteIntoTable(table *descriptor.RelationDesc, bat *batch.Batch) error {
	return ihi.WriteIntoIndex(ihi.dbDesc,table,&table.Primary_index,nil,bat)
}

func (ihi * IndexHandlerImpl) callbackForEncodeRowInBatch(callbackCtx interface{}, tuple Tuple) error {
	callback := callbackCtx.(callbackPackage)
	tke := ihi.tch.GetEncoder()

	key, _, err := tke.EncodePrimaryIndexKey(callback.prefix,callback.index,0,tuple)
	if err != nil {
		return err
	}

	value, _, err := tke.EncodePrimaryIndexValue(nil, callback.index, 0, tuple, ihi.serializer)
	if err != nil {
		return err
	}

	//write key,value into the kv storage
	err = ihi.kv.Set(key,value)
	if err != nil {
		return err
	}
	return nil
}

func (ihi * IndexHandlerImpl) WriteIntoIndex(db *descriptor.DatabaseDesc,
		table *descriptor.RelationDesc,
		index *descriptor.IndexDesc,
		attrs []descriptor.AttributeDesc,
		bat *batch.Batch) error {
	//1.encode prefix (tenantID,dbID,tableID,indexID)
	tke := ihi.tch.GetEncoder()
	var prefix TupleKey
	prefix,_ = tke.EncodeIndexPrefix(prefix, uint64(db.ID), uint64(table.ID), uint64(index.ID))

	callback := callbackPackage{
		index:  index,
		prefix: prefix,
	}

	//2.encode every row in the batch
	ba := NewBatchAdapter(bat)
	err := ba.ForEach(callback, ihi.callbackForEncodeRowInBatch)
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

