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

package engine

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
)

var (
	errorBatchAttributeDoNotExistInTheRelation = errors.New("batch attribute do not exist in the relation")
	errorNotHiddenPrimaryKey                   = errors.New("it is not hidden primary key")
	errorDuplicateAttributeNameInBatch         = errors.New("duplicate attribute name in the batch")
	errorDoNotGetValidValueForTheAttribute     = errors.New("can not get the value for the attribute")
)

func (trel *TpeRelation) Rows() int64 {
	rows := int64(0)
	//read global shards
	for _, info := range trel.shards.GetShardInfos() {
		stats := info.GetStatistics()
		rows += int64(stats.GetApproximateKeys())
	}
	return rows
}

func (trel *TpeRelation) Size(s string) int64 {
	size := int64(0)
	//read global shards
	for _, info := range trel.shards.GetShardInfos() {
		stats := info.GetStatistics()
		size += int64(stats.GetApproximateSize())
	}
	return size
}

func (trel *TpeRelation) Cardinality(_ string) int64 {
	return 1
}

func (trel *TpeRelation) Close() {
}

func (trel *TpeRelation) ID() string {
	return trel.desc.Name
}

func (trel *TpeRelation) Nodes() engine.Nodes {
	for i, node := range trel.nodes {
		logutil.Infof("index %d storeID %v all_nodes %v", i, trel.storeID, node)
	}
	return trel.nodes
}

func (trel *TpeRelation) GetPriKeyOrHideKey() ([]engine.Attribute, bool) {
	var attrs []engine.Attribute
	hasPriKey := false
	for _, attr := range trel.desc.Attributes {
		if attr.Is_hidden {
			attrs = append(attrs, engine.Attribute{
				Name:    attr.Name,
				Alg:     0,
				Type:    attr.TypesType,
				Default: attr.Default,
				Primary: attr.Is_primarykey,
			})
			return attrs, false
		}
		if attr.Is_primarykey {
			attrs = append(attrs, engine.Attribute{
				Name:    attr.Name,
				Alg:     0,
				Type:    attr.TypesType,
				Default: attr.Default,
				Primary: attr.Is_primarykey,
			})
			hasPriKey = true
		}
	}
	if hasPriKey {
		return attrs, hasPriKey
	}
	return nil, false
}

func (trel *TpeRelation) TableDefs() []engine.TableDef {
	var defs []engine.TableDef
	var pkNames []string
	for _, attr := range trel.desc.Attributes {
		//skip hidden attribute ?
		if !attr.Is_hidden {
			if attr.Is_primarykey {
				pkNames = append(pkNames, attr.Name)
			}
			def := &engine.AttributeDef{Attr: engine.Attribute{
				Name:    attr.Name,
				Alg:     0,
				Type:    attr.TypesType,
				Default: attr.Default,
				Primary: attr.Is_primarykey,
			}}
			defs = append(defs, def)
		}
	}

	if len(pkNames) != 0 {
		defs = append(defs, &engine.PrimaryIndexDef{
			Names: pkNames,
		})
	}

	if len(trel.desc.Comment) != 0 {
		defs = append(defs, &engine.CommentDef{Comment: trel.desc.Comment})
	}
	return defs
}

func (trel *TpeRelation) Write(_ uint64, batch *batch.Batch) error {
	//attribute set
	attrSet := make(map[string]uint32)
	for _, attr := range trel.desc.Attributes {
		attrSet[attr.Name] = attr.ID
	}

	//check if the attribute in the batch exists in the relation or not.
	var attrDescs []descriptor.AttributeDesc
	batchAttrSet := make(map[string]int)
	for posInBatch, batchAttrName := range batch.Attrs {
		if _, ok := batchAttrSet[batchAttrName]; ok {
			return errorDuplicateAttributeNameInBatch
		} else {
			batchAttrSet[batchAttrName] = posInBatch
		}

		if _, ok := attrSet[batchAttrName]; ok {
			attrDescs = append(attrDescs, trel.desc.Attributes[posInBatch])
		} else {
			return errorBatchAttributeDoNotExistInTheRelation
		}
	}

	//Ensure the position mapping from the attribute in the relation
	//to the attribute in the batch.
	//Then, it is convenient to get the right data from the batch
	//in encoding and serialization.
	writeStates := make([]tuplecodec.AttributeStateForWrite, len(trel.desc.Attributes))

	//find the attributes not covered by the batch in the relation
	for attrIdx, attrDesc := range trel.desc.Attributes {
		writeStates[attrIdx].AttrDesc = attrDesc
		writeStates[attrIdx].PositionInBatch = -1
		writeStates[attrIdx].NeedGenerated = false
		//attribute not in the batch
		if posInBatch, exist := batchAttrSet[attrDesc.Name]; !exist {
			//hidden primary key
			if attrDesc.Is_hidden && attrDesc.Is_primarykey {
				//it is hidden primary key
				writeStates[attrIdx].PositionInBatch = -1
				writeStates[attrIdx].NeedGenerated = true
			} else if attrDesc.Default.Exist { //default expr
				writeStates[attrIdx].PositionInBatch = -1
				writeStates[attrIdx].NeedGenerated = true
			} else {
				return errorDoNotGetValidValueForTheAttribute
			}
		} else {
			writeStates[attrIdx].PositionInBatch = posInBatch
			writeStates[attrIdx].NeedGenerated = false
		}
	}

	writeCtx := &tuplecodec.WriteContext{
		DbDesc:          trel.dbDesc,
		TableDesc:       trel.desc,
		IndexDesc:       &trel.desc.Primary_index,
		BatchAttrs:      attrDescs,
		AttributeStates: writeStates,
		NodeID:          trel.storeID,
	}

	err := trel.computeHandler.Write(writeCtx, batch)
	if err != nil {
		return err
	}
	return nil
}

func (trel *TpeRelation) AddTableDef(u uint64, def engine.TableDef) error {
	panic("implement me")
}

func (trel *TpeRelation) DelTableDef(u uint64, def engine.TableDef) error {
	panic("implement me")
}

func (trel *TpeRelation) parallelReader(cnt int) []engine.Reader {
	tcnt := cnt
	if cnt <= 0 {
		tcnt = 1
	}
	var retReaders []engine.Reader = make([]engine.Reader, cnt)
	var tpeReaders []*TpeReader = make([]*TpeReader, tcnt)
	//split shards into multiple readers
	shardInfos := trel.shardsInThisNode.GetShardInfos()
	shardInfosCount := len(shardInfos)

	shardCountPerReader := shardInfosCount / tcnt

	if shardInfosCount%tcnt != 0 {
		shardCountPerReader++
	}

	//for test
	//one reader for all shards
	if trel.useOneThread {
		shardCountPerReader = shardInfosCount
	}

	startIndex := 0
	for i := 0; i < len(tpeReaders); i++ {
		endIndex := tuplecodec.Min(startIndex+shardCountPerReader, shardInfosCount)
		var infos []ShardInfo
		for j := startIndex; j < endIndex; j++ {
			info := shardInfos[j]
			newInfo := ShardInfo{
				startKey:        info.GetStartKey(),
				endKey:          info.GetEndKey(),
				nextScanKey:     nil,
				completeInShard: false,
				shardID:         info.GetShardID(),
				node: ShardNode{
					Addr:         info.GetShardNode().Addr,
					StoreID:      info.GetShardNode().StoreID,
					StoreIDbytes: info.GetShardNode().StoreIDbytes,
					Statistics:   info.GetStatistics(),
				},
			}
			infos = append(infos, newInfo)
		}

		if len(infos) != 0 {
			tpeReaders[i] = &TpeReader{
				dbDesc:         trel.dbDesc,
				tableDesc:      trel.desc,
				computeHandler: trel.computeHandler,
				shardInfos:     infos,
				parallelReader: true,
				isDumpReader:   false,
				id:             i,
				storeID:        trel.storeID,
			}
		} else {
			tpeReaders[i] = &TpeReader{isDumpReader: true, id: i}
		}

		logutil.Infof("store id %d reader %d shard startIndex %d shardCountPerReader %d shardCount %d endIndex %d isDumpReader %v",
			trel.storeID, i, startIndex, shardCountPerReader, shardInfosCount, endIndex, tpeReaders[i].isDumpReader)
		startIndex += shardCountPerReader
	}

	for i, reader := range tpeReaders {
		if reader != nil {
			retReaders[i] = reader
			logutil.Infof("-->reader %v", reader.shardInfos)
		} else {
			retReaders[i] = &TpeReader{isDumpReader: true}
		}
	}
	return retReaders
}

func (trel *TpeRelation) NewReader(cnt int, _ extend.Extend, _ []byte) []engine.Reader {
	logutil.Infof("newreader cnt %d", cnt)
	if trel.computeHandler.ParallelReader() || trel.computeHandler.MultiNode() {
		return trel.parallelReader(cnt)
	}
	var readers []engine.Reader = make([]engine.Reader, cnt)
	tr := &TpeReader{
		dbDesc:         trel.dbDesc,
		tableDesc:      trel.desc,
		computeHandler: trel.computeHandler,
		parallelReader: false,
		isDumpReader:   false,
		multiNode:      trel.computeHandler.MultiNode(),
		storeID:        trel.storeID,
	}
	shardInfos := trel.shardsInThisNode.GetShardInfos()
	for _, info := range shardInfos {
		newInfo := ShardInfo{
			startKey:        info.GetStartKey(),
			endKey:          info.GetEndKey(),
			nextScanKey:     nil,
			completeInShard: false,
			node: ShardNode{
				Addr:         info.GetShardNode().Addr,
				StoreID:      info.GetShardNode().StoreID,
				StoreIDbytes: info.GetShardNode().StoreIDbytes,
			},
		}
		tr.shardInfos = append(tr.shardInfos, newInfo)
		logutil.Infof("single reader %v", newInfo)
	}
	readers[0] = tr
	for i := 1; i < cnt; i++ {
		readers[i] = &TpeReader{isDumpReader: true}
	}
	return readers
}
