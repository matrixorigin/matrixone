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
	"fmt"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/computation"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

const (
	DATABASE_ID = "database_id"
	TABLE_ID    = "table_id"
)

var (
	errorDatabaseExists                          = errors.New("database has exists")
	errorTableExists                             = errors.New("table has exists")
	errorTableDeletedAlready                     = errors.New("table is deleted already. It is impossible.")
	errorWrongDatabaseIDInDatabaseDesc           = errors.New("wrong database id in the database desc.  It is impossible.")
	errorDatabaseDeletedAlready                  = errors.New("database is deleted already")
	ErrorIsNotShards                             = errors.New("it is not the shards")
	errorShardsAreNil                            = errors.New("shards are nil")
	errorThereAreNotNodesHoldTheTable            = errors.New("there are not nodes hold the table")
	errorCanNotDropTheInternalDatabase           = errors.New("you can not drop the internal database")
	errorCanNotDropTheTableInTheInternalDatabase = errors.New("you can not drop the table in the internal database")
)

var _ computation.ComputationHandler = &ComputationHandlerImpl{}

type ComputationHandlerImpl struct {
	dh             descriptor.DescriptorHandler
	kv             KVHandler
	tch            *TupleCodecHandler
	serializer     ValueSerializer
	indexHandler   index.IndexHandler
	epochHandler   *EpochHandler
	parallelReader bool
	multiNode      bool
}

func (chi *ComputationHandlerImpl) Read(readCtx interface{}) (*batch.Batch, error) {
	var bat *batch.Batch
	var err error
	bat, _, err = chi.indexHandler.ReadFromIndex(readCtx)
	if err != nil {
		return nil, err
	}
	return bat, nil
}

// judge the operation is delete or insert, true for delete, false for insert
func GetDeleteFlag(bat *batch.Batch) bool {
	deleteFlag := false
	if len(bat.Zs) > 0 {
		deleteFlag = true
	}
	for i := 0; i < len(bat.Zs); i++ {
		if bat.Zs[i] != -1 {
			deleteFlag = false
			break
		}
	}
	return deleteFlag
}

func (chi *ComputationHandlerImpl) Write(writeCtx interface{}, bat *batch.Batch) error {
	if bat == nil {
		return nil
	}

	var err error
	if GetDeleteFlag(bat) {
		err = chi.indexHandler.DeleteFromIndex(writeCtx, bat)
	} else if bat.Zs == nil {
		err = chi.indexHandler.WriteIntoIndex(writeCtx, bat)
	} else {
		err = chi.indexHandler.UpdateIntoIndex(writeCtx, bat)
	}

	if err != nil {
		return err
	}
	return nil
}

func NewComputationHandlerImpl(dh descriptor.DescriptorHandler, kv KVHandler, tch *TupleCodecHandler, serial ValueSerializer, ih index.IndexHandler, epoch *EpochHandler, parallelReader bool, multiNode bool) *ComputationHandlerImpl {
	return &ComputationHandlerImpl{
		dh:             dh,
		kv:             kv,
		tch:            tch,
		serializer:     serial,
		indexHandler:   ih,
		epochHandler:   epoch,
		parallelReader: parallelReader,
		multiNode:      multiNode,
	}
}

func (chi *ComputationHandlerImpl) CreateDatabase(epoch uint64, dbName string, typ int) (uint64, error) {
	//1.check db existence
	_, err := chi.dh.LoadDatabaseDescByName(dbName)
	if err != nil {
		if err != errorDoNotFindTheDesc {
			return 0, err
		}
		//do not find the desc
	} else {
		return 0, errorDatabaseExists
	}

	// Now,the db does not exist

	//2. Get the next id for the db
	id, err := chi.kv.NextID(DATABASE_ID)
	if err != nil {
		return 0, err
	}

	//3. Save the descriptor
	desc := &descriptor.DatabaseDesc{
		ID:               uint32(id),
		Name:             dbName,
		Update_time:      time.Now().Unix(),
		Create_epoch:     epoch,
		Is_deleted:       false,
		Drop_epoch:       0,
		Max_access_epoch: epoch,
		Typ:              typ,
	}

	err = chi.dh.StoreDatabaseDescByID(id, desc)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (chi *ComputationHandlerImpl) DropDatabase(epoch uint64, dbName string) error {
	//1. check database exists
	dbDesc, err := chi.dh.LoadDatabaseDescByName(dbName)
	if err != nil {
		return err
	}

	if chi.isInternalDatabase(uint64(dbDesc.ID)) {
		return errorCanNotDropTheInternalDatabase
	}

	//2. list tables and drop them one by one
	tableDescs, err := chi.ListTables(uint64(dbDesc.ID))
	if err != nil {
		return err
	}

	for _, desc := range tableDescs {
		_, err := chi.DropTableByDesc(epoch, uint64(dbDesc.ID), desc)
		if err != nil {
			return err
		}
	}

	//3. attach tag
	dbDesc.Is_deleted = true
	dbDesc.Drop_epoch = epoch

	//Note: we do not save the database desc into the AsyncTable

	//4. delete the database desc
	err = chi.dh.DeleteDatabaseDescByID(uint64(dbDesc.ID))
	if err != nil {
		return err
	}
	return nil
}

func (chi *ComputationHandlerImpl) GetDatabase(dbName string) (*descriptor.DatabaseDesc, error) {
	//1. check database exists
	dbDesc, err := chi.dh.LoadDatabaseDescByName(dbName)
	if err != nil {
		return nil, err
	}

	//2. check it is deleted
	if dbDesc.Is_deleted {
		return nil, errorDatabaseDeletedAlready
	}

	return dbDesc, nil
}

//callbackForGetDatabaseDesc extracts the databaseDesc
func (chi *ComputationHandlerImpl) callbackForGetDatabaseDesc(callbackCtx interface{}, dis []*orderedcodec.DecodedItem) ([]byte, error) {
	//get the name and the desc
	descAttr := InternalDescriptorTableDesc.Attributes[InternalDescriptorTable_desc_ID]
	descDI := dis[InternalDescriptorTable_desc_ID]
	if !(descDI.IsValueType(descAttr.Ttype)) {
		return nil, errorTypeInValueNotEqualToTypeInAttribute
	}

	//deserialize the desc
	if bytesInValue, ok := descDI.Value.([]byte); ok {
		dbDesc, err := UnmarshalDatabaseDesc(bytesInValue)
		if err != nil {
			return nil, err
		}
		//skip deleted table
		if dbDesc.Is_deleted {
			return nil, nil
		}
		if out, ok2 := callbackCtx.(*[]*descriptor.DatabaseDesc); ok2 {
			*out = append(*out, dbDesc)
		}
	}
	return nil, nil
}

func (chi *ComputationHandlerImpl) ListDatabases() ([]*descriptor.DatabaseDesc, error) {
	var dbDescs []*descriptor.DatabaseDesc
	_, err := chi.dh.GetValuesWithPrefix(math.MaxUint64, &dbDescs, chi.callbackForGetDatabaseDesc)
	if err != nil && err != errorDoNotFindTheDesc {
		return nil, err
	}
	return dbDescs, nil
}

func (chi *ComputationHandlerImpl) CreateTable(epoch, dbId uint64, tableDesc *descriptor.RelationDesc) (uint64, error) {
	//1. check database exists
	dbDesc, err := chi.dh.LoadDatabaseDescByID(dbId)
	if err != nil {
		return 0, err
	}

	//the database exists
	//2. check table exists
	_, err = chi.dh.LoadRelationDescByName(uint64(dbDesc.ID), tableDesc.Name)
	if err != nil {
		if err != errorDoNotFindTheDesc {
			return 0, err
		}
		//do no find the desc
	} else {
		return 0, errorTableExists
	}

	//3. get the nextid for the table
	id, err := chi.kv.NextID(TABLE_ID)
	if err != nil {
		return 0, err
	}

	//4. save the descriptor
	tableDesc.ID = uint32(id)
	tableDesc.Create_epoch = epoch
	tableDesc.Create_time = time.Now().Unix()
	tableDesc.Max_access_epoch = epoch

	err = chi.dh.StoreRelationDescByID(dbId, id, tableDesc)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (chi *ComputationHandlerImpl) isInternalDatabase(dbID uint64) bool {
	return dbID == InternalDatabaseID
}

func (chi *ComputationHandlerImpl) isInternalTable(tableID uint64) bool {
	return tableID == InternalDescriptorTableID ||
		tableID == InternalAsyncGCTableID
}

func (chi *ComputationHandlerImpl) DropTable(epoch, dbId uint64, tableName string) (uint64, error) {
	if chi.isInternalDatabase(dbId) {
		return 0, errorCanNotDropTheTableInTheInternalDatabase
	}

	//1. check database exists
	dbDesc, err := chi.dh.LoadDatabaseDescByID(dbId)
	if err != nil {
		return 0, err
	}

	//2. check table exists
	tableDesc, err := chi.dh.LoadRelationDescByName(uint64(dbDesc.ID), tableName)
	if err != nil {
		return 0, err
	}

	return chi.DropTableByDesc(epoch, dbId, tableDesc)
}

func (chi *ComputationHandlerImpl) DropTableByDesc(epoch, dbId uint64, tableDesc *descriptor.RelationDesc) (uint64, error) {
	if chi.isInternalDatabase(dbId) {
		return 0, errorCanNotDropTheTableInTheInternalDatabase
	}

	//check the table is deleted already
	if tableDesc.Is_deleted {
		return 0, errorTableDeletedAlready
	}

	//3. attach the tag
	tableDesc.Drop_epoch = epoch
	tableDesc.Drop_time = time.Now().Unix()
	tableDesc.Is_deleted = true

	//4. save thing into the internal async gc (epoch(pk),dbid,tableid,desc)
	err := chi.dh.StoreRelationDescIntoAsyncGC(epoch, dbId, tableDesc)
	if err != nil {
		return 0, err
	}

	//5. delete the tableDesc from the internal descriptor table
	err = chi.dh.DeleteRelationDescByID(dbId,
		uint64(tableDesc.ID))
	if err != nil {
		return 0, err
	}
	return uint64(tableDesc.ID), nil
}

//callbackForGetTableDesc extracts the tableDesc
func (chi *ComputationHandlerImpl) callbackForGetTableDesc(callbackCtx interface{}, dis []*orderedcodec.DecodedItem) ([]byte, error) {
	//get the name and the desc
	descAttr := InternalDescriptorTableDesc.Attributes[InternalDescriptorTable_desc_ID]
	descDI := dis[InternalDescriptorTable_desc_ID]
	if !(descDI.IsValueType(descAttr.Ttype)) {
		return nil, errorTypeInValueNotEqualToTypeInAttribute
	}

	//deserialize the desc
	if bytesInValue, ok := descDI.Value.([]byte); ok {
		tableDesc, err := UnmarshalRelationDesc(bytesInValue)
		if err != nil {
			return nil, err
		}
		//skip deleted table
		if tableDesc.Is_deleted {
			return nil, nil
		}
		if out, ok2 := callbackCtx.(*[]*descriptor.RelationDesc); ok2 {
			*out = append(*out, tableDesc)
		}
	}
	return nil, nil
}

func (chi *ComputationHandlerImpl) ListTables(dbId uint64) ([]*descriptor.RelationDesc, error) {
	//1. check database exists
	dbDesc, err := chi.dh.LoadDatabaseDescByID(dbId)
	if err != nil {
		return nil, err
	}

	//check database
	if uint64(dbDesc.ID) != dbId {
		return nil, errorWrongDatabaseIDInDatabaseDesc
	}

	//2. list tables
	// tenantID,dbID,tableID,indexID + parentID(dbId here) + ID + Name + Bytes
	var tableDescs []*descriptor.RelationDesc
	_, err = chi.dh.GetValuesWithPrefix(dbId, &tableDescs, chi.callbackForGetTableDesc)
	if err != nil && err != errorDoNotFindTheDesc {
		return nil, err
	}

	return tableDescs, nil
}

func (chi *ComputationHandlerImpl) GetTable(dbId uint64, name string) (*descriptor.RelationDesc, error) {
	//1. check database exists
	dbDesc, err := chi.dh.LoadDatabaseDescByID(dbId)
	if err != nil {
		return nil, err
	}

	//check database
	if uint64(dbDesc.ID) != dbId {
		return nil, errorWrongDatabaseIDInDatabaseDesc
	}

	//2. Get the table
	tableDesc, err := chi.dh.LoadRelationDescByName(dbId, name)
	if err != nil {
		return nil, err
	}

	//3. check the table is deleted
	if tableDesc.Is_deleted {
		return nil, errorTableDeletedAlready
	}
	return tableDesc, nil
}

func (chi *ComputationHandlerImpl) RemoveDeletedTable(epoch uint64) (int, error) {
	return chi.epochHandler.RemoveDeletedTable(epoch)
}

func (chi *ComputationHandlerImpl) GetNodesHoldTheTable(dbId uint64, desc *descriptor.RelationDesc) (engine.Nodes, interface{}, error) {
	if chi.kv.GetKVType() == KV_MEMORY {
		var nds = []engine.Node{
			{
				Id:   "0",
				Addr: "localhost:20000",
				Data: nil,
			},
		}
		return nds, &Shards{}, nil
	}
	tce := chi.tch.GetEncoder()
	prefix, _ := tce.EncodeIndexPrefix(nil, dbId, uint64(desc.ID), uint64(PrimaryIndexID))
	ret, err := chi.kv.GetShardsWithPrefix(prefix)
	if err != nil {
		return nil, nil, err
	}

	shards, ok := ret.(*Shards)
	if !ok {
		return nil, nil, ErrorIsNotShards
	}

	if shards == nil {
		return nil, nil, errorShardsAreNil
	}

	var nodes engine.Nodes
	for i, node := range shards.nodes {
		logutil.Infof("xindex %d all_nodes %v", i, node)
		nodeShards, err := json.Marshal(node.Shards)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, engine.Node{
			Id:   node.StoreIDbytes,
			Addr: node.Addr,
			Data: nodeShards, //put the shards info here
		})
	}

	if len(nodes) == 0 {
		return nil, nil, errorThereAreNotNodesHoldTheTable
	}

	return nodes, ret, nil
}

func (chi *ComputationHandlerImpl) ParallelReader() bool {
	return chi.parallelReader
}

func (chi *ComputationHandlerImpl) MultiNode() bool {
	return chi.multiNode
}

type AttributeStateForWrite struct {
	PositionInBatch int

	//true - the attribute value should be generated.
	//false - the attribute value got from the batch.
	NeedGenerated bool

	AttrDesc descriptor.AttributeDesc

	//the value for the attribute especially for
	//the implicit primary key
	ImplicitPrimaryKey interface{}
}

type WriteContext struct {
	//target database,table and index
	DbDesc    *descriptor.DatabaseDesc
	TableDesc *descriptor.RelationDesc
	IndexDesc *descriptor.IndexDesc

	//write control for the attribute
	AttributeStates []AttributeStateForWrite

	//the attributes need to be written
	BatchAttrs []descriptor.AttributeDesc

	callback callbackPackage

	NodeID uint64

	//to set
	keys   []TupleKey
	values []TupleValue
	t0     time.Duration
	colIndex	map[string]int
}

func (wc *WriteContext) resetWriteCache() {
	wc.keys = nil
	wc.values = nil
}

//for parallel readers
type ParallelReaderContext struct {
	ID int

	//index in shard info
	ShardIndex int

	//the startKey of the shard
	ShardStartKey []byte

	//the endKey of the shard
	ShardEndKey []byte

	//the next scan key of the shard
	ShardNextScanKey []byte

	//scan end key for the shard
	ShardScanEndKey []byte

	//finished ?
	CompleteInShard bool

	ReadCnt int

	CountOfWithoutPrefix int
}

func (prc *ParallelReaderContext) Reset() {
	prc.ID = -1
	prc.ShardIndex = -1
	prc.ShardStartKey = nil
	prc.ShardEndKey = nil
	prc.ShardNextScanKey = nil
	prc.ShardScanEndKey = nil
	prc.CompleteInShard = false
	prc.ReadCnt = 0
	prc.CountOfWithoutPrefix = 0
}

func (prc *ParallelReaderContext) Set(id, shardIndex int) {
	prc.ID = id
	prc.ShardIndex = shardIndex
}

func (prc *ParallelReaderContext) SetShardInfo(shardStartKey, shardEndKey, nextScanKeyInShard, scanEndKeyInShard []byte) {
	prc.ShardStartKey = shardStartKey
	prc.ShardEndKey = shardEndKey
	prc.ShardNextScanKey = nextScanKeyInShard
	prc.ShardScanEndKey = scanEndKeyInShard
}

func (prc *ParallelReaderContext) addReadCount(c int) {
	prc.ReadCnt += c
}

func (prc *ParallelReaderContext) getReadCount() int {
	return prc.ReadCnt
}

func (prc *ParallelReaderContext) addCountOfWithoutPrefix(c int) {
	prc.CountOfWithoutPrefix += c
}

func (prc *ParallelReaderContext) getCountOfWithoutPrefix() int {
	return prc.CountOfWithoutPrefix
}

func (prc ParallelReaderContext) String() string {
	return fmt.Sprintf("id %d shardIndex %d readCnt %d CountOfWithoutPrefix %d shardStartKey %v shardNextScanKey %v shardScanEndKey %v shardEndKey %v  completeInShard %v",
		prc.ID,
		prc.ShardIndex,
		prc.ReadCnt,
		prc.CountOfWithoutPrefix,
		prc.ShardStartKey,
		prc.ShardNextScanKey,
		prc.ShardScanEndKey,
		prc.ShardEndKey,
		prc.CompleteInShard,
	)
}

type SingleReaderContext struct {
	//true -- the scanner has scanned all shards
	CompleteInAllShards bool

	//for prefix scan in next time
	PrefixForScanKey []byte

	//the length of the prefix
	LengthOfPrefixForScanKey int

	PrefixEnd []byte
}

type ReadContext struct {
	//target database,table and index
	DbDesc    *descriptor.DatabaseDesc
	TableDesc *descriptor.RelationDesc
	IndexDesc *descriptor.IndexDesc

	//the attributes to be read
	ReadAttributesNames []string

	//the attributes for the read
	ReadAttributeDescs []*descriptor.AttributeDesc

	ParallelReader bool

	MultiNode bool

	//for test
	ReadCount int

	ParallelReaderContext

	SingleReaderContext

	DumpData 	bool // dumpData flag

	Opt	*batch.DumpOption
}

func (rc *ReadContext) AddReadCount() int {
	rc.ReadCount++
	return rc.ReadCount
}
