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
	"encoding/json"
	"errors"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	timeout               = 2000 * time.Millisecond
	idPoolSize            = 20
)

var (
	errorAllocateIDTimeout = errors.New("allocate id timeout")
	errorCanNotComeHere = errors.New("can not come here")
	errorIDTypeDoesNotExist = errors.New("id type does not exist")
	errorInitIDPoolTimeout = errors.New("init id pool is timeout")
	errorCubeDriverIsNull = errors.New("cube driver is nil")
	errorInvalidIDPool = errors.New("invalid idpool")
	errorInvalidKeyValueCount = errors.New("key count != value count")
	errorUnsupportedInCubeKV = errors.New("unsupported in cubekv")
	errorPrefixLengthIsLongerThanStartKey = errors.New("the preifx length is longer than the startKey")
	errorRangeIsInvalid = errors.New("the range is invalid")
	errorNoKeysToSet = errors.New("the count of keys is zero")
	errorAsyncTpeCheckKeysExistGenNilResponse = errors.New("AsyncTpeCheckKeysExist generates nil response")
)
var _ KVHandler = &CubeKV{}

type IDPool struct {
	lock int32
	idStart uint64
	idEnd uint64
}

type CubeKV struct {
	Cube    driver.CubeDriver
	dbIDPool    IDPool
	tableIDPool IDPool
	limit uint64
}

func initIDPool(cd driver.CubeDriver, typ string,pool *IDPool) error {
	var id uint64
	var err error
	waitTime := time.After(timeout * 10)
	for {
		quit := false
		select {
		case <-waitTime:
			return errorInitIDPoolTimeout
		default:
			id, err = cd.AllocID(catalog.String2Bytes(typ), idPoolSize)
			if err == nil {
				quit = true
			}
		}
		if quit {
			break
		}
		time.Sleep(timeout / 2)
	}

	pool.lock = 0
	pool.idEnd = id
	pool.idStart = MaxUint64(id - idPoolSize +1, UserTableIDOffset)

	if pool.idStart > pool.idEnd {
		return errorInvalidIDPool
	}

	return nil
}

func NewCubeKV(cd driver.CubeDriver, limit uint64) (*CubeKV, error) {
	if cd == nil {
		return nil, errorCubeDriverIsNull
	}
	ck := &CubeKV{Cube: cd, limit: limit}
	err := initIDPool(cd,DATABASE_ID,&ck.dbIDPool)
	if err != nil {
		return nil, err
	}
	err = initIDPool(cd,TABLE_ID,&ck.dbIDPool)
	if err != nil {
		return nil, err
	}
	return ck, err
}

func (ck * CubeKV) GetKVType() KVType {
	return KV_CUBE
}

// refreshIDPool refreshes the id pool by requesting it from the cube.
func (ck *CubeKV) refreshIDPool(typ string,pool *IDPool) {
	//lock
	if !atomic.CompareAndSwapInt32(&pool.lock,0,1) {
		return
	}

	//unlock
	defer func() {
		atomic.StoreInt32(&pool.lock,0)
	}()

	if pool.idStart <= pool.idEnd {
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)

	callback := func(_ server.CustomRequest, data []byte, err error) {
		defer wg.Done()
		if err != nil {
			logutil.Errorf("refresh db id failed, checkpoint is %d, %d", pool.idStart, pool.idEnd)
			return
		}

		id, err := catalog.Bytes2Uint64(data)
		if err != nil {
			logutil.Errorf("get result of AllocId failed, %v\n", err)
			return
		}

		atomic.SwapUint64(&pool.idEnd,id)
		atomic.SwapUint64(&pool.idStart, id - idPoolSize + 1)
	}

	ck.Cube.AsyncAllocID(catalog.String2Bytes(typ),idPoolSize,callback,nil)
	wg.Wait()
}

// allocateFromPool allocates the id from the pool
func (ck * CubeKV) allocateFromPool(typ string,pool *IDPool) (uint64,error) {
	timeoutC := time.After(timeout)
	var id uint64 = 0
	for {
		select {
		case <- timeoutC:
			return 0, errorAllocateIDTimeout
		default:
			if atomic.LoadInt32(&pool.lock) == 0 {
				id = atomic.AddUint64(&pool.idStart,1)
				if id <= atomic.LoadUint64(&pool.idEnd) {
					return id, nil
				}else{
					ck.refreshIDPool(typ,pool)
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
	return id, errorCanNotComeHere
}

// allocateID allocates a id for the typ
func (ck * CubeKV) allocateID(typ string) (uint64,error)  {
	switch typ {
	case DATABASE_ID:
		return ck.allocateFromPool(typ,&ck.dbIDPool)
	case TABLE_ID:
		return ck.allocateFromPool(typ,&ck.tableIDPool)
	}
	return 0, errorIDTypeDoesNotExist
}

func (ck * CubeKV) NextID(typ string) (uint64, error) {
	return ck.allocateID(typ)
}

func (ck * CubeKV) Set(key TupleKey, value TupleValue) error {
	return ck.Cube.Set(key,value)
}

func (ck * CubeKV) SetBatch(keys []TupleKey, values []TupleValue) error {
	if len(keys) != len(values) {
		return errorInvalidKeyValueCount
	}

	if len(keys) == 0 {
		return errorNoKeysToSet
	}

	var retErr error = nil
	var checkErr int32

	atomic.StoreInt32(&checkErr,0)

	wg := sync.WaitGroup{}
	wg.Add(len(keys))

	callback := func(req server.CustomRequest,resp []byte, err error) {
		if err != nil {
			if atomic.CompareAndSwapInt32(&checkErr,0,1) {
				retErr = errors.New(string(resp))
			}
			logutil.Errorf("AsyncSetIfNotExist key %v failed. error %v",req,err)
		}

		wg.Done()
	}

	for i, key := range keys {
		ck.Cube.AsyncSet(key,values[i],callback,nil)
	}
	wg.Wait()

	return retErr
}

func (ck * CubeKV) DedupSet(key TupleKey, value TupleValue) error {
	return ck.Cube.SetIfNotExist(key, value)
}

func (ck * CubeKV) DedupSetBatch(keys []TupleKey, values []TupleValue) error {
	if len(keys) != len(values) {
		return errorInvalidKeyValueCount
	}

	if len(keys) == 0 {
		return errorNoKeysToSet
	}

	//1,sort all keys
	keysIndexes := make([]int,len(keys))
	for i := 0; i < len(keysIndexes); i++ {
		keysIndexes[i] = i
	}

	sort.Slice(keysIndexes, func(i, j int) bool {
		ki := keysIndexes[i]
		kj := keysIndexes[j]

		return keys[ki].Less(keys[kj])
	})

	//2. check key duplicate in the request
	for i := 1; i < len(keysIndexes); i++ {
		preKey := keys[keysIndexes[i - 1]]
		curKey := keys[keysIndexes[i]]
		if bytes.Compare(preKey,curKey) == 0 {
			return errorKeyExists
		}
	}

	//get all shards for all keys
	//3.partition keys according to shards
	shard2keysIndex := make(map[uint64][]int)
	for _, ki := range keysIndexes {
		shard, _ := ck.Cube.RaftStore().GetRouter().SelectShard(uint64(pb.KVGroup),keys[ki])
		shard2keysIndex[shard.GetID()] = append(shard2keysIndex[shard.GetID()],ki)
	}

	keyExisted := int32(0)
	atomic.StoreInt32(&keyExisted,0)
	var keyExistedShardID uint64 = math.MaxUint64
	var keyExistedIndex int = -1
	setKeyExistedFunc := func(shardID uint64,keyIdx int) {
		if atomic.CompareAndSwapInt32(&keyExisted,0,1) {
			keyExistedShardID = shardID
			keyExistedIndex = keyIdx
		}
	}

	hasErr := int32(0)
	atomic.StoreInt32(&hasErr, 0)
	var checkKeysExistedErr error
	setErrFunc := func(err error) {
		if err != nil {
			if atomic.CompareAndSwapInt32(&hasErr,0,1) {
				checkKeysExistedErr = err
			}
		}
	}

	checkKeysExistedWG := sync.WaitGroup{}
	callbackCheckKeysExisted := func(cr server.CustomRequest, resp []byte, cubeErr error) {
		setErrFunc(cubeErr)
		if cubeErr != nil {
			if errors.Is(cubeErr,raftstore.ErrTimeout) {
				logutil.Errorf("DedupSetBatch cube timeout :%v",cubeErr)
			}else{
				logutil.Errorf("DedupSetBatch cube error :%v",cubeErr)
			}
		}

		if len(resp) != 0 {
			tce := driver.TpeCheckKeysExistInBatchResponse{}
			err := json.Unmarshal(resp, &tce)
			setErrFunc(err)
			if err != nil {
				logutil.Errorf("DedupSetBatch unmarshal response failed.err:%v",err)
				return
			}else{
				if tce.ExistedKeyIndex != -1 {
					realKeyIndex := shard2keysIndex[tce.ShardID][tce.ExistedKeyIndex]
					setKeyExistedFunc(tce.ShardID,realKeyIndex)
					logutil.Errorf("DedupSetBatch response.ExistedKeyIndex %d realKeyIndex %d in shardID %d has key %v ",
						tce.ExistedKeyIndex,
						realKeyIndex,
						tce.ShardID,
						keys[realKeyIndex])
				}
			}
		}else{
			logutil.Errorf("DedupSetBatch get nil repsonse.")
			setErrFunc(errorAsyncTpeCheckKeysExistGenNilResponse)
		}
		checkKeysExistedWG.Done()
	}

	//4.check keys exist or not in the every shard
	for shardID, keyIdxSlice := range shard2keysIndex {
		var shardkeys [][]byte
		for _, keyIdx := range keyIdxSlice {
			shardkeys = append(shardkeys,keys[keyIdx])
		}
		if len(shardkeys) != 0 {
			checkKeysExistedWG.Add(1)
			ck.Cube.AsyncTpeCheckKeysExist(shardID,shardkeys,callbackCheckKeysExisted)
		}
	}

	checkKeysExistedWG.Wait()

	if checkKeysExistedErr != nil {
		return checkKeysExistedErr
	}

	if keyExistedIndex != -1 || keyExistedShardID != math.MaxUint64 {
		return errorKeyExists
	}

	//5.AsyncSet keys into the storage

	var retErr error = nil
	var checkErr int32

	atomic.StoreInt32(&checkErr,0)

	wg := sync.WaitGroup{}
	wg.Add(len(keys))

	callback := func(req server.CustomRequest,resp []byte, err error) {
		if err != nil {
			if atomic.CompareAndSwapInt32(&checkErr,0,1) {
				retErr = errors.New(string(resp))
			}
			if errors.Is(err,raftstore.ErrTimeout) {
				logutil.Errorf("AsyncSet key %v timeout. error %v",req.Key,err)
			}else{
				logutil.Errorf("AsyncSet key %v failed. error %v",req.Key,err)
			}
		}

		wg.Done()
	}

	for i, key := range keys {
		ck.Cube.AsyncSet(key,values[i],callback,nil)
	}
	wg.Wait()
	return retErr
}

func (ck * CubeKV) Delete(key TupleKey) error {
	return ck.Cube.Delete(key)
}

func (ck *CubeKV) DeleteWithPrefix(prefix TupleKey) error {
	if prefix == nil {
		return errorPrefixIsNull
	}

	ret, err := ck.GetShardsWithPrefix(prefix)
	if err != nil {
		return err
	}

	shards,ok := ret.(*Shards)
	if !ok {
		return ErrorIsNotShards
	}

	//shrink [start,end) according to the shard.
	adjustRange := func (start, end []byte, shardStart, shardEnd []byte) ([]byte, []byte) {
		if len(shardStart) > 0 && bytes.Compare(start, shardStart) < 0 {
			start = shardStart
		}

		if len(shardEnd) > 0 && bytes.Compare(end, shardEnd) > 0 {
			end = shardEnd
		}
		return start, end
	}

	prefixEnd := SuccessorOfPrefix(prefix)

	for _, info := range shards.shardInfos {
		startKey,endKey := adjustRange(prefix,prefixEnd,info.startKey,info.endKey)
		logutil.Infof("delete range %v,%v",startKey,endKey)
		err = ck.Cube.TpeDeleteBatchWithRange(startKey,endKey)
		if err != nil {
			return err
		}
	}
	return nil
}

// Get gets the value of the key.
// If the key does not exist, it returns the null
func (ck * CubeKV) Get(key TupleKey) (TupleValue, error) {
	return ck.Cube.Get(key)
}

func (ck * CubeKV) GetBatch(keys []TupleKey) ([]TupleValue, error) {
	var values []TupleValue
	for _, key := range keys {
		get, err := ck.Get(key)
		if err != nil {
			return nil, err
		}
		values = append(values,get)
	}
	return values, nil
}

func (ck * CubeKV) GetRange(startKey TupleKey, endKey TupleKey) ([]TupleValue, error) {
	var values []TupleValue
	lastKey := startKey
	for {
		_, retValues, complete, nextScanKey, err := ck.Cube.TpeScan(lastKey, endKey, math.MaxUint64,false)
		if err != nil {
			return nil, err
		}

		for i := 0 ; i < len(retValues); i ++ {
			values = append(values,retValues[i])
		}

		lastKey = nextScanKey
		//all shards has been scanned
		if complete {
			break
		}
	}

	return values, nil
}

func (ck * CubeKV) GetRangeWithLimit(startKey TupleKey, endKey TupleKey, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error) {
	var keys []TupleKey
	var values []TupleValue
	var scanKeys [][]byte
	var scanValues [][]byte
	var nextScanKey []byte
	var err error
	lastKey := startKey
	readCnt := uint64(0)
	complete := false

	for readCnt < limit {
		needCnt := limit - readCnt
		scanKeys, scanValues, complete, nextScanKey, err = ck.Cube.TpeScan(lastKey, endKey, needCnt, true)
		if err != nil {
			return nil, nil, false, nil, err
		}

		readCnt += uint64(len(scanKeys))

		for i := 0 ; i < len(scanKeys); i ++{
			keys = append(keys,scanKeys[i])
			values = append(values,scanValues[i])
		}

		lastKey = nextScanKey
		//all shards has been scanned
		if complete {
			break
		}
	}

	return keys, values, complete, nextScanKey, err
}

func (ck * CubeKV) GetWithPrefix(prefixOrStartkey TupleKey, prefixLen int, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error) {
	if prefixOrStartkey == nil {
		return nil, nil, false, nil, errorPrefixIsNull
	}

	if prefixLen > len(prefixOrStartkey) {
		return nil, nil, false, nil, errorPrefixLengthIsLongerThanStartKey
	}

	var keys []TupleKey
	var values []TupleValue
	var scanKeys [][]byte
	var scanValues [][]byte
	var nextScanKey []byte
	var err error

	lastKey := prefixOrStartkey
	readCnt := uint64(0)
	complete := false

	for readCnt < limit {
		needCnt := limit - readCnt
		scanKeys, scanValues, complete, nextScanKey, err = ck.Cube.TpePrefixScan(lastKey,prefixLen,needCnt)
		if err != nil {
			return nil, nil, false, nil, err
		}

		readCnt += uint64(len(scanKeys))

		for i := 0 ; i < len(scanKeys); i ++{
			keys = append(keys,scanKeys[i])
			values = append(values,scanValues[i])
		}

		lastKey = nextScanKey
		if complete {
			break
		}
	}

	return keys, values, complete, nextScanKey, err
}

func (ck * CubeKV) GetShardsWithRange(startKey TupleKey, endKey TupleKey) (interface{}, error) {
	wantRange := Range{startKey: startKey,endKey: endKey}

	var shardInfos []ShardInfo
	var stores = make(map[uint64]string)

	callback := func(shard meta.Shard, store meta.Store) bool {
		//the shard overlaps the [startKey,endKey)
		checkRange := Range{startKey: shard.GetStart(), endKey: shard.GetEnd()}

		ok, err := isOverlap(wantRange,checkRange)
		if err != nil {
			logutil.Errorf("wantRange or checkRange may be invalid")
		}

		if ok {
			if _,exist := stores[store.ID]; !exist {
				stores[store.ID] = store.ClientAddr
			}
			shardInfos = append(shardInfos,ShardInfo{
				startKey: shard.GetStart(),
				endKey:   shard.GetEnd(),
				node:     ShardNode{
					Addr:    store.ClientAddr,
					IDbytes: string(codec.Uint642Bytes(store.ID)),
					ID: store.ID,
				},
			})
		}

		return true
	}

	ck.Cube.RaftStore().GetRouter().Every(uint64(pb.KVGroup),true,callback)

	var nodes []ShardNode
	for id, addr := range stores {
		nodes = append(nodes,ShardNode{
			Addr:    addr,
			IDbytes: string(codec.Uint642Bytes(id)),
			ID: id,
		})
	}

	if len(nodes) == 0 {
		logutil.Warnf("there are no nodes hold the range [%v %v)",startKey,endKey)
	}

	sd := &Shards{
		nodes:      nodes,
		shardInfos: shardInfos,
	}
	return sd, nil
}

func (ck * CubeKV) GetShardsWithPrefix(prefix TupleKey) (interface{}, error) {
	prefixEnd := SuccessorOfPrefix(prefix)
	return ck.GetShardsWithRange(prefix,prefixEnd)
}


