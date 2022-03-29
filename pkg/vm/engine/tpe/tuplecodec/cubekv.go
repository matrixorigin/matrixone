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
	"context"
	"errors"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
)

const (
	timeout    = 2000 * time.Millisecond
	idPoolSize = 20
)

var (
	errorAllocateIDTimeout                    = errors.New("allocate id timeout")
	errorCanNotComeHere                       = errors.New("can not come here")
	errorIDTypeDoesNotExist                   = errors.New("id type does not exist")
	errorInitIDPoolTimeout                    = errors.New("init id pool is timeout")
	errorCubeDriverIsNull                     = errors.New("cube driver is nil")
	errorInvalidIDPool                        = errors.New("invalid idpool")
	errorInvalidKeyValueCount                 = errors.New("key count != value count")
	errorUnsupportedInCubeKV                  = errors.New("unsupported in cubekv")
	errorPrefixLengthIsLongerThanStartKey     = errors.New("the preifx length is longer than the startKey 1")
	errorRangeIsInvalid                       = errors.New("the range is invalid")
	errorNoKeysToSet                          = errors.New("the count of keys is zero")
	errorAsyncTpeCheckKeysExistGenNilResponse = errors.New("TpeAsyncCheckKeysExist generates nil response")
)
var _ KVHandler = &CubeKV{}

type IDPool struct {
	lock    int32
	idStart uint64
	idEnd   uint64
}

type CubeKV struct {
	Cube                     driver.CubeDriver
	dbIDPool                 IDPool
	tableIDPool              IDPool
	limit                    uint64
	tpeDedupSetBatchTimeout  time.Duration
	tpeDedupSetBatchTryCount int
}

func initIDPool(cd driver.CubeDriver, typ string, pool *IDPool) error {
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
	pool.idStart = MaxUint64(id-idPoolSize+1, UserTableIDOffset)

	if pool.idStart > pool.idEnd {
		return errorInvalidIDPool
	}

	return nil
}

func NewCubeKV(cd driver.CubeDriver, limit uint64, tpeDedupSetBatchTimeout time.Duration, tpeDedupSetBatchTryCount int) (*CubeKV, error) {
	if cd == nil {
		return nil, errorCubeDriverIsNull
	}
	ck := &CubeKV{
		Cube:                     cd,
		limit:                    limit,
		tpeDedupSetBatchTimeout:  tpeDedupSetBatchTimeout,
		tpeDedupSetBatchTryCount: tpeDedupSetBatchTryCount}

	err := initIDPool(cd, DATABASE_ID, &ck.dbIDPool)
	if err != nil {
		return nil, err
	}
	err = initIDPool(cd, TABLE_ID, &ck.dbIDPool)
	if err != nil {
		return nil, err
	}
	return ck, err
}

func (ck *CubeKV) GetKVType() KVType {
	return KV_CUBE
}

// refreshIDPool refreshes the id pool by requesting it from the cube.
func (ck *CubeKV) refreshIDPool(typ string, pool *IDPool) {
	//lock
	if !atomic.CompareAndSwapInt32(&pool.lock, 0, 1) {
		return
	}

	//unlock
	defer func() {
		atomic.StoreInt32(&pool.lock, 0)
	}()

	if pool.idStart <= pool.idEnd {
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)

	callback := func(_ driver.CustomRequest, data []byte, err error) {
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

		atomic.SwapUint64(&pool.idEnd, id)
		atomic.SwapUint64(&pool.idStart, id-idPoolSize+1)
	}

	ck.Cube.AsyncAllocID(catalog.String2Bytes(typ), idPoolSize, callback, nil)
	wg.Wait()
}

// allocateFromPool allocates the id from the pool
func (ck *CubeKV) allocateFromPool(typ string, pool *IDPool) (uint64, error) {
	timeoutC := time.After(timeout)
	var id uint64 = 0
	for {
		select {
		case <-timeoutC:
			return 0, errorAllocateIDTimeout
		default:
			if atomic.LoadInt32(&pool.lock) == 0 {
				id = atomic.AddUint64(&pool.idStart, 1)
				if id <= atomic.LoadUint64(&pool.idEnd) {
					return id, nil
				} else {
					ck.refreshIDPool(typ, pool)
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// allocateID allocates a id for the typ
func (ck *CubeKV) allocateID(typ string) (uint64, error) {
	switch typ {
	case DATABASE_ID:
		return ck.allocateFromPool(typ, &ck.dbIDPool)
	case TABLE_ID:
		return ck.allocateFromPool(typ, &ck.tableIDPool)
	}
	return 0, errorIDTypeDoesNotExist
}

func (ck *CubeKV) NextID(typ string) (uint64, error) {
	return ck.allocateID(typ)
}

func (ck *CubeKV) Set(key TupleKey, value TupleValue) error {
	return ck.Cube.Set(key, value)
}

func (ck *CubeKV) SetBatch(keys []TupleKey, values []TupleValue) error {
	if len(keys) != len(values) {
		return errorInvalidKeyValueCount
	}

	if len(keys) == 0 {
		return errorNoKeysToSet
	}

	var retErr error = nil
	var checkErr int32

	atomic.StoreInt32(&checkErr, 0)

	wg := sync.WaitGroup{}
	wg.Add(len(keys))

	callback := func(req driver.CustomRequest, resp []byte, err error) {
		if err != nil {
			if atomic.CompareAndSwapInt32(&checkErr, 0, 1) {
				retErr = errors.New(string(resp))
			}
			logutil.Errorf("AsyncSetIfNotExist key %v failed. error %v", req, err)
		}

		wg.Done()
	}

	for i, key := range keys {
		ck.Cube.AsyncSet(key, values[i], callback, nil)
	}
	wg.Wait()

	return retErr
}

func (ck *CubeKV) DedupSet(key TupleKey, value TupleValue) error {
	return ck.Cube.SetIfNotExist(key, value)
}

// splitKeysAccordingToShards collects the keys that belongs to the same shard
// return shardID -> keys, the shards and its keys
func (ck *CubeKV) splitKeysAccordingToShards(keys []TupleKey, keysIndexes []int) map[uint64][]int {
	ret := make(map[uint64][]int)
	for _, ki := range keysIndexes {
		shard, _ := ck.Cube.RaftStore().GetRouter().SelectShardWithPolicy(uint64(pb.KVGroup), keys[ki], rpcpb.SelectLeader)
		ret[shard.GetID()] = append(ret[shard.GetID()], ki)
	}
	return ret
}

// collectNeedReRouteKeys collects all keys that from the shards which need to be reroute
func (ck *CubeKV) collectNeedReRouteKeys(needReRouteBoard map[uint64]int8, previousShard2keysIndex map[uint64][]int) []int {
	var ret []int
	for shardID, needReRoute := range needReRouteBoard {
		if needReRoute != 0 {
			ret = append(ret, previousShard2keysIndex[shardID]...)
		}
	}
	return ret
}

// mergeShard2keysIndex merges the src into the dest
func (ck *CubeKV) mergeShard2keysIndexWithoutOrder(dest map[uint64][]int, src map[uint64][]int) {
	for id, s := range src {
		if d, exist := dest[id]; !exist {
			dest[id] = s
		} else {
			//dedup
			var dedup = make(map[int]int8)
			for _, de := range d {
				dedup[de] = 0
			}
			for _, se := range s {
				dedup[se] = 0
			}

			for de, _ := range dedup {
				dest[id] = append(dest[id], de)
			}
		}
	}
}

type checkKeysExistedContext struct {
	//keys that want to be checked
	keys []TupleKey

	//shardID -> [keyIdx1,keyIdx2,...,]
	shard2keysIndex map[uint64][]int

	//cubeDriver
	cube driver.CubeDriver

	//1 - any key existed; 0 - none key existed
	keyExisted               int32
	keyExistedInWhichShardID uint64
	keyExistedIndex          int

	//errors
	//shardID -> error that from checking the shard
	errorsFromCheckShards map[uint64]error

	//timeout
	timeoutFromCheckShards map[uint64]int8

	//needReRoute
	reRouteFromCheckShards map[uint64]int8

	//needWait
	needWait bool

	//wait
	wg sync.WaitGroup
}

func (ckec *checkKeysExistedContext) setCubeDriver(cube driver.CubeDriver) {
	ckec.cube = cube
}

func (ckec *checkKeysExistedContext) setKeys(keys []TupleKey) {
	ckec.keys = keys
}

func (ckec *checkKeysExistedContext) setShard2keysIndex(shard2keysIndex map[uint64][]int) {
	ckec.shard2keysIndex = shard2keysIndex
}

// reset clear all things.
func (ckec *checkKeysExistedContext) reset() {
	ckec.cube = nil
	ckec.resetKeyExisted()
	ckec.errorsFromCheckShards = make(map[uint64]error)
	ckec.timeoutFromCheckShards = make(map[uint64]int8)
	ckec.reRouteFromCheckShards = make(map[uint64]int8)
	ckec.needWait = false
}

func (ckec *checkKeysExistedContext) resetShardError(shardID uint64) {
	ckec.errorsFromCheckShards[shardID] = nil
	ckec.timeoutFromCheckShards[shardID] = 0
	ckec.reRouteFromCheckShards[shardID] = 0
}

// resetKeyExisted reset the label of the existed key
func (ckec *checkKeysExistedContext) resetKeyExisted() {
	atomic.StoreInt32(&ckec.keyExisted, 0)
	ckec.keyExistedInWhichShardID = math.MaxUint64
	ckec.keyExistedIndex = -1
}

// isKeyExisted check any existed or not
func (ckec *checkKeysExistedContext) isKeyExisted() bool {
	return ckec.keyExistedIndex != -1 || ckec.keyExistedInWhichShardID != math.MaxUint64
}

// setKeyExisted set the key existed
func (ckec *checkKeysExistedContext) setKeyExisted(shardID uint64, keyIdx int) {
	if atomic.CompareAndSwapInt32(&ckec.keyExisted, 0, 1) {
		ckec.keyExistedInWhichShardID = shardID
		ckec.keyExistedIndex = keyIdx
	}
}

// collectNeedReRouteKeysIndexes collects all keys from the shards which need to be reroute
func (ckec *checkKeysExistedContext) collectNeedReRouteKeysIndexes() []int {
	var ret []int
	for shardID, needReRoute := range ckec.reRouteFromCheckShards {
		if needReRoute != 0 {
			ret = append(ret, ckec.shard2keysIndex[shardID]...)
		}
	}
	return ret
}

// collectTimeoutShards collects all shards and keysIndexes that are timeout
func (ckec *checkKeysExistedContext) collectTimeoutShardsAndKeysIndexes() ([]uint64, map[uint64][]int) {
	var ret []uint64
	var ret2 = make(map[uint64][]int)
	for shardID, isTimeout := range ckec.timeoutFromCheckShards {
		if isTimeout != 0 {
			ret = append(ret, shardID)
			ret2[shardID] = append(ret2[shardID], ckec.shard2keysIndex[shardID]...)
		}
	}
	return ret, ret2
}

// collectTimeoutShards collects all shards and keysIndexes that are timeout
func (ckec *checkKeysExistedContext) collectTimeoutKeysIndexes() []int {
	var ret []int
	for shardID, isTimeout := range ckec.timeoutFromCheckShards {
		if isTimeout != 0 {
			ret = append(ret, ckec.shard2keysIndex[shardID]...)
		}
	}
	return ret
}

// SetWait sets the wait
func (ckec *checkKeysExistedContext) setNeedWait(w bool) {
	ckec.needWait = w
}

// GetWait gets the wait
func (ckec *checkKeysExistedContext) getNeedWait() bool {
	return ckec.needWait
}

func (ckec *checkKeysExistedContext) AddWait() {
	ckec.wg.Add(1)
}

// wait waits the wait group done
func (ckec *checkKeysExistedContext) wait() {
	ckec.wg.Wait()
}

func (ckec *checkKeysExistedContext) callbackForCheckKeysExisted(cr driver.CustomRequest, resp []byte, cubeErr error) {
	if cubeErr != nil {
		if isTimeoutError(cubeErr) {
			logutil.Errorf("DedupSetBatch cube timeout :%v", cubeErr)
			ckec.timeoutFromCheckShards[cr.ToShard] = 1
		} else if isNeedReRouteError(cubeErr) {
			logutil.Errorf("DedupSetBatch cube needreroute :%v", cubeErr)
			ckec.reRouteFromCheckShards[cr.ToShard] = 1
		} else {
			logutil.Errorf("DedupSetBatch cube error :%v", cubeErr)
		}
		ckec.errorsFromCheckShards[cr.ToShard] = cubeErr
	} else if len(resp) != 0 {
		tce := pb.TpeCheckKeysExistInBatchResponse{}
		protoc.MustUnmarshal(&tce, resp)
		if tce.ExistedKeyIndex != -1 {
			realKeyIndex := ckec.shard2keysIndex[tce.ShardID][tce.ExistedKeyIndex]
			ckec.setKeyExisted(tce.ShardID, realKeyIndex)
			logutil.Errorf("DedupSetBatch response.ExistedKeyIndex %d realKeyIndex %d in shardID %d has key %v ",
				tce.ExistedKeyIndex,
				realKeyIndex,
				tce.ShardID,
				ckec.keys[realKeyIndex])
		}
	} else {
		logutil.Errorf("DedupSetBatch get nil repsonse.")
		ckec.errorsFromCheckShards[cr.ToShard] = errorAsyncTpeCheckKeysExistGenNilResponse
	}
	ckec.wg.Done()
}

// doCheckKeysExists checks the keys
func (ckec *checkKeysExistedContext) doCheckKeysExists(timeout time.Duration) {
	for shardID, keyIdxSlice := range ckec.shard2keysIndex {
		ckec.resetShardError(shardID)
		var shardkeys [][]byte
		for _, keyIdx := range keyIdxSlice {
			shardkeys = append(shardkeys, ckec.keys[keyIdx])
		}
		if len(shardkeys) != 0 {
			ckec.AddWait()
			ckec.cube.TpeAsyncCheckKeysExist(shardID, shardkeys, time.Second*time.Duration(timeout), ckec.callbackForCheckKeysExisted)
		}
	}
}

func (ckec *checkKeysExistedContext) checkErrorsFromCheckShards(needAllError bool) error {
	var e error = nil
	//check errors
	for _, err := range ckec.errorsFromCheckShards {
		if err != nil {
			if needAllError { //return any error includes the ErrTimeout
				e = err
				break
			} else if !(isTimeoutError(err) || isNeedReRouteError(err)) {
				//if there is a error that is not the ErrTimeout and the NeedReRoutError, just return it
				e = err
				break
			}
		}
	}

	return e
}

func isTimeoutError(err error) bool {
	//!!!NOTE: the timeout error is not the raftstore.ErrTimeout
	//return strings.Index(err.Error(), "exec timeout") != -1
	return errors.Is(err, context.DeadlineExceeded)
}

func isNeedReRouteError(err error) bool {
	return errors.Is(err, raftstore.ErrKeysNotInShard) || raftstore.IsShardUnavailableErr(err)
}

func checkErrorsFunc(errs []error, needAllError bool) error {
	var e error = nil
	//check errors
	for _, err := range errs {
		if err != nil {
			if needAllError { //return any error includes the ErrTimeout
				e = err
				break
			} else if !isTimeoutError(err) {
				//if there is a error that is not the ErrTimeout, just return it
				e = err
				break
			}
		}
	}

	return e
}

func checkShardsErrorsFunc(errs map[uint64]error, needAllError bool) error {
	var e error = nil
	//check errors
	for _, err := range errs {
		if err != nil {
			if needAllError { //return any error includes the ErrTimeout
				e = err
				break
			} else if !(isTimeoutError(err) || isNeedReRouteError(err)) {
				//if there is a error that is not the ErrTimeout and the NeedReRoutError, just return it
				e = err
				break
			}
		}
	}

	return e
}

func (ck *CubeKV) DedupSetBatch(keys []TupleKey, values []TupleValue) error {
	if len(keys) != len(values) {
		return errorInvalidKeyValueCount
	}

	if len(keys) == 0 {
		return errorNoKeysToSet
	}

	//1,sort all keys
	keysIndexes := make([]int, len(keys))
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
		preKey := keys[keysIndexes[i-1]]
		curKey := keys[keysIndexes[i]]
		if bytes.Compare(preKey, curKey) == 0 {
			return errorKeyExists
		}
	}

	lastKeyIndexes := keysIndexes

	//get all shards for all keys
	//3.partition keys according to shards
	for itry := 0; itry < ck.tpeDedupSetBatchTryCount; itry++ {
		ctx := &checkKeysExistedContext{}

		//init context
		ctx.reset()
		ctx.setCubeDriver(ck.Cube)
		shard2keysIndex := ck.splitKeysAccordingToShards(keys, lastKeyIndexes)
		ctx.setKeys(keys)
		ctx.setShard2keysIndex(shard2keysIndex)

		//check keys
		ctx.doCheckKeysExists(ck.tpeDedupSetBatchTimeout)

		ctx.wait()

		//check key existed
		if ctx.isKeyExisted() {
			return errorKeyExists
		}

		//check other errors that can not be skipped at last time
		needAllError := (itry == ck.tpeDedupSetBatchTryCount-1)
		err := ctx.checkErrorsFromCheckShards(needAllError)
		if err != nil {
			return err
		}

		needReRouteKeys := ctx.collectNeedReRouteKeysIndexes()
		timeoutKeys := ctx.collectTimeoutKeysIndexes()
		lastKeyIndexes = nil
		lastKeyIndexes = append(lastKeyIndexes, needReRouteKeys...)
		lastKeyIndexes = append(lastKeyIndexes, timeoutKeys...)
		if len(lastKeyIndexes) == 0 {
			break
		}
	}

	shard2keysIndex := ck.splitKeysAccordingToShards(keys, keysIndexes)

	//5.AsyncSet keys into the storage
	configTryCount := ck.tpeDedupSetBatchTryCount
	//The plan A : async write batch
	planA := true

	if planA {
		setKeysErrors := make(map[uint64]error)
		setKeysTimeoutBoard := make(map[uint64]int8)
		var setKeysRetErr error = nil

		setKeysWg := sync.WaitGroup{}

		callbackAsyncSetKeys := func(cr driver.CustomRequest, resp []byte, cubeErr error) {
			if cubeErr != nil {
				if isTimeoutError(cubeErr) {
					logutil.Errorf("DedupSetBatch asyncSetKeys cube timeout :%v", cubeErr)
					setKeysTimeoutBoard[cr.ToShard] = 1
				} else {
					logutil.Errorf("DedupSetBatch asyncSetKeys cube error :%v", cubeErr)
				}
				setKeysErrors[cr.ToShard] = cubeErr
			}
			setKeysWg.Done()
		}

		for shardID, keyIdxSlice := range shard2keysIndex {
			setKeysErrors[shardID] = nil
			setKeysTimeoutBoard[shardID] = 0
			var shardkeys [][]byte
			var shardvalues [][]byte
			for _, keyIdx := range keyIdxSlice {
				shardkeys = append(shardkeys, keys[keyIdx])
				shardvalues = append(shardvalues, values[keyIdx])
			}
			if len(shardkeys) != 0 {
				setKeysWg.Add(1)
				ck.Cube.TpeAsyncSetKeysValuesInbatch(shardID, shardkeys, shardvalues, time.Second*time.Duration(ck.tpeDedupSetBatchTimeout), callbackAsyncSetKeys)
			}
		}
		setKeysWg.Wait()

		setKeysRetErr = checkShardsErrorsFunc(setKeysErrors, false)
		if setKeysRetErr != nil {
			return setKeysRetErr
		}

		//try another times
		for try := 1; try < int(configTryCount); try++ {
			needWait := false
			for shardID, keyIdxSlice := range shard2keysIndex {
				setKeysErrors[shardID] = nil
				if setKeysTimeoutBoard[shardID] != 0 {
					setKeysTimeoutBoard[shardID] = 0

					var shardkeys [][]byte
					var shardvalues [][]byte
					for _, keyIdx := range keyIdxSlice {
						shardkeys = append(shardkeys, keys[keyIdx])
						shardvalues = append(shardvalues, values[keyIdx])
					}
					if len(shardkeys) != 0 {
						needWait = true
						setKeysWg.Add(1)
						ck.Cube.TpeAsyncSetKeysValuesInbatch(shardID, shardkeys, shardvalues, time.Second*time.Duration(ck.tpeDedupSetBatchTimeout), callbackAsyncSetKeys)
					}
				}
			}
			if needWait {
				logutil.Infof("async_setkeys_wait_try %d times", try+1)
				setKeysWg.Wait()
				setKeysRetErr = checkShardsErrorsFunc(setKeysErrors, false)
				if setKeysRetErr != nil {
					return setKeysRetErr
				}
			} else {
				logutil.Infof("async_setkeys_wait_done after try %d times", try+1)
				break
			}
		}

		setKeysRetErr = checkShardsErrorsFunc(setKeysErrors, true)
		if setKeysRetErr != nil {
			return setKeysRetErr
		}
		return setKeysRetErr
	} else {
		//The plan B below : async write key one by one

		var retErr error = nil

		keysErrors := make([]error, len(keys))

		//try the first time
		wg := sync.WaitGroup{}
		wg.Add(len(keys))

		keysTimeoutBoard := make([]int8, len(keys))

		callbackAsyncSet := func(req driver.CustomRequest, resp []byte, err error) {
			if err != nil {
				//unmarshal request
				setReq := &pb.SetRequest{}
				protoc.MustUnmarshal(setReq, req.Cmd)

				keysErrors[setReq.GetKeyIndex()] = err

				if isTimeoutError(err) {
					logutil.Errorf("AsyncSet key %v timeout. error %v", req.Key, err)
					keysTimeoutBoard[setReq.GetKeyIndex()] = 1
				} else {
					logutil.Errorf("AsyncSet key %v failed. error %v", req.Key, err)
				}
			}

			wg.Done()
		}

		for i, key := range keys {
			ck.Cube.TpeAsyncSet(key, values[i], i, time.Second*time.Duration(ck.tpeDedupSetBatchTimeout), callbackAsyncSet, nil)
		}
		wg.Wait()

		retErr = checkErrorsFunc(keysErrors, false)
		if retErr != nil {
			return retErr
		}

		//try another times
		for try := 1; try < int(configTryCount); try++ {
			//2. async request again
			needWait := false
			for i, key := range keys {
				//reset error
				keysErrors[i] = nil
				if keysTimeoutBoard[i] != 0 {
					needWait = true
					wg.Add(1)
					//!!!Note: reset before asyncset
					keysTimeoutBoard[i] = 0
					ck.Cube.TpeAsyncSet(key, values[i], i, time.Second*time.Duration(ck.tpeDedupSetBatchTimeout), callbackAsyncSet, nil)
				}
			}

			//3. wait request to be done
			if needWait {
				logutil.Infof("wait_try %d times", try+1)
				wg.Wait()

				retErr = checkErrorsFunc(keysErrors, false)
				if retErr != nil {
					return retErr
				}
			} else {
				logutil.Infof("wait_done after try %d times", try+1)
				break
			}
		}
		retErr = checkErrorsFunc(keysErrors, true)
		if retErr != nil {
			return retErr
		}
		return retErr
	}
}

func (ck *CubeKV) Delete(key TupleKey) error {
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

	shards, ok := ret.(*Shards)
	if !ok {
		return ErrorIsNotShards
	}

	//shrink [start,end) according to the shard.
	adjustRange := func(start, end []byte, shardStart, shardEnd []byte) ([]byte, []byte) {
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
		startKey, endKey := adjustRange(prefix, prefixEnd, info.startKey, info.endKey)
		logutil.Infof("delete range %v,%v", startKey, endKey)
		err = ck.Cube.TpeDeleteBatchWithRange(startKey, endKey)
		if err != nil {
			return err
		}
	}
	return nil
}

// Get gets the value of the key.
// If the key does not exist, it returns the null
func (ck *CubeKV) Get(key TupleKey) (TupleValue, error) {
	return ck.Cube.Get(key)
}

func (ck *CubeKV) GetBatch(keys []TupleKey) ([]TupleValue, error) {
	var values []TupleValue
	for _, key := range keys {
		get, err := ck.Get(key)
		if err != nil {
			return nil, err
		}
		values = append(values, get)
	}
	return values, nil
}

func (ck *CubeKV) GetRange(startKey TupleKey, endKey TupleKey) ([]TupleValue, error) {
	var values []TupleValue
	lastKey := startKey
	for {
		_, retValues, complete, nextScanKey, err := ck.Cube.TpeScan(lastKey, endKey, nil, math.MaxUint64, false)
		if err != nil {
			return nil, err
		}

		for i := 0; i < len(retValues); i++ {
			values = append(values, retValues[i])
		}

		lastKey = nextScanKey
		//all shards has been scanned
		if complete {
			break
		}
	}

	return values, nil
}

func (ck *CubeKV) GetRangeWithLimit(startKey TupleKey, endKey TupleKey, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error) {
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
		scanKeys, scanValues, complete, nextScanKey, err = ck.Cube.TpeScan(lastKey, endKey, nil, needCnt, true)
		if err != nil {
			return nil, nil, false, nil, err
		}

		readCnt += uint64(len(scanKeys))

		for i := 0; i < len(scanKeys); i++ {
			keys = append(keys, scanKeys[i])
			values = append(values, scanValues[i])
		}

		lastKey = nextScanKey
		//all shards has been scanned
		if complete {
			break
		}
	}

	return keys, values, complete, nextScanKey, err
}

func (ck *CubeKV) GetRangeWithPrefixLimit(startKey TupleKey, endKey TupleKey, prefix TupleKey, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error) {
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
		scanKeys, scanValues, complete, nextScanKey, err = ck.Cube.TpeScan(lastKey, endKey, prefix, needCnt, true)
		if err != nil {
			return nil, nil, false, nil, err
		}

		readCnt += uint64(len(scanKeys))

		for i := 0; i < len(scanKeys); i++ {
			keys = append(keys, scanKeys[i])
			values = append(values, scanValues[i])
		}

		lastKey = nextScanKey
		//all shards has been scanned
		if complete {
			break
		}
	}

	return keys, values, complete, nextScanKey, err
}

func (ck *CubeKV) GetWithPrefix(prefixOrStartkey TupleKey, prefixLen int, prefixEnd []byte, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error) {
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

	realPrefix := prefixOrStartkey[:prefixLen]
	lastKey := prefixOrStartkey
	readCnt := uint64(0)
	complete := false

	for readCnt < limit {
		needCnt := limit - readCnt
		if len(lastKey) < prefixLen || !bytes.HasPrefix(lastKey, realPrefix) {
			//the lastKey does not has the prefix anymore.
			//There are no keys started with the prefix in the rest of the shards.
			//quit
			complete = true
			logutil.Warnf("the lastKey does not has the prefix anymore. quit")
			break
		}
		scanKeys, scanValues, complete, nextScanKey, err = ck.Cube.TpePrefixScan(lastKey, prefixLen, prefixEnd, needCnt)
		if err != nil {
			return nil, nil, false, nil, err
		}

		readCnt += uint64(len(scanKeys))

		for i := 0; i < len(scanKeys); i++ {
			keys = append(keys, scanKeys[i])
			values = append(values, scanValues[i])
		}

		lastKey = nextScanKey
		if complete {
			break
		}
	}

	return keys, values, complete, nextScanKey, err
}

func (ck *CubeKV) GetShardsWithRange(startKey TupleKey, endKey TupleKey) (interface{}, error) {
	wantRange := Range{startKey: startKey, endKey: endKey}

	var shardInfos []ShardInfo
	var stores = make(map[uint64]string)

	callback := func(shard metapb.Shard, store metapb.Store) bool {
		logutil.Infof("originshardinfo %v %v", shard.GetStart(), shard.GetEnd())
		//the shard overlaps the [startKey,endKey)
		checkRange := Range{startKey: shard.GetStart(), endKey: shard.GetEnd()}

		ok, err := isOverlap(wantRange, checkRange)
		if err != nil {
			logutil.Errorf("wantRange or checkRange may be invalid")
		}

		if ok {
			if _, exist := stores[store.ID]; !exist {
				stores[store.ID] = store.ClientAddress
			}
			shardInfos = append(shardInfos, ShardInfo{
				startKey: shard.GetStart(),
				endKey:   shard.GetEnd(),
				node: ShardNode{
					Addr:    store.ClientAddress,
					IDbytes: string(codec.Uint642Bytes(store.ID)),
					ID:      store.ID,
				},
			})

			info := shardInfos[len(shardInfos)-1]

			logutil.Infof("shardinfo startKey %v endKey %v", info.GetStartKey(), info.GetEndKey())
		}

		return true
	}

	ck.Cube.RaftStore().GetRouter().Every(uint64(pb.KVGroup), true, callback)

	var nodes []ShardNode
	for id, addr := range stores {
		nodes = append(nodes, ShardNode{
			Addr:    addr,
			IDbytes: string(codec.Uint642Bytes(id)),
			ID:      id,
		})
	}

	if len(nodes) == 0 {
		logutil.Warnf("there are no nodes hold the range [%v %v)", startKey, endKey)
	}

	logutil.Infof("shardinfo count %d ", len(shardInfos))

	sd := &Shards{
		nodes:      nodes,
		shardInfos: shardInfos,
	}
	return sd, nil
}

func (ck *CubeKV) GetShardsWithPrefix(prefix TupleKey) (interface{}, error) {
	prefixEnd := SuccessorOfPrefix(prefix)
	return ck.GetShardsWithRange(prefix, prefixEnd)
}
