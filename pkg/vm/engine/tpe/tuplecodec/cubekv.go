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
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"math"
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

func NewCubeKV(cd driver.CubeDriver) (*CubeKV,error) {
	if cd == nil {
		return nil, errorCubeDriverIsNull
	}
	ck := &CubeKV{Cube: cd}
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

func (ck * CubeKV) SetBatch(keys []TupleKey, values []TupleValue) []error {
	var errs []error
	for i, key := range keys {
		err := ck.Set(key,values[i])
		errs = append(errs,err)
	}
	return errs
}

func (ck * CubeKV) DedupSet(key TupleKey, value TupleValue) error {
	return ck.Cube.SetIfNotExist(key, value)
}

func (ck * CubeKV) DedupSetBatch(keys []TupleKey, values []TupleValue) []error {
	var errs []error
	for i, key := range keys {
		err := ck.DedupSet(key,values[i])
		errs = append(errs,err)
	}
	return errs
}

func (ck * CubeKV) Delete(key TupleKey) error {
	return ck.Cube.Delete(key)
}

func (ck *CubeKV) DeleteWithPrefix(prefix TupleKey) error {
	if prefix == nil {
		return errorPrefixIsNull
	}
	return errorUnsupportedInCubeKV
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
	_, retValues, _, _, err := ck.Cube.TpeScan(startKey, endKey, math.MaxUint64,false)
	if err != nil {
		return nil, err
	}
	var values []TupleValue
	for i := 0 ; i < len(retValues); i ++ {
		values = append(values,retValues[i])
	}
	return values,err
}

func (ck * CubeKV) GetRangeWithLimit(startKey TupleKey, endKey TupleKey, limit uint64) ([]TupleKey, []TupleValue, error) {
	scanKeys, scanValues, _, _, err := ck.Cube.TpeScan(startKey, endKey, limit, true)
	if err != nil {
		return nil,nil, err
	}
	var keys []TupleKey
	var values []TupleValue
	for i := 0 ; i < len(scanKeys); i ++{
		keys = append(keys,scanKeys[i])
		values = append(values,scanValues[i])
	}
	return keys,values,err
}

func (ck * CubeKV) GetWithPrefix(prefixOrStartkey TupleKey, prefixLen int, limit uint64) ([]TupleKey, []TupleValue, error) {
	if prefixOrStartkey == nil {
		return nil, nil, errorPrefixIsNull
	}

	if prefixLen > len(prefixOrStartkey) {
		return nil, nil, errorPrefixLengthIsLongerThanStartKey
	}

	scanKeys, scanValues, _, _, err := ck.Cube.TpePrefixScan(prefixOrStartkey,prefixLen,limit)
	if err != nil {
		return nil, nil, err
	}
	var keys []TupleKey
	var values []TupleValue
	for i := 0 ; i < len(scanKeys); i ++{
		keys = append(keys,scanKeys[i])
		values = append(values,scanValues[i])
	}

	return keys,values,err
}

func (ck * CubeKV) GetShardsWithRange(startKey TupleKey, endKey TupleKey) (interface{}, error) {
	wantRange := Range{startKey: startKey,endKey: endKey}

	isInfinity := func(in []byte) bool {
		return len(in) == 0
	}

	wantNegativeInfinity := isInfinity(wantRange.startKey)
	wantPositiveInfinity := isInfinity(wantRange.endKey)

	var shardInfos []ShardInfo
	var stores map[uint64]string

	callback := func(shard meta.Shard, store meta.Store) bool {
		//the shard overlaps the [startKey,endKey)
		checkRange := Range{startKey: shard.GetStart(), endKey: shard.GetEnd()}
		checkNegativeInfinity := isInfinity(checkRange.startKey)
		checkPositiveInfinity := isInfinity(checkRange.endKey)
		ok := false

		if wantNegativeInfinity && wantPositiveInfinity {
			//(-infinity,+infinity)
			ok = true
		}else if wantNegativeInfinity && !wantPositiveInfinity {
			//(-infinity,x)
			if checkNegativeInfinity && checkPositiveInfinity {
				ok = true
			}else if checkNegativeInfinity && !checkPositiveInfinity {
				//(-infinity,y)
				// x<=y, or x > y
				ok = true
			}else if !checkNegativeInfinity && checkPositiveInfinity {
				//[y,infinity)
				ok = bytes.Compare(wantRange.endKey,checkRange.startKey) > 0
			}else{
				//[y,z)
				ok = bytes.Compare(wantRange.endKey,checkRange.startKey) > 0
			}
		}else if !wantNegativeInfinity && wantPositiveInfinity {
			//[x,infinity)
			if checkNegativeInfinity && checkPositiveInfinity {
				ok = true
			}else if checkNegativeInfinity && !checkPositiveInfinity {
				//(-infinity,y)
				ok = bytes.Compare(wantRange.startKey,checkRange.endKey) < 0
			}else if !checkNegativeInfinity && checkPositiveInfinity {
				//[y,infinity)
				ok = true
			}else{
				//[y,z)
				ok = bytes.Compare(wantRange.startKey,checkRange.endKey) < 0
			}
		}else{
			//[a, b)
			if checkNegativeInfinity && checkPositiveInfinity {
				ok = true
			}else if checkNegativeInfinity && !checkPositiveInfinity {
				//(-infinity,y)
				ok = bytes.Compare(wantRange.startKey,checkRange.endKey) < 0
			}else if !checkNegativeInfinity && checkPositiveInfinity {
				//[y,infinity)
				ok = bytes.Compare(wantRange.endKey,checkRange.startKey) > 0
			}else{
				//[y,z)
				ok = bytes.Compare(wantRange.endKey,checkRange.startKey) > 0 &&
						bytes.Compare(wantRange.endKey,checkRange.endKey) <= 0 ||
					bytes.Compare(wantRange.startKey,checkRange.startKey) >= 0 &&
						bytes.Compare(wantRange.startKey,checkRange.endKey) < 0
			}
		}

		if ok {
			if _,exist := stores[store.ID]; !exist {
				stores[store.ID] = store.ClientAddr
			}
			shardInfos = append(shardInfos,ShardInfo{
				startKey: shard.GetStart(),
				endKey:   shard.GetEnd(),
				node:     ShardNode{
					Addr: store.ClientAddr,
					ID: string(codec.Uint642Bytes(store.ID)),
				},
			})
		}

		return true
	}
	ck.Cube.RaftStore().GetRouter().Every(uint64(pb.KVGroup),true,callback)

	var nodes []ShardNode
	for id, addr := range stores {
		nodes = append(nodes,ShardNode{
			Addr: addr,
			ID:   string(codec.Uint642Bytes(id)),
		})
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


