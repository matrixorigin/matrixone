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
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
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
	ret, err := ck.Cube.Scan(startKey, endKey, math.MaxUint64)
	if err != nil {
		return nil, err
	}
	var values []TupleValue
	//ret[even index] is key
	//ret[odd index] is value
	for i := 1 ; i < len(ret); i += 2 {
		values = append(values,ret[i])
	}
	return values,err
}

func (ck * CubeKV) GetRangeWithLimit(startKey TupleKey, limit uint64) ([]TupleKey, []TupleValue, error) {
	ret, err := ck.Cube.Scan(startKey, nil, limit)
	if err != nil {
		return nil,nil, err
	}
	var keys []TupleKey
	var realKey TupleKey
	var values []TupleValue
	//ret[even index] is key
	//ret[odd index] is value
	for i := 1 ; i < len(ret); i += 2 {
		realKey = kv.DecodeDataKey(ret[i-1])
		keys = append(keys,realKey)
		values = append(values,ret[i])
	}
	return keys,values,err
}

func (ck * CubeKV) GetWithPrefix(prefix TupleKey, prefixLen int, limit uint64) ([]TupleKey, []TupleValue, error) {
	panic("implement me")
}

func (ck * CubeKV) GetShardsWithRange(startKey TupleKey, endKey TupleKey) (interface{}, error) {
	panic("implement me")
}

func (ck * CubeKV) GetShardsWithPrefix(prefix TupleKey) (interface{}, error) {
	panic("implement me")
}


