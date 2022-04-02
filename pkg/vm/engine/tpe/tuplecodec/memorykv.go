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
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/btree"
	"sync"
)

var _ KVHandler = &MemoryKV{}
var _ btree.Item = &MemoryItem{}

var (
	errorKeyIsNull                      = errors.New("the key is null")
	errorPrefixIsNull                   = errors.New("the prefix is null")
	errorKeysCountNotEqualToValuesCount = errors.New("the count of keys is not equal to the count of values")
	errorKeyExists                      = errors.New("key exists")
	errorIsNotMemoryItem                = errors.New("it is not the memory item")
	errorUnsupportedInMemoryKV          = errors.New("unsupported in memory kv")
)

type MemoryItem struct {
	key   TupleKey
	value TupleValue
}

func NewMemoryItem(key TupleKey, value TupleValue) *MemoryItem {
	return &MemoryItem{
		key:   key,
		value: value,
	}
}

func (m *MemoryItem) Less(than btree.Item) bool {
	if x, ok := than.(*MemoryItem); ok {
		return m.key.Less(x.key)
	}
	panic("it is not memoryItem")
}

// MemoryKV for test
type MemoryKV struct {
	rwLock    sync.RWMutex
	container *btree.BTree
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		container: btree.New(2),
	}
}

func (m *MemoryKV) GetKVType() KVType {
	return KV_MEMORY
}

func (m *MemoryKV) NextID(typ string) (uint64, error) {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	value := m.container.Get(NewMemoryItem(TupleKey(typ), nil))
	var buf [8]byte
	var nextID uint64
	if value != nil {
		//add id + 1
		if x, ok := value.(*MemoryItem); ok {
			nextID = binary.BigEndian.Uint64(x.value)
		} else {
			return 0, errorIsNotMemoryItem
		}
	} else {
		//id = 3, return 2
		nextID = UserTableIDOffset
	}
	binary.BigEndian.PutUint64(buf[:], nextID+1)
	x := NewMemoryItem(TupleKey(typ), buf[:])
	m.container.ReplaceOrInsert(x)
	return nextID, nil
}

func (m *MemoryKV) Set(key TupleKey, value TupleValue) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	if key == nil {
		return errorKeyIsNull
	}
	m.container.ReplaceOrInsert(NewMemoryItem(key, value))
	return nil
}

func (m *MemoryKV) SetBatch(keys []TupleKey, values []TupleValue) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	kl := len(keys)
	vl := len(values)
	if kl != vl {
		return errorKeysCountNotEqualToValuesCount
	}

	for i := 0; i < kl; i++ {
		if keys[i] == nil {
			return errorKeyIsNull
		} else {
			m.container.ReplaceOrInsert(NewMemoryItem(keys[i], values[i]))
		}
	}
	return nil
}

func (m *MemoryKV) DedupSet(key TupleKey, value TupleValue) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	if key == nil {
		return errorKeyIsNull
	}
	if m.container.Has(NewMemoryItem(key, nil)) {
		return errorKeyExists
	}
	m.container.ReplaceOrInsert(NewMemoryItem(key, value))
	return nil
}

func (m *MemoryKV) DedupSetBatch(keys []TupleKey, values []TupleValue) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	var err error
	kl := len(keys)
	vl := len(values)
	if kl != vl {
		return errorKeysCountNotEqualToValuesCount
	}

	//check nils and duplication
	for i := 0; i < kl; i++ {
		if keys[i] == nil {
			return errorKeyIsNull
		}

		if m.container.Has(NewMemoryItem(keys[i], nil)) {
			return errorKeyExists
		}
		m.container.ReplaceOrInsert(NewMemoryItem(keys[i], values[i]))
	}
	return err
}

func (m *MemoryKV) Delete(key TupleKey) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	m.container.Delete(NewMemoryItem(key, nil))
	return nil
}

func (m *MemoryKV) DeleteWithPrefix(prefix TupleKey) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	var keys []TupleKey
	iter := func(i btree.Item) bool {
		if x, ok := i.(*MemoryItem); ok {
			if bytes.HasPrefix(x.key, prefix) {
				keys = append(keys, x.key)
			}
		} else {
			keys = append(keys, nil)
		}
		return true
	}

	m.container.Ascend(iter)

	for _, key := range keys {
		m.container.Delete(NewMemoryItem(key, nil))
	}

	return nil
}

func (m *MemoryKV) Get(key TupleKey) (TupleValue, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	if key == nil {
		return nil, errorKeyIsNull
	}
	item := m.container.Get(NewMemoryItem(key, nil))
	if x, ok := item.(*MemoryItem); ok {
		return x.value, nil
	}
	return nil, nil
}

func (m *MemoryKV) GetBatch(keys []TupleKey) ([]TupleValue, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	kl := len(keys)

	//check nils
	for i := 0; i < kl; i++ {
		if keys[i] == nil {
			return nil, errorKeyIsNull
		}
	}

	var values []TupleValue
	for i := 0; i < kl; i++ {
		item := m.container.Get(NewMemoryItem(keys[i], nil))
		if x, ok := item.(*MemoryItem); ok {
			values = append(values, x.value)
		} else {
			values = append(values, nil)
		}
	}
	return values, nil
}

func (m *MemoryKV) GetRange(startKey TupleKey, endKey TupleKey) ([]TupleValue, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	var values []TupleValue
	iter := func(i btree.Item) bool {
		if x, ok := i.(*MemoryItem); ok {
			values = append(values, x.value)
		} else {
			values = append(values, nil)
		}
		return true
	}

	m.container.AscendRange(
		NewMemoryItem(startKey, nil),
		NewMemoryItem(endKey, nil),
		iter)
	return values, nil
}

func (m *MemoryKV) GetRangeWithLimit(startKey TupleKey, endKey TupleKey, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	var keys []TupleKey
	var values []TupleValue
	cnt := uint64(0)
	iter := func(i btree.Item) bool {
		if cnt >= limit {
			return false
		}

		if x, ok := i.(*MemoryItem); ok {
			//endKey <= key
			if endKey != nil && bytes.Compare(endKey, x.key) <= 0 {
				return false
			}
			keys = append(keys, x.key)
			values = append(values, x.value)
		} else {
			keys = append(keys, nil)
			values = append(values, nil)
		}
		cnt++
		return true
	}

	m.container.AscendGreaterOrEqual(
		NewMemoryItem(startKey, nil),
		iter)

	complete := false
	nextScanKey := []byte{}
	if len(keys) != 0 {
		more := 0
		checkMoreDataIter := func(i btree.Item) bool {
			if x, ok := i.(*MemoryItem); ok {
				//endKey <= key
				if endKey != nil && bytes.Compare(endKey, x.key) <= 0 {
					return false
				}
				more++
			}
			return true
		}
		checkKey := SuccessorOfKey(keys[len(keys)-1])
		m.container.AscendGreaterOrEqual(
			NewMemoryItem(checkKey, nil),
			checkMoreDataIter)

		if more > 0 {
			complete = false
			nextScanKey = checkKey
		} else {
			complete = true
			nextScanKey = nil
		}
	} else {
		complete = true
		nextScanKey = nil
	}
	return keys, values, complete, nextScanKey, nil
}

func (m *MemoryKV) GetRangeWithPrefixLimit(startKey TupleKey, endKey TupleKey, prefix TupleKey, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error) {
	return nil, nil, false, nil, errorUnsupportedInMemoryKV
}

func (m *MemoryKV) GetWithPrefix(prefixOrStartkey TupleKey, prefixLen int, prefixEnd []byte, needKeyOnly bool, limit uint64) ([]TupleKey, []TupleValue, bool, TupleKey, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	if prefixOrStartkey == nil {
		return nil, nil, false, nil, errorPrefixIsNull
	}

	if prefixLen > len(prefixOrStartkey) {
		return nil, nil, false, nil, errorPrefixLengthIsLongerThanStartKey
	}

	var keys []TupleKey
	var values []TupleValue
	cnt := uint64(0)
	iter := func(i btree.Item) bool {
		if cnt >= limit {
			return false
		}
		cnt++
		if x, ok := i.(*MemoryItem); ok {
			if !bytes.HasPrefix(x.key, prefixOrStartkey[:prefixLen]) {
				return false
			}
			keys = append(keys, x.key)
			values = append(values, x.value)
		} else {
			keys = append(keys, nil)
			values = append(values, nil)
		}
		return true
	}

	m.container.AscendGreaterOrEqual(NewMemoryItem(prefixOrStartkey, nil), iter)

	complete := false
	nextScanKey := []byte{}
	if len(keys) != 0 {
		more := 0
		checkMoreDataIter := func(i btree.Item) bool {
			if x, ok := i.(*MemoryItem); ok {
				if !bytes.HasPrefix(x.key, prefixOrStartkey[:prefixLen]) {
					return false
				}
				more++
			}
			return true
		}
		checkKey := SuccessorOfKey(keys[len(keys)-1])
		m.container.AscendGreaterOrEqual(
			NewMemoryItem(checkKey, nil),
			checkMoreDataIter)

		if more > 0 {
			complete = false
			nextScanKey = checkKey
		} else {
			complete = true
			nextScanKey = nil
		}
	} else {
		complete = true
		nextScanKey = nil
	}

	return keys, values, complete, nextScanKey, nil
}

func (m *MemoryKV) GetShardsWithRange(startKey TupleKey, endKey TupleKey) (interface{}, error) {
	return nil, errorUnsupportedInMemoryKV
}

func (m *MemoryKV) GetShardsWithPrefix(prefix TupleKey) (interface{}, error) {
	return nil, errorUnsupportedInMemoryKV
}

func (m *MemoryKV) PrintKeys() {
	iter := func(i btree.Item) bool {
		if x, ok := i.(*MemoryItem); ok {
			fmt.Println(x.key)
		}
		return true
	}
	fmt.Println("---keys---")
	m.container.Ascend(iter)
}
