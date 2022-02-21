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
	"fmt"
	"github.com/google/btree"
	"sync"
)

var _ KVHandler = &MemoryKV{}
var _ btree.Item = &MemoryItem{}

var (
	errorKeyIsNull = errors.New("the key is null")
	errorPrefixIsNull = errors.New("the prefix is null")
	errorKeysCountNotEqualToValuesCount = errors.New("the count of keys is not equal to the count of values")
	errorKeyExists = errors.New("key exists")
)

type MemoryItem struct {
	key   TupleKey
	value TupleValue
}

func NewMemoryItem(key TupleKey,value TupleValue) *MemoryItem {
	return &MemoryItem{
		key:   key,
		value: value,
	}
}

func (m *MemoryItem) Less(than btree.Item) bool {
	if x,ok := than.(*MemoryItem) ; ok {
		return m.key.Less(x.key)
	}
	panic("it is not memoryItem")
	return false
}

// MemoryKV for test
type MemoryKV struct {
	rwLock sync.RWMutex
	container *btree.BTree
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		container: btree.New(2),
	}
}

func (m *MemoryKV) NextID(typ string) (uint64, error) {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	panic("implement me")
}

func (m *MemoryKV) Set(key TupleKey, value TupleValue) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	if key == nil {
		return errorKeyIsNull
	}
	m.container.ReplaceOrInsert(NewMemoryItem(key,value))
	return nil
}

func (m *MemoryKV) SetBatch(keys []TupleKey, values []TupleValue) []error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	var errs []error
	kl := len(keys)
	vl := len(values)
	if kl != vl {
		return append(errs, errorKeysCountNotEqualToValuesCount)
	}

	for i := 0; i < kl; i++ {
		if keys[i] == nil {
			errs = append(errs, errorKeyIsNull)
		}else{
			m.container.ReplaceOrInsert(NewMemoryItem(keys[i],values[i]))
		}
	}
	return errs
}

func (m *MemoryKV) DedupSet(key TupleKey, value TupleValue) error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	if key == nil {
		return errorKeyIsNull
	}
	if m.container.Has(NewMemoryItem(key,nil)) {
		return errorKeyExists
	}
	m.container.ReplaceOrInsert(NewMemoryItem(key,value))
	return nil
}

func (m *MemoryKV) DedupSetBatch(keys []TupleKey, values []TupleValue) []error {
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	var errs []error
	kl := len(keys)
	vl := len(values)
	if kl != vl {
		return append(errs, errorKeysCountNotEqualToValuesCount)
	}

	//check nils and duplication
	for i := 0; i < kl; i++ {
		if keys[i] == nil {
			errs = append(errs, errorKeyIsNull)
			continue
		}

		if m.container.Has(NewMemoryItem(keys[i],nil)) {
			errs = append(errs, errorKeyExists)
			continue
		}
		m.container.ReplaceOrInsert(NewMemoryItem(keys[i],values[i]))
		errs = append(errs,nil)
	}
	return errs
}

func (m *MemoryKV) Get(key TupleKey) (TupleValue, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	if key == nil {
		return nil, errorKeyIsNull
	}
	item := m.container.Get(NewMemoryItem(key,nil))
	if x,ok := item.(*MemoryItem) ; ok {
		return x.value,nil
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
		item := m.container.Get(NewMemoryItem(keys[i],nil))
		if x,ok := item.(*MemoryItem); ok {
			values = append(values,x.value)
		}else{
			values = append(values,nil)
		}
	}
	return values, nil
}

func (m *MemoryKV) GetRange(startKey TupleKey, endKey TupleKey) ([]TupleValue, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	var values []TupleValue
	iter := func(i btree.Item) bool {
		if x,ok := i.(*MemoryItem); ok {
			values = append(values,x.value)
		}else{
			values = append(values,nil)
		}
		return true
	}

	m.container.AscendRange(
		NewMemoryItem(startKey,nil),
		NewMemoryItem(endKey,nil),
		iter)
	return values, nil
}

func (m *MemoryKV) GetRangeWithLimit(startKey TupleKey, limit uint64) ([]TupleKey, []TupleValue, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	var keys []TupleKey
	var values []TupleValue
	cnt := uint64(0)
	iter := func(i btree.Item) bool {
		if cnt >= limit {
			return false
		}
		cnt++
		if x,ok := i.(*MemoryItem); ok {
			keys = append(keys,x.key)
			values = append(values,x.value)
		}else{
			keys = append(keys,nil)
			values = append(values,nil)
		}
		return true
	}

	m.container.AscendGreaterOrEqual(
		NewMemoryItem(startKey,nil),
		iter)
	return keys,values, nil
}


func (m *MemoryKV) GetWithPrefix(prefix TupleKey, prefixLen int, limit uint64) ([]TupleKey, []TupleValue, error) {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	if prefix == nil {
		return nil, nil, errorPrefixIsNull
	}

	var keys []TupleKey
	var values []TupleValue
	cnt := uint64(0)
	iter := func(i btree.Item) bool {
		if cnt >= limit {
			return false
		}
		cnt++
		if x,ok := i.(*MemoryItem); ok {
			if !bytes.HasPrefix(x.key,prefix[:prefixLen]) {
				return false
			}
			keys = append(keys,x.key)
			values = append(values,x.value)
		}else{
			keys = append(keys,nil)
			values = append(values,nil)
		}
		return true
	}

	m.container.AscendGreaterOrEqual(NewMemoryItem(prefix,nil),iter)
	return keys, values, nil
}

func (m *MemoryKV) GetShardsWithRange(startKey TupleKey, endKey TupleKey) (interface{}, error) {
	panic("implement me")
}

func (m *MemoryKV) GetShardsWithPrefix(prefix TupleKey) (interface{}, error) {
	panic("implement me")
}

func (m *MemoryKV) PrintKeys()  {
	iter := func(i btree.Item) bool {
		if x,ok := i.(*MemoryItem); ok {
			fmt.Println(x.key)
		}
		return true
	}
	fmt.Println("---keys---")
	m.container.Ascend(iter)
}