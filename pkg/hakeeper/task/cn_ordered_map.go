// Copyright 2022 Matrix Origin
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

package task

import (
	"sort"
)

type OrderedMap struct {
	Map         map[string]uint32
	OrderedKeys []string
}

func NewOrderedMap() *OrderedMap {
	return &OrderedMap{
		Map: make(map[string]uint32),
	}
}

func (o *OrderedMap) Len() int {
	if len(o.Map) != len(o.OrderedKeys) {
		panic("task.OrderedMap panic")
	}
	return len(o.Map)
}

func (o *OrderedMap) sort() {
	sort.Slice(o.OrderedKeys, func(i, j int) bool {
		return o.Map[o.OrderedKeys[i]] < o.Map[o.OrderedKeys[j]]
	})
}

func (o *OrderedMap) Set(key string, val uint32) {
	if _, ok := o.Map[key]; !ok {
		o.OrderedKeys = append(o.OrderedKeys, key)
	}
	o.Map[key] = val
	o.sort()
}

func (o *OrderedMap) Get(key string) uint32 {
	return o.Map[key]
}

func (o *OrderedMap) Inc(key string) {
	if _, ok := o.Map[key]; !ok {
		o.Map[key] = 0
		o.OrderedKeys = append(o.OrderedKeys, key)
	}
	o.Map[key]++
	o.sort()
}

func (o *OrderedMap) Min() string {
	return o.OrderedKeys[0]
}
