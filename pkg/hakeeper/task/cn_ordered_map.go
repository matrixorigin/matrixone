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

type cnMap struct {
	m           map[string]uint32
	orderedKeys []string
}

func newOrderedMap(keys map[string]struct{}) *cnMap {
	orderedMap := &cnMap{
		m:           make(map[string]uint32, len(keys)),
		orderedKeys: make([]string, 0, len(keys)),
	}

	for key := range keys {
		orderedMap.m[key] = 0
		orderedMap.orderedKeys = append(orderedMap.orderedKeys, key)
	}

	return orderedMap
}

func (o *cnMap) sort() {
	sort.Slice(o.orderedKeys, func(i, j int) bool {
		return o.m[o.orderedKeys[i]] < o.m[o.orderedKeys[j]]
	})
}

func (o *cnMap) set(key string, val uint32) {
	if _, ok := o.m[key]; !ok {
		o.orderedKeys = append(o.orderedKeys, key)
	}
	o.m[key] = val
	o.sort()
}

func (o *cnMap) get(key string) uint32 {
	return o.m[key]
}

func (o *cnMap) inc(key string) {
	o.set(key, o.get(key)+1)
}

func (o *cnMap) min() string {
	if len(o.orderedKeys) == 0 {
		return ""
	}
	return o.orderedKeys[0]
}
