// Copyright 2021 - 2022 Matrix Origin
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

package dnservice

// Filter filter unexpected DN store
type Filter interface {
	Filter(store *dnStore) bool
}

// filterOutFull filter out full DN store
type filterOutFull struct {
}

func (f *filterOutFull) Filter(store *dnStore) bool {
	return store.length >= store.capacity
}

// filterDnStore filters dn store according to Filters
func filterDnStore(stores []*dnStore, filters []Filter) []*dnStore {
	var candidates []*dnStore
	for _, store := range stores {
		for _, filter := range filters {
			if filter.Filter(store) {
				continue
			}
			candidates = append(candidates, store)
		}
	}
	return candidates
}

// availableStores selects available dn stores.
func spareStores(working []*dnStore) []*dnStore {
	return filterDnStore(working, []Filter{
		&filterOutFull{},
	})
}
