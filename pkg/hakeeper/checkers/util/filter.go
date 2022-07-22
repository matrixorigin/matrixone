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

package util

// IFilter filter interface
type IFilter interface {
	Filter(store *Store) bool
}

type Filter func(store *Store) bool

func (f Filter) Filter(store *Store) bool {
	return f(store)
}

// FilterStore filters store according to Filters
func FilterStore(stores []*Store, filters []IFilter) (candidates []*Store) {
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

type ExcludedFilter struct {
	excluded map[string]struct{}
}

func NewExcludedFilter(stores ...string) *ExcludedFilter {
	e := ExcludedFilter{map[string]struct{}{}}
	for _, store := range stores {
		e.excluded[store] = struct{}{}
	}
	return &e
}

func (e *ExcludedFilter) Filter(store *Store) bool {
	if _, ok := e.excluded[string(store.ID)]; ok {
		return true
	}
	return false
}
