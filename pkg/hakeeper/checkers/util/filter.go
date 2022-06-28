// Copyright 2022 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
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
