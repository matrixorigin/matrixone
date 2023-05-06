// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"sync"
)

type memStore struct {
	sync.Mutex
	caches map[string]uint64
	steps  map[string]int
}

func newMemStore() IncrValueStore {
	return &memStore{
		caches: make(map[string]uint64),
		steps:  make(map[string]int),
	}
}

func (s *memStore) Create(
	ctx context.Context,
	key string,
	value uint64,
	step int) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.caches[key]; ok {
		return nil
	}
	s.caches[key] = value
	s.steps[key] = step
	return nil
}

func (s *memStore) Alloc(
	ctx context.Context,
	key string,
	count int) (uint64, uint64, error) {
	s.Lock()
	defer s.Unlock()
	curr, ok := s.caches[key]
	if !ok {
		panic("missing incr column record")
	}
	next := getNext(curr, count, s.steps[key])
	s.caches[key] = next
	from, to := getNextRange(curr, next, s.steps[key])
	return from, to, nil
}

func (s *memStore) Delete(
	ctx context.Context,
	keys []string) error {
	s.Lock()
	defer s.Unlock()
	for _, key := range keys {
		delete(s.caches, key)
		delete(s.steps, key)
	}
	return nil
}

func (s *memStore) Close() {

}

func getNext(curr uint64, count, step int) uint64 {
	return curr + uint64(count*step)
}

// [from, to)
func getNextRange(curr uint64, next uint64, step int) (uint64, uint64) {
	return curr + uint64(step), next + uint64(step)
}
