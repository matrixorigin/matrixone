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

// IDAllocator is used to fetch new replica ID.
type IDAllocator interface {
	// When IDAllocator was exhaused temporarily, return `false`.
	Next() (uint64, bool)
}

type TestIDAllocator struct {
	id uint64
}

func NewTestIDAllocator(startFrom uint64) *TestIDAllocator {
	return &TestIDAllocator{id: startFrom}
}

func (a *TestIDAllocator) Next() (uint64, bool) {
	a.id += 1
	return a.id, true
}

type StoreID string

const (
	NullStoreID = StoreID("")
)

// Store records metadata for dn store.
type Store struct {
	ID       StoreID
	Length   int
	Capacity int
}

func NewStore(storeID string, length int, capacity int) *Store {
	return &Store{
		ID:       StoreID(storeID),
		Length:   length,
		Capacity: capacity,
	}
}

// ClusterStores collects stores by their status.
type ClusterStores struct {
	Working []*Store
	Expired []*Store
}

func NewClusterStores() *ClusterStores {
	return &ClusterStores{}
}

// RegisterWorking collects working stores.
func (cs *ClusterStores) RegisterWorking(store *Store) {
	cs.Working = append(cs.Working, store)
}

// RegisterExpired collects expired stores.
func (cs *ClusterStores) RegisterExpired(store *Store) {
	cs.Expired = append(cs.Expired, store)
}

// WorkingStores returns all recorded working stores.
// NB: the returned order isn't deterministic.
func (cs *ClusterStores) WorkingStores() []*Store {
	return cs.Working
}

// ExpiredStores returns all recorded expired stores.
// NB: the returned order isn't deterministic.
func (cs *ClusterStores) ExpiredStores() []*Store {
	return cs.Expired
}
