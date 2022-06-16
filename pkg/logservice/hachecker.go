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

package logservice

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
)

const (
	minIDAllocCapacity uint64 = 1024
	defaultIDBatchSize uint64 = 1024 * 10
)

type idAllocator struct {
	// [nextID, lastID] is the range of IDs that can be assigned.
	// the next ID to be assigned is nextID
	nextID uint64
	lastID uint64
}

func newIDAllocator() hakeeper.IDAllocator {
	return &idAllocator{nextID: 1, lastID: 0}
}

func (a *idAllocator) Next() (uint64, bool) {
	return a.next()
}

func (a *idAllocator) set(next uint64, last uint64) {
	a.nextID = next
	a.lastID = last
}

func (a *idAllocator) next() (uint64, bool) {
	if a.nextID <= a.lastID {
		v := a.nextID
		a.nextID++
		return v, true
	}
	return 0, false
}

func (a *idAllocator) capacity() uint64 {
	if a.nextID <= a.lastID {
		return (a.lastID - a.nextID) + 1
	}
	return 0
}

func (l *logStore) updateIDAlloc(count uint64) error {
	cmd := hakeeper.GetGetIDCmd(count)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		plog.Errorf("propose get id failed, %v", err)
		return err
	}
	l.alloc.set(result.Value, result.Value+count-1)
	return nil
}

func (l *logStore) healthCheck() {
	leaderID, term, ok, err := l.nh.GetLeaderID(hakeeper.DefaultHAKeeperShardID)
	if err != nil {
		plog.Errorf("failed to get HAKeeper Leader ID, %v", err)
		return
	}
	if ok && leaderID == l.haKeeperReplicaID {
		if l.alloc.capacity() < minIDAllocCapacity {
			if err := l.updateIDAlloc(defaultIDBatchSize); err != nil {
				// TODO: check whether this is temp error
				plog.Errorf("failed to update ID alloc, %v", err)
				return
			}
		}
		s, err := l.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.StateQuery{})
		if err != nil {
			// TODO: check whether this is temp error
			return
		}
		state := s.(*hakeeper.HAKeeperState)
		opts := l.checker.Check(l.alloc,
			state.ClusterInfo, state.DNState, state.LogState, state.Tick)
	}
}
