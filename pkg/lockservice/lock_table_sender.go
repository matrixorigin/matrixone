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

package lockservice

import (
	"context"
	"sync"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

type channelBasedSender struct {
	out      chan pb.LockTable
	filter   func(pb.LockTable) bool
	changedC chan pb.LockTable
	mu       struct {
		sync.Mutex
		tables map[uint64]pb.LockTable
	}
}

func newChannelBasedSender(
	out chan pb.LockTable,
	filter func(pb.LockTable) bool) KeepaliveSender {
	s := &channelBasedSender{
		out:      out,
		filter:   filter,
		changedC: make(chan pb.LockTable, 1024),
	}
	s.mu.tables = make(map[uint64]pb.LockTable)
	return s
}

func (s *channelBasedSender) Keep(
	ctx context.Context,
	locks []pb.LockTable) ([]pb.LockTable, error) {
	var changed []pb.LockTable
	for _, lock := range locks {
		if v, ok := s.add(lock); ok {
			changed = append(changed, v)
		}
		if s.filter != nil && !s.filter(lock) {
			continue
		}
		s.out <- lock
	}
	return changed, nil
}

func (s *channelBasedSender) Close() error {
	return nil
}

func (s *channelBasedSender) Changed() chan pb.LockTable {
	return s.changedC
}

func (s *channelBasedSender) add(value pb.LockTable) (pb.LockTable, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.mu.tables[value.Table]
	if !ok {
		s.mu.tables[value.Table] = value
		return pb.LockTable{}, false
	}
	return l, l.Changed(value)
}
