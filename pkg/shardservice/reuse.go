// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
)

type shardSlice struct {
	values []pb.TableShard
	local  []int
}

func newSlice() *shardSlice {
	return reuse.Alloc[shardSlice](nil)
}

func (s shardSlice) TypeName() string {
	return "sharding.slice.shard"
}

func (s *shardSlice) reset() {
	s.values = s.values[:0]
	s.local = s.local[:0]
}

func (s *shardSlice) close() {
	reuse.Free(s, nil)
}

type futureSlice struct {
	values []*morpc.Future
}

func newFutureSlice() *futureSlice {
	return reuse.Alloc[futureSlice](nil)
}

func (s futureSlice) TypeName() string {
	return "sharding.slice.future"
}

func (s *futureSlice) reset() {
	for i := range s.values {
		s.values[i] = nil
	}
	s.values = s.values[:0]
}

func (s *futureSlice) close() {
	reuse.Free(s, nil)
}

func init() {
	reuse.CreatePool[shardSlice](
		func() *shardSlice {
			return &shardSlice{}
		},
		func(s *shardSlice) {
			s.reset()
		},
		reuse.DefaultOptions[shardSlice]().
			WithEnableChecker(),
	)

	reuse.CreatePool[futureSlice](
		func() *futureSlice {
			return &futureSlice{}
		},
		func(s *futureSlice) {
			s.reset()
		},
		reuse.DefaultOptions[futureSlice]().
			WithEnableChecker(),
	)
}
