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
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestChannelBasedSender(t *testing.T) {
	outC := make(chan pb.LockTable, 1)
	defer close(outC)

	s := newChannelBasedSender(outC, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	_, err := s.Keep(context.Background(), []pb.LockTable{{}})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(outC))
}

func TestChannelBasedSenderWithFilter(t *testing.T) {
	outC := make(chan pb.LockTable, 1)
	defer close(outC)

	s := newChannelBasedSender(outC, func(lt pb.LockTable) bool { return false })
	defer func() {
		assert.NoError(t, s.Close())
	}()

	_, err := s.Keep(context.Background(), []pb.LockTable{{}})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(outC))
}

func TestChannelBasedSenderWithVerionChanged(t *testing.T) {
	outC := make(chan pb.LockTable, 100)
	defer close(outC)

	s := newChannelBasedSender(outC, func(lt pb.LockTable) bool { return true })
	defer func() {
		assert.NoError(t, s.Close())
	}()

	changed, err := s.Keep(context.Background(), []pb.LockTable{{Table: 1}, {Table: 2, Version: 2}, {Table: 3}})
	assert.NoError(t, err)
	assert.Empty(t, changed)

	changed, err = s.Keep(context.Background(), []pb.LockTable{{Table: 1}, {Table: 2, Version: 1}, {Table: 3}})
	assert.NoError(t, err)
	assert.Equal(t, []pb.LockTable{{Table: 2, Version: 2}}, changed)
}
