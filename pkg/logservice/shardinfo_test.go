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
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetShardInfo(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		done := false
		for i := 0; i < 1000; i++ {
			si, ok, err := GetShardInfo(testServiceAddress, 1)
			if err != nil || !ok {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			done = true
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, uint64(1), si.ReplicaID)
			addr, ok := si.Replicas[si.ReplicaID]
			assert.True(t, ok)
			assert.Equal(t, testServiceAddress, addr)
			break
		}
		if !done {
			t.Fatalf("failed to get shard info")
		}
		_, ok, err := GetShardInfo(testServiceAddress, 2)
		require.Equal(t, dragonboat.ErrShardNotFound, err)
		assert.False(t, ok)
	}
	runServiceTest(t, false, true, fn)
}
