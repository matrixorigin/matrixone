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

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestNewReplica(t *testing.T) {
	r := newReplica(newTestShard(1, 2, 3, "dn1"), nil)
	select {
	case <-r.startedC:
		assert.Fail(t, "cannot started")
	default:
	}
}

func TestWaitStarted(t *testing.T) {
	r := newReplica(newTestShard(1, 2, 3, "dn1"), nil)
	c := make(chan struct{})
	func() {
		r.waitStarted()
		c <- struct{}{}
	}()

	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "wait started failed")
	}
}

func newTestShard(shardID, replicaID, logShardID uint64, address string) metadata.DNShard {
	return metadata.DNShard{
		DNShardRecord: metadata.DNShardRecord{
			ShardID:    shardID,
			LogShardID: logShardID,
		},
		ReplicaID: replicaID,
		Address:   address,
	}
}
