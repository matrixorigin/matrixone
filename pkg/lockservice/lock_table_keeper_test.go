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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
)

func TestKeeper(t *testing.T) {
	interval := time.Millisecond * 100
	c := make(chan pb.LockTable, 2)
	n := 0
	runKeeperTest(
		t,
		interval,
		c,
		func(lt pb.LockTable) bool {
			n++
			return n <= 2
		},
		func(k *lockTableKeeper) {
			k.Add(pb.LockTable{})
			for {
				select {
				case <-time.After(time.Second * 10):
					assert.Fail(t, "missing keep")
				default:
					if len(c) == 2 {
						return
					}
					time.Sleep(interval)
				}
			}
		})
}

func TestGetLockTables(t *testing.T) {
	runKeeperTest(
		t,
		time.Minute,
		nil,
		func(lt pb.LockTable) bool {
			return false
		},
		func(k *lockTableKeeper) {
			var values []pb.LockTable
			values = k.getLockTables(values)
			assert.Empty(t, values)

			k.Add(pb.LockTable{})
			values = k.getLockTables(values)
			assert.Equal(t, 1, len(values))

			k.Add(pb.LockTable{Table: 1})
			values = k.getLockTables(values)
			assert.Equal(t, 2, len(values))
		})
}

func runKeeperTest(
	t *testing.T,
	interval time.Duration,
	c chan pb.LockTable,
	filter func(pb.LockTable) bool,
	fn func(*lockTableKeeper)) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	k := NewLockTableKeeper(newChannelBasedSender(c, filter), interval)
	defer func() {
		assert.NoError(t, k.Close())
	}()
	fn(k.(*lockTableKeeper))
}
