// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

const (
	contextTimeout = time.Second * 5
)

type common struct {
	sid            string
	stopper        *stopper.Stopper
	log            *log.MOLogger
	haKeeperConfig logservice.HAKeeperClientConfig
	pool           dataPool
	rt             moruntime.Runtime
	rpcConfig      rpc.Config
	shardReplicaID *sync.Map
}

func newCommon() *common {
	return &common{
		shardReplicaID: &sync.Map{},
	}
}

func (c *common) withLog(l *log.MOLogger) *common {
	c.log = l
	return c
}

func (c *common) withPool(p dataPool) *common {
	c.pool = p
	return c
}

func (c *common) withRuntime(rt moruntime.Runtime) *common {
	c.rt = rt
	return c
}

// for test only.
func (c *common) setShardReplicaID(shardID, replicaID uint64) {
	c.shardReplicaID.Store(shardID, replicaID)
}
