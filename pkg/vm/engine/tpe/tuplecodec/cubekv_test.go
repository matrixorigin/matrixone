// Copyright 2021 Matrix Origin
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

package tuplecodec

import (
	cconfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	aoe3 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/testutil"
	aoeStorage "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap/zapcore"
	"testing"
	"time"
)

const (
	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
)

var preAllocShardNum = uint64(20)

func NewTestCluster(t *testing.T) *testutil.TestAOECluster {
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = preAllocShardNum
			c.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = nil
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &aoeStorage.Options{}
			mdCfg := &aoeStorage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &aoeStorage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &aoeStorage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cconfig.Config) {
				cfg.Worker.RaftEventWorkers = 8
			}),
			raftstore.WithTestClusterLogLevel(zapcore.ErrorLevel),
			raftstore.WithTestClusterDataPath("./clusterstore")))

	c.Start()
	return c
}

func TestCubeKV_NextID(t *testing.T) {
	tc := NewTestCluster(t)
	defer tc.Stop()

	convey.Convey("next id",t, func() {
		kv, _ := NewCubeKV(tc.CubeDrivers[0])

		typ := DATABASE_ID
		for i := 0; i < 100; i++ {
			id, err := kv.NextID(typ)
			convey.So(err,convey.ShouldBeNil)
			convey.So(id,convey.ShouldEqual,kv.dbIDPool.idStart)
		}
	})

}
