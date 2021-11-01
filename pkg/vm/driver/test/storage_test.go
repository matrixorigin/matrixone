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

package test

import (
	"bytes"
	"errors"
	"fmt"
	"go.uber.org/zap/zapcore"
	stdLog "log"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	aoe3 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	// "sync"
	"testing"
	"time"

	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
	restart            = false
)

var tableInfo *aoe.TableInfo

func init() {
	tableInfo = adaptor.MockTableInfo(colCnt)
	tableInfo.Id = 100
}
func TestChannel(t *testing.T) {
	completeC := make(chan interface{}, 1)
	completeC<-nil
	value := <-completeC
	switch value.(type) {
	case error:
		fmt.Printf("error\n")
		// return nil, v
	default:
		// return value.([]byte), nil
		fmt.Printf("default\n")
	}
}
func TestAOEStorage(t *testing.T) {
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			// c.ServerConfig.ExternalServer = true
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &storage.Options{}
			mdCfg := &storage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &storage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterLogLevel(zapcore.InfoLevel),
			raftstore.WithTestClusterDataPath("./test")))

	c.Start()
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)

	stdLog.Printf("driver all started.")

	driver := c.CubeDrivers[0]

	driver.RaftStore().GetRouter().ForeachShards(uint64(pb.AOEGroup), func(shard meta.Shard) bool {
		stdLog.Printf("shard %d, peer count is %d\n", shard.ID, len(shard.Replicas))
		return true
	})

	t0 := time.Now()
	//Set Test
	err := driver.Set([]byte("Hello-"), []byte("World-"))
	require.NoError(t, err, "Set fail")
	fmt.Printf("time cost for set is %d ms\n", time.Since(t0).Milliseconds())

	err = driver.SetIfNotExist([]byte("Hello_IfNotExist"), []byte("World_IfNotExist1"))
	require.NoError(t, err, "SetIfNotExist fail")

	err = driver.SetIfNotExist([]byte("Hello_IfNotExist"), []byte("World_IfNotExist2"))
	require.Equal(t, err, errors.New("key is already existed"), "SetIfNotExist wrong")

	// wg := sync.WaitGroup{}
	// wg.Add(1)
	// driver.AsyncSet([]byte("Hello_Async"), []byte("World_Async"), func(i interface{}, data []byte, err error) {
	// 	require.NoError(t, err, "AsyncSet Fail")
	// 	wg.Done()
	// }, nil)
	// wg.Wait()
	// wg.Add(1)
	// driver.AsyncSetIfNotExist([]byte("Hello_AsyncSetIfNotExist"), []byte("World_AsyncSetIfNotExist1"), func(i interface{}, data []byte, err error) {
	// 	require.NoError(t, err, "AsyncSetIfNotExist fail")
	// 	wg.Done()
	// }, nil)
	// wg.Wait()
	// wg.Add(1)
	// driver.AsyncSetIfNotExist([]byte("Hello_AsyncSetIfNotExist"), []byte("World_AsyncSetIfNotExist2"), func(i interface{}, data []byte, err error) {
	// 	require.Equal(t, err, errors.New("key is already existed"), "AsyncSetIfNotExist wrong")
	// 	wg.Done()
	// }, nil)
	// wg.Wait()
	//Get Test
	t0 = time.Now()
	value, err := driver.Get([]byte("Hello-"))
	require.NoError(t, err, "Get Fail")
	require.Equal(t, []byte("World-"), value, "Get wrong")
	fmt.Printf("time cost for get is %d ms\n", time.Since(t0).Milliseconds())
	value, err = driver.Get([]byte("Hello_IfNotExist"))
	require.NoError(t, err, "Get2 Fail")
	require.Equal(t, []byte("World_IfNotExist1"), value, "Get2 wrong")
	// value, err = driver.Get([]byte("Hello_Async"))
	// require.NoError(t, err, "Get2 Fail")
	// require.Equal(t, []byte("World_Async"), value, "Get3 wrong")
	// value, err = driver.Get([]byte("Hello_AsyncSetIfNotExist"))
	// require.NoError(t, err, "Get4 Fail")
	// require.Equal(t, []byte("World_AsyncSetIfNotExist1"), value, "Get4 wrong")
	value, err = driver.Get([]byte("NotExist"))
	require.NoError(t, err, "Get NotExist Fail")
	require.Equal(t, "", string(value), "Get NotExist wrong")
	kvs, err := driver.Scan(nil, nil, 0)
	require.NoError(t, err)
	require.Equal(t, 8, len(kvs))
	//Prefix Test
	for i := uint64(0); i < 20; i++ {
		key := fmt.Sprintf("prefix-%d", i)
		_, err = driver.Exec(pb.Request{
			Type: pb.Set,
			Set: pb.SetRequest{
				Key:   []byte(key),
				Value: codec.Uint642Bytes(i),
			},
		})
		require.NoError(t, err)
	}

	t0 = time.Now()
	keys, err := driver.PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(keys))
	fmt.Printf("time cost for prefix is %d ms\n", time.Since(t0).Milliseconds())

	kvs, err = driver.PrefixScan([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 40, len(kvs))

	err = driver.Delete([]byte("prefix-0"))
	require.NoError(t, err)
	keys, err = driver.PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 19, len(keys))

	//Scan Test
	t0 = time.Now()
	for i := uint64(0); i < 10; i++ {
		for j := uint64(0); j < 5; j++ {
			key := fmt.Sprintf("/prefix/%d/%d", i, j)
			_, err = driver.Exec(pb.Request{
				Type: pb.Set,
				Set: pb.SetRequest{
					Key:   []byte(key),
					Value: []byte(key),
				},
			})
		}
		require.NoError(t, err)
	}
	fmt.Printf("time cost for 50 set is %d ms\n", time.Since(t0).Milliseconds())
	t0 = time.Now()
	kvs, err = driver.Scan([]byte("/prefix/"), []byte("/prefix/2/"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(kvs))
	fmt.Printf("time cost for scan is %d ms\n", time.Since(t0).Milliseconds())
	t0 = time.Now()
	for i := uint64(0); i < 10; i++ {
		for j := uint64(0); j < 5; j++ {
			key := fmt.Sprintf("/prefix/%d/%d", i, j)
			value, err = driver.Exec(pb.Request{
				Type: pb.Get,
				Get: pb.GetRequest{
					Key: []byte(key),
				},
			})
			require.NoError(t, err)
			require.Equal(t, key, string(value))
		}
	}
	fmt.Printf("time cost for 50 read is %d ms\n", time.Since(t0).Milliseconds())
	//Delete Test
	// err=driver.DeleteIfExist([]byte("prefix-0"))
	// require.Equal(t,errors.New("request key is not existed"),err,"DeleteIfExist fail")
	//AllocId Test
	shard, err := driver.GetShardPool().Alloc(uint64(pb.AOEGroup), []byte("test-1"))
	require.NoError(t, err)
	_, err = driver.AllocID([]byte("alloc_id"), 0)
	require.NoError(t, err, "AllocID fail")
	// wg.Add(1)
	// driver.AsyncAllocID([]byte("async_alloc_id"), 0, func(i interface{}, data []byte, err error) {
	// 	require.NoError(t, err, "AsyncSet Fail")
	// 	wg.Done()
	// }, nil)
	// wg.Wait()
	//CreateTableTest
	toShard := shard.ShardID
	stdLog.Printf(">>>toShard %d", toShard)
	err = driver.CreateTablet(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard, tableInfo)
	require.NoError(t, err)

	err = driver.CreateTablet(codec.Bytes2String(codec.EncodeKey(toShard, 101)), toShard, &aoe.TableInfo{Id: 101})
	require.NotNil(t, err)
	names, err := driver.TabletNames(toShard)
	require.NoError(t, err)
	require.Equal(t, 1, len(names))
	// require.Equal(t,tableInfo.Id,id,"DropTablet wrong")
	//AppendTest
	attrs := helper.Attribute(*tableInfo)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}

	ids, err := driver.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err)
	require.Equal(t, 0, len(ids.Ids))

	ibat := mock.MockBatch(typs, blockRows)
	var buf bytes.Buffer
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	for i := 0; i < blockCnt; i++ {
		err = driver.Append(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard, buf.Bytes())
		if err != nil {
			stdLog.Printf("%v", err)
		}
		require.NoError(t, err, "Append%d fail", i)
		segmentedIndex, err := driver.GetSegmentedId(toShard)
		require.NoError(t, err)
		stdLog.Printf("[Debug]call GetSegmentedId after write %d batch, result is %d", i, segmentedIndex)
	}
	ids, err = driver.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err)
	stdLog.Printf("[Debug]SegmentIds is %v\n", ids)
	require.Equal(t, segmentCnt, len(ids.Ids))

	//DropTableTest
	_, err = driver.DropTablet(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err, "DropTablet fail")
	time.Sleep(3 * time.Second)

	if restart {
		doRestartStorage(t)
	}

}

func doRestartStorage(t *testing.T) {
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &storage.Options{}
			mdCfg := &storage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &storage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}), testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterRecreate(false),
			raftstore.WithTestClusterLogLevel(zapcore.ErrorLevel),
			raftstore.WithTestClusterDataPath("./test")))
	defer func() {
		logutil.Debug(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	c.Start()
	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)

	driver := c.CubeDrivers[0]
	t0 := time.Now()
	err := driver.Set([]byte("Hello1"), []byte("World"))
	require.NoError(t, err)
	fmt.Printf("time cost for set is %d ms\n", time.Since(t0).Milliseconds())

	t0 = time.Now()
	value, err := driver.Get([]byte("Hello1"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("World"))
	fmt.Printf("time cost for get is %d ms\n", time.Since(t0).Milliseconds())

	t0 = time.Now()
	kvs, err := driver.Scan([]byte("/prefix/"), []byte("/prefix/2/"), 0)
	fmt.Printf("time cost for scan is %d ms\n", time.Since(t0).Milliseconds())
	require.NoError(t, err)
	require.Equal(t, 20, len(kvs))

	pool := driver.GetShardPool()
	stdLog.Printf("GetShardPool returns %v", pool)
	shard, err := pool.Alloc(uint64(pb.AOEGroup), []byte("test-1"))
	require.NoError(t, err)

	err = driver.CreateTablet(codec.Bytes2String(codec.EncodeKey(shard.ShardID, tableInfo.Id)), shard.ShardID, tableInfo)
	assert.NotNil(t, err)
	stdLog.Printf("[Debug]rsp of calling CreateTablet, %v", err)

	ids, err := driver.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(shard.ShardID, tableInfo.Id)), shard.ShardID)
	require.NoError(t, err)
	stdLog.Printf("[Debug]SegmentIds is %v\n", ids)
	assert.Equal(t, segmentCnt, len(ids.Ids))

}
