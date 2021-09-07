package test

import (
	"bytes"
	"fmt"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/logutil"
	"matrixone/pkg/sql/protocol"
	aoe3 "matrixone/pkg/vm/driver/aoe"
	"matrixone/pkg/vm/driver/config"
	"matrixone/pkg/vm/driver/pb"
	"matrixone/pkg/vm/driver/testutil"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"sync"
	"testing"
	"time"
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
	tableInfo = md.MockTableInfo(colCnt)
	tableInfo.Id = 100
}

func TestStorage(t *testing.T) {
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			c.ServerConfig.ExternalServer = true
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &e.Options{}
			mdCfg := &e.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &e.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &e.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterLogLevel("info"),
			raftstore.WithTestClusterDataPath("./test")))

	c.Start()
	defer func() {
		stdLog.Printf("3>>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	c.RaftCluster.WaitLeadersByCount(t, 21, time.Second*30)

	stdLog.Printf("driver all started.")

	driver := c.CubeDrivers[0]

	driver.RaftStore().GetRouter().ForeachShards(uint64(pb.AOEGroup), func(shard *bhmetapb.Shard) bool {
		stdLog.Printf("shard %d, peer count is %d\n", shard.ID, len(shard.Peers))
		return true
	})

	t0 := time.Now()
	//Set Test
	err := driver.SetIfNotExist([]byte("Hello"), []byte("World"))
	require.NoError(t, err)
	fmt.Printf("time cost for set is %d ms\n", time.Since(t0).Milliseconds())

	err = driver.SetIfNotExist([]byte("Hello"), []byte("World1"))
	require.NotNil(t, err)

	//Get Test
	t0 = time.Now()
	value, err := driver.Get([]byte("Hello"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("World"))
	fmt.Printf("time cost for get is %d ms\n", time.Since(t0).Milliseconds())

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

	keys, err := driver.PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(keys))
	fmt.Printf("time cost for prefix is %d ms\n", time.Since(t0).Milliseconds())

	kvs, err := driver.PrefixScan([]byte("prefix-"), 0)
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

	//AllocId Test
	req := pb.Request{
		Type:  pb.Incr,
		Group: pb.KVGroup,
		AllocID: pb.AllocIDRequest{
			Key: []byte("alloc-key"),
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	driver.AsyncExec(req, func(i interface{}, data []byte, err error) {
		wg.Done()
		if err != nil {
			logutil.Errorf("call AllocId failed, %v\n", err)
			return
		}
		resp, err := codec.Bytes2Uint64(data)
		if err != nil {
			logutil.Errorf("get result of AllocId failed, %v\n", err)
			return
		}
		logutil.Infof("Alloc id is %d\n", resp)
	}, nil)

	shard, err := driver.GetShardPool().Alloc(uint64(pb.AOEGroup), []byte("test-1"))
	require.NoError(t, err)
	//CreateTableTest
	toShard := shard.ShardID
	stdLog.Printf(">>>toShard %d", toShard)
	err = driver.CreateTablet(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard, tableInfo)
	require.NoError(t, err)

	names, err := driver.TabletNames(toShard)
	require.NoError(t, err)
	require.Equal(t, 1, len(names))

	//AppendTest
	attrs := helper.Attribute(*tableInfo)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}

	ids, err := driver.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err)
	require.Equal(t, 0, len(ids.Ids))

	ibat := chunk.MockBatch(typs, blockRows)
	var buf bytes.Buffer
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	for i := 0; i < blockCnt; i++ {
		err = driver.Append(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard, buf.Bytes())
		if err != nil {
			stdLog.Printf("%v", err)
		}
		require.NoError(t, err)
		segmentedIndex, err := driver.GetSegmentedId(toShard)
		require.NoError(t, err)
		stdLog.Printf("[Debug]call GetSegmentedId after write %d batch, result is %d", i, segmentedIndex)
	}
	ids, err = driver.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err)
	stdLog.Printf("[Debug]SegmentIds is %v\n", ids)
	require.Equal(t, segmentCnt, len(ids.Ids))

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
			opts := &e.Options{}
			mdCfg := &e.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &e.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &e.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}), testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterRecreate(false),
			raftstore.WithTestClusterLogLevel("error"),
			raftstore.WithTestClusterDataPath("./test")))
	defer func() {
		logutil.Debug(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	c.Start()
	c.RaftCluster.WaitShardByCounts(t, [3]int{21, 21, 21}, time.Second*30)
	c.RaftCluster.WaitLeadersByCount(t, 21, time.Second*30)

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
