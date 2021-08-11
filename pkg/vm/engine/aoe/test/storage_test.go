package test

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixcube/raftstore"
	stdLog "log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/config"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"testing"
	"time"

	"github.com/fagongzi/log"
	"github.com/stretchr/testify/require"
)

const (
	blockRows          = 100
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 2
	blockCnt           = blockCntPerSegment * segmentCnt
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
			c.ClusterConfig.PreAllocatedGroupNum = 5
			c.ServerConfig.ExternalServer = true
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*daoe.Storage, error) {
			opts := &e.Options{}
			mdCfg := &md.Configuration{
				Dir:              path,
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
			return daoe.NewStorageWithOptions(path, opts)
		}), testutil.WithTestAOEClusterUsePebble())
	c.Start()

	c.RaftCluster.WaitShardByCount(t, 1, time.Second*10)

	stdLog.Printf("app all started.")
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()

	//testAOEStorage(t, c)
	testKVStorage(t, c)

}

func TestRestartStorage(t *testing.T) {
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 5
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*daoe.Storage, error) {
			opts := &e.Options{}
			mdCfg := &md.Configuration{
				Dir:              path,
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
			return daoe.NewStorageWithOptions(path, opts)
		}), testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(raftstore.WithTestClusterRecreate(false)))
	c.Start()
	c.RaftCluster.WaitShardByCount(t, 1, time.Second*10)
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	testAOEStorageAfterRestart(t, c)
	//testKVStorageAfterRestart(t, c)
}

func testAOEStorage(t *testing.T, c *testutil.TestAOECluster) {
	d := c.CubeDrivers[0]

	shard, err := d.GetShardPool().Alloc(uint64(pb.AOEGroup), []byte("test-1"))
	require.NoError(t, err)
	//CreateTableTest
	toShard := shard.ShardID
	err = d.CreateTablet(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard, tableInfo)
	require.NoError(t, err)

	names, err := d.TabletNames(toShard)
	require.NoError(t, err)
	require.Equal(t, 1, len(names))

	//AppendTest
	attrs := helper.Attribute(*tableInfo)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}

	ids, err := d.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err)
	require.Equal(t, 0, len(ids.Ids))
	ibat := chunk.MockBatch(typs, blockRows)
	var buf bytes.Buffer
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	for i := 0; i < blockCnt; i++ {
		err = d.Append(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard, buf.Bytes())
		require.NoError(t, err)
		segmentedIndex, err := d.GetSegmentedId(toShard)
		require.NoError(t, err)
		stdLog.Printf("[Debug]call GetSegmentedId after write %d batch, result is %d", i, segmentedIndex)
	}
	ids, err = d.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err)
	stdLog.Printf("[Debug]SegmentIds is %v\n", ids)
	require.Equal(t, segmentCnt, len(ids.Ids))

	time.Sleep(3 * time.Second)
}

func testAOEStorageAfterRestart(t *testing.T, c *testutil.TestAOECluster) {
	d := c.CubeDrivers[0]
	shard, err := d.GetShardPool().Alloc(uint64(pb.AOEGroup), []byte("test-1"))
	require.NoError(t, err)
	ids, err := d.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(shard.ShardID, tableInfo.Id)), shard.ShardID)
	require.NoError(t, err)
	require.Less(t, 0, len(ids.Ids))
}

func testKVStorage(t *testing.T, c *testutil.TestAOECluster) {
	app := c.CubeDrivers[0]

	//Set Test
	err := app.SetIfNotExist([]byte("Hello"), []byte("World"))
	require.NoError(t, err)

	err = app.SetIfNotExist([]byte("Hello"), []byte("World1"))
	require.NotNil(t, err)

	//Get Test
	value, err := app.Get([]byte("Hello"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("World"))

	//Prefix Test
	for i := uint64(0); i < 20; i++ {
		key := fmt.Sprintf("prefix-%d", i)
		_, err = app.Exec(pb.Request{
			Type: pb.Set,
			Set: pb.SetRequest{
				Key:   []byte(key),
				Value: codec.Uint642Bytes(i),
			},
		})
		require.NoError(t, err)
	}

	t0 := time.Now()
	keys, err := app.PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(keys))
	fmt.Printf("time cost for prefix is %d ms\n", time.Since(t0).Milliseconds())

	kvs, err := app.PrefixScan([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 40, len(kvs))

	err = app.Delete([]byte("prefix-0"))
	require.NoError(t, err)
	keys, err = app.PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 19, len(keys))

	//Scan Test
	t0 = time.Now()
	for i := uint64(0); i < 10; i++ {
		for j := uint64(0); j < 5; j++ {
			key := fmt.Sprintf("/prefix/%d/%d", i, j)
			_, err = app.Exec(pb.Request{
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
	kvs, err = app.Scan([]byte("/prefix/"), []byte("/prefix/2/"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(kvs))
	fmt.Printf("time cost for scan is %d ms\n", time.Since(t0).Milliseconds())
	t0 = time.Now()
	for i := uint64(0); i < 10; i++ {
		for j := uint64(0); j < 5; j++ {
			key := fmt.Sprintf("/prefix/%d/%d", i, j)
			value, err = app.Exec(pb.Request{
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
}

func testKVStorageAfterRestart(t *testing.T, c *testutil.TestAOECluster) {
	app := c.CubeDrivers[0]

	kvs, err := app.Scan([]byte("/prefix/"), []byte("/prefix/2/"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(kvs))
}
