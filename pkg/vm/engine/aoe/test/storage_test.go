package test

import (
	"bytes"
	"fmt"
	"github.com/fagongzi/log"
	putil "github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"testing"
	"time"
)

const (
	blockRows          = 100
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 2
	batchCnt           = blockCntPerSegment * segmentCnt
)

func TestStorage(t *testing.T) {
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	log.SetHighlighting(false)
	log.SetLevelByString("error")
	putil.SetLogger(log.NewLoggerWithPrefix("prophet"))
	c, err := testutil.NewTestClusterStore(t, true, func(path string) (storage.DataStorage, error) {
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
	})
	defer c.Stop()
	time.Sleep(2 * time.Second)

	assert.NoError(t, err)
	stdLog.Printf("app all started.")

	//testCodec(t, c)
	testKVStorage(t, c)
	//testAOEStorage(t, c)
}

func testCodec(t *testing.T, c *testutil.TestCluster) {

	key := codec.String2Bytes("Hello")
	err := c.Applications[0].Set(key, key)
	require.NoError(t, err)

	value, err := c.Applications[0].Get(key)
	require.NoError(t, err)
	require.Equal(t, value, key)

	key1 := codec.EncodeKey("Hello", 1, 2)
	err = c.Applications[0].Set(key1, key1)
	require.NoError(t, err)

	value1, err := c.Applications[0].Get(key1)
	require.NoError(t, err)
	require.Equal(t, value1, key1)

}

func testKVStorage(t *testing.T, c *testutil.TestCluster) {

	//Set Test
	err := c.Applications[0].SetIfNotExist([]byte("Hello"), []byte("World"))
	require.NoError(t, err)

	err = c.Applications[0].SetIfNotExist([]byte("Hello"), []byte("World1"))
	require.NotNil(t, err)

	//Get Test
	value, err := c.Applications[0].Get([]byte("Hello"))
	require.NoError(t, err)
	require.Equal(t, value, []byte("World"))

	//Prefix Test
	for i := uint64(0); i < 20; i++ {
		key := fmt.Sprintf("prefix-%d", i)
		_, err = c.Applications[0].Exec(pb.Request{
			Type: pb.Set,
			Set: pb.SetRequest{
				Key:   []byte(key),
				Value: codec.Uint642Bytes(i),
			},
		})
		require.NoError(t, err)
	}

	keys, err := c.Applications[0].PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(keys))

	kvs, err := c.Applications[0].PrefixScan([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 40, len(kvs))

	err = c.Applications[0].Delete([]byte("prefix-0"))
	require.NoError(t, err)
	keys, err = c.Applications[0].PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 19, len(keys))

	//Scan Test
	t0 := time.Now()
	for i := uint64(0); i < 10; i++ {
		for j := uint64(0); j < 5; j++ {
			key := fmt.Sprintf("/prefix/%d/%d", i, j)
			_, err = c.Applications[0].Exec(pb.Request{
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
	kvs, err = c.Applications[0].Scan([]byte("/prefix/"), []byte("/prefix/2/"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(kvs))
	fmt.Printf("time cost for scan is %d ms\n", time.Since(t0).Milliseconds())
	t0 = time.Now()
	for i := uint64(0); i < 10; i++ {
		for j := uint64(0); j < 5; j++ {
			key := fmt.Sprintf("/prefix/%d/%d", i, j)
			value, err = c.Applications[0].Exec(pb.Request{
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

func testAOEStorage(t *testing.T, c *testutil.TestCluster) {
	var sharids []uint64
	c.Applications[0].RaftStore().GetRouter().Every(uint64(pb.AOEGroup), true, func(shard *bhmetapb.Shard, store bhmetapb.Store) {
		stdLog.Printf("shard id is %d, leader address is %s, MCpu is %d", shard.ID, store.ClientAddr, len(c.Applications[0].RaftStore().GetRouter().GetStoreStats(store.ID).GetCpuUsages()))
		sharids = append(sharids, shard.ID)
	})
	require.Less(t, 0, len(sharids))
	//CreateTableTest
	colCnt := 4
	tableInfo := md.MockTableInfo(colCnt)
	toShard := sharids[0]
	err := c.Applications[0].CreateTablet(fmt.Sprintf("%d#%d", tableInfo.Id, toShard), toShard, tableInfo)
	require.NoError(t, err)

	names, err := c.Applications[0].TabletNames(toShard)
	require.NoError(t, err)
	require.Equal(t, 1, len(names))

	//AppendTest
	attrs := helper.Attribute(*tableInfo)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}
	ibat := chunk.MockBatch(typs, blockRows)
	var buf bytes.Buffer
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	ids, err := c.Applications[0].GetSegmentIds(fmt.Sprintf("%d#%d", tableInfo.Id, toShard), toShard)
	require.NoError(t, err)
	fmt.Printf("SegmentIds is %v", ids)
	err = c.Applications[0].Append(fmt.Sprintf("%d#%d", tableInfo.Id, toShard), toShard, buf.Bytes())
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
}
