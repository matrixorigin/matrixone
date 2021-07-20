package dist

import (
	"bytes"
	"fmt"
	"github.com/fagongzi/util/format"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"testing"
	"time"
)

var (
	tmpDir = "/tmp/aoe-cluster-test"

	blockRows          uint64 = 100
	blockCntPerSegment uint64 = 4
	insertRows                = blockRows * blockCntPerSegment * 10
	insertCnt          uint64 = 20
	batchInsertRows           = insertRows / insertCnt
)

func recreateTestTempDir() (err error) {
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return err
	}
	err = os.MkdirAll(tmpDir, os.ModeDir)
	return err
}

func cleanupTmpDir() error {
	return os.RemoveAll(tmpDir)
}

type testCluster struct {
	t            *testing.T
	applications []Storage
}

func newTestClusterStore(t *testing.T) (*testCluster, error) {
	if err := recreateTestTempDir(); err != nil {
		return nil, err
	}
	c := &testCluster{t: t}
	for i := 0; i < 3; i++ {
		metaStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		pebbleDataStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		aoeDataStorage, err := daoe.NewStorage(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		//aoeDataStorage := mem.NewStorage()
		a, err := NewStorageWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, func(cfg *config.Config) {


			cfg.DataPath = fmt.Sprintf("%s/node-%d", tmpDir, i)
			cfg.RaftAddr = fmt.Sprintf("127.0.0.1:1000%d", i)
			cfg.ClientAddr = fmt.Sprintf("127.0.0.1:2000%d", i)

			pConfig.DefaultSchedulers = nil
			cfg.Replication.DisableShardSplit = true
			cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
			cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)
			cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 100)

			cfg.Prophet.Name = fmt.Sprintf("node-%d", i)
			cfg.Prophet.StorageNode = true
			cfg.Prophet.RPCAddr = fmt.Sprintf("127.0.0.1:3000%d", i)
			if i != 0 {
				cfg.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
			}
			cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://127.0.0.1:4000%d", i)
			cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://127.0.0.1:5000%d", i)
			cfg.Prophet.Schedule.EnableJointConsensus = true

		}, server.Cfg{
			Addr: fmt.Sprintf("127.0.0.1:908%d", i),
		})
		if err != nil {
			return nil, err
		}
		c.applications = append(c.applications, a)
	}
	return c, nil
}

func (c *testCluster) stop() {
	for _, s := range c.applications {
		s.Close()
	}
}

func TestClusterStartAndStop(t *testing.T) {
	//defer cleanupTmpDir()
	c, err := newTestClusterStore(t)

	defer c.stop()

	time.Sleep(2 * time.Second)

	assert.NoError(t, err)
	stdLog.Printf("app all started.")


	//testKVStorage(t, c)
	testAOEStorage(t, c)
}

func testKVStorage(t *testing.T, c *testCluster) {
	//Set Test
	resp, err := c.applications[0].Exec(pb.Request{
		Type: pb.Set,
		Set: pb.SetRequest{
			Key:   []byte("Hello"),
			Value: []byte("World"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, "OK", string(resp))

	//Get Test
	value, err := c.applications[0].Exec(pb.Request{
		Type: pb.Get,
		Get: pb.GetRequest{
			Key : []byte("Hello"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, value, []byte("World"))

	//Prefix Test
	for i:=uint64(0); i< 20; i++ {
		key := fmt.Sprintf("prefix-%d", i)
		_, err = c.applications[0].Exec(pb.Request{
			Type: pb.Set,
			Set: pb.SetRequest{
				Key: []byte(key),
				Value: format.Uint64ToBytes(i),
			},
		})
		require.NoError(t, err)
	}

	keys, err := c.applications[0].PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(keys))


	kvs, err := c.applications[0].PrefixScan([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 40, len(kvs))

	err = c.applications[0].Delete([]byte("prefix-0"))
	require.NoError(t, err)
	keys, err = c.applications[0].PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 19, len(keys))
}

func testAOEStorage(t *testing.T, c *testCluster)  {
	//CreateTableTest
	colCnt := 4
	tableInfo := md.MockTableInfo(colCnt)
	toShard := uint64(0)
	c.applications[0].RaftStore().GetRouter().Every(uint64(pb.AOEGroup), false, func(shard *bhmetapb.Shard, address string){
		toShard = shard.ID
	})
	require.Less(t, uint64(0), toShard)
	err := c.applications[0].CreateTablet(fmt.Sprintf("%d#%d", tableInfo.Id, toShard),toShard, tableInfo)
	require.NoError(t, err)

	names, err := c.applications[0].TabletNames(toShard)

	require.NoError(t, err)
	require.Equal(t, 1, len(names))

	//AppendTest
	attrs := helper.Attribute(*tableInfo)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}
	ibat := chunk.MockBatch(typs, batchInsertRows)
	var buf bytes.Buffer
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	err = c.applications[0].Append(fmt.Sprintf("%d#%d", tableInfo.Id, toShard), toShard, buf.Bytes())
	require.NoError(t, err)

}

func MockBatch(types []types.Type, rows uint64) *batch.Batch {
	var attrs []string
	for _, t := range types {
		attrs = append(attrs, t.Oid.String())
	}

	bat := batch.New(true, attrs)
	for i, colType := range types {
		vec := vector.MockVector(colType, rows)
		bat.Vecs[i] = vec.CopyToVector()
	}

	return bat
}