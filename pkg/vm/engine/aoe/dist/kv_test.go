package dist

import (
	"fmt"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"os"
	"testing"
	"time"
)

var (
	tmpDir = "./cube-test"
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
		memDataStorage := mem.NewStorage()
		if err != nil {
			return nil, err
		}
		a, err := NewStorageWithOptions(metaStorage, pebbleDataStorage, memDataStorage, func(cfg *config.Config) {
			cfg.DataPath = fmt.Sprintf("%s/node-%d", tmpDir, i)
			cfg.RaftAddr = fmt.Sprintf("127.0.0.1:1000%d", i)
			cfg.ClientAddr = fmt.Sprintf("127.0.0.1:2000%d", i)

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
	defer cleanupTmpDir()
	c, err := newTestClusterStore(t)

	defer c.stop()

	time.Sleep(2 * time.Second)

	assert.NoError(t, err)
	stdLog.Printf("app all started.")

	//Set Test
	resp, err := c.applications[0].Exec(pb.Request{
		Type: pb.Set,
		Group: pb.KVGroup,
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


	// To Shard(Not Existed) Get Test
	gValue, err := c.applications[0].ExecWithGroup(pb.Request{
		Type: pb.Get,
		Shard: 13,
		Get: pb.GetRequest{
			Key : []byte("Hello"),
		},
	}, pb.AOEGroup)
	require.Error(t, err, ErrShardNotExisted)
	require.Nil(t, gValue)



	// Dynamic Create Shard Test
	client := c.applications[0].RaftStore().Prophet().GetClient()
	err = client.AsyncAddResources(raftstore.NewResourceAdapterWithShard(
		bhmetapb.Shard{
			Start:  []byte("2"),
			End:    []byte("3"),
			Unique: "gTable1",
			Group:  uint64(pb.AOEGroup),
		}))
	//
	require.NoError(t, err)
	time.Sleep(5 * time.Second)

	// Get With Group Test
	gValue, err = c.applications[0].ExecWithGroup(pb.Request{
		Type: pb.Get,
		Shard: 13,
		Get: pb.GetRequest{
			Key : []byte("Hello"),
		},
	}, pb.AOEGroup)
	require.NoError(t, err)
	require.Nil(t, gValue)

	// Set With Group Test
	resp, err = c.applications[0].ExecWithGroup(pb.Request{
		Type: pb.Set,
		Shard: 13,
		Set: pb.SetRequest{
			Key:   []byte("Hello"),
			Value: []byte("World"),
		},
	}, pb.AOEGroup)
	require.NoError(t, err)
	require.Equal(t, "OK", string(resp))

	// Get With Group Test
	gValue, err = c.applications[0].ExecWithGroup(pb.Request{
		Type: pb.Get,
		Shard: 13,
		Get: pb.GetRequest{
			Key : []byte("Hello"),
		},
	}, pb.AOEGroup)
	require.NoError(t, err)
	require.Equal(t, gValue, []byte("World"))


	//PrefixKeys Test
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


}