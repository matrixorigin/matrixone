package catalogkv

import (
	"fmt"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
	stdLog "log"
	"os"
	"testing"
	"time"
)

var (
	tmpDir = "./cube-test/"
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
	applications []*server.Application
}

func newTestClusterStore(t *testing.T, initShardsFunc func() []bhmetapb.Shard) (*testCluster, error) {
	if err := recreateTestTempDir(); err != nil {
		return nil, err
	}
	util.SetLogger(&emptyLog{})
	c := &testCluster{t: t}
	for i := 0; i < 3; i++ {
		dataStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		cfg := &config.Config{}
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

		cfg.Storage.MetaStorage, err = pebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
			return dataStorage
		}
		cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
			cb(dataStorage)
		}
		cfg.Customize.CustomInitShardsFactory = initShardsFunc
		s := raftstore.NewStore(cfg)
		h := NewHandler(s)

		c.applications = append(c.applications, server.NewApplication(server.Cfg{
			Addr:    fmt.Sprintf("127.0.0.1:808%d", i),
			Store:   s,
			Handler: h,
		}))
	}
	return c, nil
}

func (c *testCluster) start() error {
	for idx, app := range c.applications {
		stdLog.Printf("start app no.%v\n", idx)
		if idx == 2 {
			time.Sleep(time.Second * 5)
		}

		if err := app.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (c *testCluster) stop() {
	for _, s := range c.applications {
		s.Stop()
	}
}

func TestClusterStartAndStop(t *testing.T) {
	defer cleanupTmpDir()
	c, err := newTestClusterStore(t, nil)
	assert.NoError(t, err)
	defer c.stop()
	assert.NoError(t, c.start())
	stdLog.Printf("app all started.")

	resp, err := setTest(c)
	assert.NoError(t, err)
	assert.Equal(t, "OK", string(resp))

	value, err := getTest(c)
	assert.NoError(t, err)
	assert.Equal(t, value, []byte("world"))

	id, err := incrTest(c, "table-id", 10)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), format.MustBytesToUint64(id))

	cid, err := incrTest(c, "table-1-col-id", 11)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), format.MustBytesToUint64(cid))
}

func setTest(c *testCluster) ([]byte, error) {
	return c.applications[0].Exec(KVArgs{
		Op: uint64(1),
		Args: [][]byte{
			[]byte("hello"),
			[]byte("world"),
		},
	}, 1*time.Second)
}

func getTest(c *testCluster) ([]byte, error) {
	return c.applications[0].Exec(KVArgs{
		Op: uint64(10000),
		Args: [][]byte{
			[]byte("hello"),
		},
	}, 1*time.Second)
}

func incrTest(c *testCluster, incrKey string, times uint64) ([]byte, error) {
	cmdAlloc := KVArgs{
		Op:   uint64(2),
		Args: [][]byte{[]byte(incrKey)},
	}
	var value []byte
	var err error
	for i := uint64(0); i < times; i++ {
		value, err = c.applications[0].Exec(cmdAlloc, 1*time.Second)
		if err != nil {
			break
		}
	}
	return value, err
}

type emptyLog struct{}

func (l *emptyLog) Info(v ...interface{}) {

}

func (l *emptyLog) Infof(format string, v ...interface{}) {
	stdLog.Printf(format, v...)
}
func (l *emptyLog) Debug(v ...interface{}) {

}

func (l *emptyLog) Debugf(format string, v ...interface{}) {
}

func (l *emptyLog) Warning(v ...interface{}) {
}

func (l *emptyLog) Warningf(format string, v ...interface{}) {
}

func (l *emptyLog) Error(v ...interface{}) {
}

func (l *emptyLog) Errorf(format string, v ...interface{}) {
	stdLog.Printf(format, v...)
}

func (l *emptyLog) Fatal(v ...interface{}) {
	stdLog.Panic(v...)
}

func (l *emptyLog) Fatalf(format string, v ...interface{}) {
	stdLog.Panicf(format, v...)
}
