package dist

import (
	"fmt"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
	stdLog "log"
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
	util.SetLogger(&emptyLog{})
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
			Addr: fmt.Sprintf("127.0.0.1:808%d", i),
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

	assert.NoError(t, err)
	stdLog.Printf("app all started.")

	resp, err := setTest([]byte("hello"), []byte("world"), c)
	assert.NoError(t, err)
	assert.Equal(t, "OK", string(resp))

	value, err := getTest([]byte("hello"), c)
	assert.NoError(t, err)
	assert.Equal(t, value, []byte("world"))

	gValue, err := getWithGroupTest([]byte("hello"), AOEGroup, c)
	assert.NoError(t, err)
	assert.Nil(t, gValue)

	resp, err = setWithGroupTest([]byte("hello"), []byte("world"), AOEGroup, c)
	assert.NoError(t, err)
	assert.Equal(t, "OK", string(resp))

	gValue, err = getWithGroupTest([]byte("hello"), AOEGroup, c)
	assert.NoError(t, err)
	assert.NotNil(t, gValue)

}

func setTest(key []byte, value []byte, c *testCluster) ([]byte, error) {
	return c.applications[0].Exec(Args{
		Op: uint64(Set),
		Args: [][]byte{
			key,
			value,
		},
	})
}

func setWithGroupTest(key []byte, value []byte, group Group, c *testCluster) ([]byte, error) {
	return c.applications[0].ExecWithGroup(Args{
		Op: uint64(Set),
		Args: [][]byte{
			key,
			value,
		},
	}, group)
}

func getWithGroupTest(key []byte, group Group, c *testCluster) ([]byte, error) {
	return c.applications[0].ExecWithGroup(Args{
		Op: uint64(Get),
		Args: [][]byte{
			key,
		},
	}, group)
}
func getTest(key []byte, c *testCluster) ([]byte, error) {
	return c.applications[0].Exec(Args{
		Op: uint64(Get),
		Args: [][]byte{
			key,
		},
	})
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
