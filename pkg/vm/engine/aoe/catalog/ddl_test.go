package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
	stdLog "log"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/dist"
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
	applications []dist.Storage
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
		a, err := dist.NewStorageWithOptions(metaStorage, pebbleDataStorage, memDataStorage, func(cfg *config.Config) {
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

	time.Sleep(2 * time.Second)

	assert.NoError(t, err)
	stdLog.Printf("app all started.")

	// mock create table(skip validation and id generate), dbid=1, tid=2,
	// 1. create meta set it's state to StateNone, encoding meta and save in kv

	cat := DefaultCatalog(c.applications[0])

	table := aoe.TableInfo{
		SchemaId: 1,
		Id:       1,
		Name:     "my_table",
		State:    aoe.StateNone,
	}

	meta, err := json.Marshal(table)
	err = c.applications[0].Set(cat.tableKey(table.SchemaId, table.Id), meta)
	assert.NoError(t, err)

	// 2. Create Shard for table
	// Dynamic Create Shard Test
	client := c.applications[0].RaftStore().Prophet().GetClient()
	key := cat.tableKey(table.SchemaId, table.Id)
	header := format.Uint64ToBytes(uint64(len(key)))
	buf := bytes.Buffer{}
	buf.Write(header)
	buf.Write(key)
	buf.Write(meta)
	err = client.AsyncAddResources(raftstore.NewResourceAdapterWithShard(
		bhmetapb.Shard{
			Start:  []byte("2"),
			End:    []byte("3"),
			Unique: "gTable1",
			Group:  uint64(aoe.AOEGroup),
			Data:   buf.Bytes(),
		}))
	assert.NoError(t, err)

	completedC := make(chan aoe.TableInfo, 1)
	defer close(completedC)
	tm := aoe.TableInfo{}
	go func() {
		for {
			value, err := c.applications[0].Get(key)
			if err != nil {
				assert.NoError(t, err)
				break
			}
			err = json.Unmarshal(value, &tm)
			if err != nil {
				assert.NoError(t, err)
				break
			}
			if tm.State == aoe.StateNone {
				stdLog.Printf("%s is creating, pls wait", tm.Name)
				time.Sleep(100 * time.Millisecond)
			} else if tm.State == aoe.StatePublic {
				completedC <- tm
				break
			}
		}
	}()

	select {
	case <-completedC:
		stdLog.Printf("%s is created", tm.Name)
		return
	case <-time.After(3 * time.Second):
		stdLog.Printf("create %s failed, timeout", table.Name)

	}
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
