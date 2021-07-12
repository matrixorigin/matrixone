package catalog

import (
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/metadata"
	"os"
	"testing"
	"time"
)

var (
	tmpDir    = "./cube-test"
	dbName    = "test_db1"
	tableName = "test_tb"
	cols      = []engine.TableDef{
		&engine.AttributeDef{
			Attr: metadata.Attribute{
				Name: "col1",
				Type: types.Type{},
				Alg:  0,
			},
		},
		&engine.AttributeDef{
			Attr: metadata.Attribute{
				Name: "col2",
				Type: types.Type{},
				Alg:  0,
			},
		},
	}
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
			Addr: fmt.Sprintf("127.0.0.1:809%d", i),
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

	require.NoError(t, err)
	stdLog.Printf("app all started.")
	catalog := DefaultCatalog(c.applications[0])
	//testDBDDL(t, catalog)
	testTableDDL(t, catalog)

}

func testTableDDL(t *testing.T, c Catalog) {
	tbs, err := c.GetTables(99)
	require.Error(t, ErrDBNotExists, err)

	dbid, err := c.CreateDatabase(dbName, engine.AOE)
	require.NoError(t, err)
	require.Less(t, uint64(0), dbid)

	tbs, err = c.GetTables(dbid)
	require.NoError(t, err)
	require.Nil(t, tbs)

	tid, err := c.CreateTable(dbid, 0, tableName, "", cols, nil)
	require.NoError(t, err)
	require.Less(t, uint64(0), tid)

	completedC := make(chan *aoe.TableInfo, 1)
	defer close(completedC)
	go func() {
		for {
			tb, _ := c.GetTable(dbid, tableName)
			if tb != nil {
				completedC <- tb
				break
			}
		}
	}()
	select {
	case <-completedC:
		stdLog.Printf("create %s finished", tableName)
		break
	case <-time.After(3 * time.Second):
		stdLog.Printf("create %s failed, timeout", tableName)
	}
	_, err = c.CreateTable(dbid, 0, tableName, "",  cols, nil)
	require.Equal(t, ErrTableCreateExists, err)

	for i := 1; i < 10; i++ {
		tid2, err := c.CreateTable(dbid, 0, fmt.Sprintf("%s%d", tableName, i), "", cols, nil)
		require.NoError(t, err)
		require.Less(t, tid, tid2)
	}
	time.Sleep(5 * time.Second)

	tbs, err = c.GetTables(dbid)

	for _, tb := range tbs {
		s, _ := json.Marshal(tb)
		stdLog.Println(string(s))
	}
	require.NoError(t, err)
	require.Equal(t, 10, len(tbs))

	dTid, err := c.DropTable(dbid, tableName)
	require.NoError(t, err)
	require.Equal(t, tid, dTid)

	_, err = c.GetTable(dbid, tableName)
	require.Error(t, ErrTableNotExists, err)

}
func testDBDDL(t *testing.T, c Catalog) {
	dbs, err := c.GetDBs()
	require.NoError(t, err)
	require.Nil(t, dbs)

	id, err := c.CreateDatabase(dbName, engine.AOE)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)

	id, err = c.CreateDatabase(dbName, engine.AOE)
	require.Equal(t, ErrDBCreateExists, err)

	dbs, err = c.GetDBs()
	require.NoError(t, err)
	require.Equal(t, 1, len(dbs))

	db, err := c.GetDB(dbName)
	require.NoError(t, err)
	require.Equal(t, dbName, db.Name)

	id, err = c.DelDatabase(dbName)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)

	db, err = c.GetDB(dbName)
	require.Error(t, ErrDBNotExists, err)
}

