package test

import (
	"bytes"
	"fmt"
	"github.com/fagongzi/log"
	putil "github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/protocol"
	catalog2 "matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/config"
	"matrixone/pkg/vm/engine/aoe/dist/pb"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	"matrixone/pkg/vm/engine/aoe/engine"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"testing"
	"time"
)

const (
	testDBName          = "db1"
	testTableNamePrefix = "test-table-"
)

func TestAOEEngine(t *testing.T) {
	putil.SetLogger(log.NewLoggerWithPrefix("prophet"))
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
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
		}), testutil.WithTestAOEClusterUsePebble(), testutil.WithTestAOEClusterRaftClusterOptions(raftstore.WithTestClusterLogLevel("debug")))
	c.Start()
	c.RaftCluster.WaitLeadersByCount(t, 21, time.Second*30)
	stdLog.Printf("[QSQ]app all started.")

	c.CubeDrivers[0].RaftStore().GetRouter().Every(uint64(pb.AOEGroup), true, func(shard *bhmetapb.Shard, store bhmetapb.Store) {
		stdLog.Printf("shard id is %d, leader address is %s, MCpu is %d", shard.ID, store.ClientAddr, len(c.CubeDrivers[0].RaftStore().GetRouter().GetStoreStats(store.ID).GetCpuUsages()))
	})

	catalog := catalog2.DefaultCatalog(c.CubeDrivers[0])
	aoeEngine := engine.Mock(&catalog)

	err := aoeEngine.Create(0, testDBName, 0)
	require.NoError(t, err)

	dbs := aoeEngine.Databases()
	require.Equal(t, 1, len(dbs))

	err = aoeEngine.Delete(1, testDBName)
	require.NoError(t, err)

	dbs = aoeEngine.Databases()
	require.Equal(t, 0, len(dbs))

	_, err = aoeEngine.Database(testDBName)
	require.NotNil(t, err)

	err = aoeEngine.Create(2, testDBName, 0)
	require.NoError(t, err)
	db, err := aoeEngine.Database(testDBName)
	require.NoError(t, err)

	tbls := db.Relations()
	require.Equal(t, 0, len(tbls))

	mockTbl := md.MockTableInfo(colCnt)
	mockTbl.Name = fmt.Sprintf("%s%d", testTableNamePrefix, 0)
	_, _, _, _, comment, defs, pdef, _ := helper.UnTransfer(*mockTbl)

	stdLog.Printf("[QSQ]sleep 10s before call create table")
	time.Sleep(10 * time.Second)

	err = db.Create(3, mockTbl.Name, defs, pdef, nil, comment)
	if err != nil {
		stdLog.Printf("[QSQ] %v", err)
	} else {
		stdLog.Printf("create table is succeeded")
	}
	require.NoError(t, err)

	tbls = db.Relations()
	require.Equal(t, 1, len(tbls))

	tb, err := db.Relation(mockTbl.Name)
	require.NoError(t, err)
	require.Equal(t, tb.ID(), mockTbl.Name)

	attrs := helper.Attribute(*mockTbl)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}
	ibat := chunk.MockBatch(typs, blockRows)
	var buf bytes.Buffer
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	stdLog.Printf("size of batch is  %d", buf.Len())

	for i := 0; i < blockCnt; i++ {
		err = tb.Write(4, ibat)
		require.NoError(t, err)
	}

	err = db.Delete(5, mockTbl.Name)
	require.NoError(t, err)

	tbls = db.Relations()
	require.Equal(t, 0, len(tbls))

	//test multiple tables creating
	for i := 0; i < 19; i++ {
		mockTbl := md.MockTableInfo(colCnt)
		mockTbl.Name = fmt.Sprintf("%s%d", testTableNamePrefix, i)
		_, _, _, _, comment, defs, pdef, _ := helper.UnTransfer(*mockTbl)
		err = db.Create(6, mockTbl.Name, defs, pdef, nil, comment)
		//require.NoError(t, err)
		if err != nil {
			stdLog.Printf("[QSQ]create table %d failed, err is %v", i, err)
		}
		//time.Sleep(200 * time.Millisecond)
	}

	tbls = db.Relations()
	require.Equal(t, 19, len(tbls))
	time.Sleep(3 * time.Second)

}
