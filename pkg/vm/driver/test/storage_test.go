// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixcube/logdb"
	"go.etcd.io/etcd/raft/v3"
	stdLog "log"
	"math"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	aoe3 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	// "github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"go.uber.org/zap/zapcore"

	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cconfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// "go.etcd.io/etcd/raft/v3"
)

const (
	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
	restart            = false
	clusterDataPath    = "./test"
	snapshotPath       = "./test"
)

var tableInfo *aoe.TableInfo
var idxInfo *aoe.IndexInfo

func init() {
	tableInfo = adaptor.MockTableInfo(colCnt)
	idxInfo = adaptor.MockIndexInfo()
	tableInfo.Id = 100
}
func MockTableInfo(i int) *aoe.TableInfo {
	tblInfo := &aoe.TableInfo{
		Name:    "mocktbl" + strconv.Itoa(i),
		Columns: make([]aoe.ColumnInfo, 0),
		Indices: make([]aoe.IndexInfo, 0),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colInfo := aoe.ColumnInfo{
			Name: name,
		}
		if i == 1 {
			colInfo.Type = types.Type{Oid: types.T(types.T_varchar), Size: 24}
		} else {
			colInfo.Type = types.Type{Oid: types.T_int32, Size: 4, Width: 4}
		}
		tblInfo.Columns = append(tblInfo.Columns, colInfo)
	}
	return tblInfo
}
func MockVector(t types.Type, j, rows int) vector.IVector {
	blockRows := uint64(rows)
	var vec vector.IVector
	switch t.Oid {
	case types.T_int8:
		vec = vector.NewStdVector(t, blockRows)
		var vals []int8
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, int8(j))
		}
		vec.Append(len(vals), vals)
	case types.T_int16:
		vec = vector.NewStdVector(t, blockRows)
		var vals []int16
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, int16(j))
		}
		vec.Append(len(vals), vals)
	case types.T_int32:
		vec = vector.NewStdVector(t, blockRows)
		var vals []int32
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, int32(j))
		}
		vec.Append(len(vals), vals)
	case types.T_int64:
		vec = vector.NewStdVector(t, blockRows)
		var vals []int64
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, int64(j))
		}
		vec.Append(len(vals), vals)
	case types.T_uint8:
		vec = vector.NewStdVector(t, blockRows)
		var vals []uint8
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, uint8(j))
		}
		vec.Append(len(vals), vals)
	case types.T_uint16:
		vec = vector.NewStdVector(t, blockRows)
		var vals []uint16
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, uint16(j))
		}
		vec.Append(len(vals), vals)
	case types.T_uint32:
		vec = vector.NewStdVector(t, blockRows)
		var vals []uint32
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, uint32(j))
		}
		vec.Append(len(vals), vals)
	case types.T_uint64:
		vec = vector.NewStdVector(t, blockRows)
		var vals []uint64
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, uint64(j))
		}
		vec.Append(len(vals), vals)
	case types.T_float32:
		vec = vector.NewStdVector(t, blockRows)
		var vals []float32
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, float32(j))
		}
		vec.Append(len(vals), vals)
	case types.T_float64:
		vec = vector.NewStdVector(t, blockRows)
		var vals []float64
		for i := uint64(0); i < blockRows; i++ {
			vals = append(vals, float64(j))
		}
		vec.Append(len(vals), vals)
	case types.T_varchar, types.T_char:
		vec = vector.NewStrVector(t, blockRows)
		vals := make([][]byte, 0, blockRows)
		prefix := "str"
		for i := uint64(0); i < blockRows; i++ {
			s := fmt.Sprintf("%s%d", prefix, j)
			vals = append(vals, []byte(s))
		}
		vec.Append(len(vals), vals)
	case types.T_datetime:
		vec = vector.NewStdVector(t, blockRows)
		vals := make([]types.Datetime, 0, blockRows)
		for i := uint64(1); i <= blockRows; i++ {
			vals = append(vals, types.FromClock(int32(j*100), 1, 1, 1, 1, 1, 1))
		}
		vec.Append(len(vals), vals)
	case types.T_date:
		vec = vector.NewStdVector(t, blockRows)
		vals := make([]types.Date, 0, blockRows)
		for i := int32(1); i <= int32(blockRows); i++ {
			vals = append(vals, types.FromCalendar(int32(j), 1, 1))
		}
		vec.Append(len(vals), vals)
	default:
		panic("not supported")
	}
	return vec
}
func MockBatch(tableInfo *aoe.TableInfo, i, rows int) *batch.Batch {
	attrs := helper.Attribute(*tableInfo)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}
	var attrsString []string
	for idx := range typs {
		attrsString = append(attrsString, "mock_"+strconv.Itoa(idx))
	}

	bat := batch.New(true, attrsString)
	var err error
	for j, colType := range typs {
		vec := MockVector(colType, i, rows)
		bat.Vecs[j], err = vec.CopyToVector()
		if err != nil {
			panic(err)
		}
		vec.Close()
	}

	return bat
}
func TestSnapshot(t *testing.T) {
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &storage.Options{}
			mdCfg := &storage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &storage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cconfig.Config) {
				cfg.Raft.RaftLog.ForceCompactCount = 1
				cfg.Raft.RaftLog.CompactThreshold = 1
				cfg.Replication.CompactLogCheckDuration.Duration = time.Second * 5
			}),
			raftstore.WithTestClusterLogLevel(zapcore.DebugLevel),
			raftstore.WithTestClusterDataPath(clusterDataPath)))

	c.Start()
	stdLog.Printf("drivers all started.")
	c.RaftCluster.WaitLeadersByCount(21, time.Second*60)
	d0 := c.CubeDrivers[0]

	leaderNode := 0
	stopNode := 2

	shard, err := d0.GetShardPool().Alloc(uint64(pb.AOEGroup), []byte("test-1"))
	require.NoError(t, err)
	// stdLog.Printf("shard id is %v", shard.ShardID)
	// leaderStore := c.RaftCluster.GetShardLeaderStore(shard.ShardID)
	// leaderStoreContainerID := leaderStore.Meta().ID
	// logutil.Infof("leaderStoreContainerID is %v", leaderStoreContainerID)

	// var stopNode int
	// var leaderNode int
	// for i := 0; i < 3; i++ {
	// 	containerID := c.RaftCluster.GetStore(i).Meta().ID
	// 	if containerID != leaderStoreContainerID {
	// 		stopNode = i
	// 	}
	// 	if containerID == leaderStoreContainerID {
	// 		leaderNode = i
	// 	}
	// }
	// logutil.Infof("stop: %v, leader: %v", stopNode, leaderNode)
	// temp := leaderNode
	// leaderNode = stopNode
	// stopNode = temp

	c.StopNode(stopNode)
	stdLog.Printf("node%v stopped.", stopNode)
	d0 = c.CubeDrivers[leaderNode]

	var insertBatches []*batch.Batch
	for i := 0; i < 10; i++ {
		//create table into the shard
		tbl := MockTableInfo(i)
		j:=0
		for {
			err = d0.CreateTablet(tbl.Name, shard.ShardID, tbl)
			if err == nil {
				break
			}
			fmt.Printf("wait %v",j)
			j++
		}
		stdLog.Printf(" create table %v", i)
		require.Nil(t, err)
		//append 1 rows into the table
		batch := MockBatch(tbl, i, 10000)
		insertBatches = append(insertBatches, batch)
		var buf bytes.Buffer
		err = protocol.EncodeBatch(batch, &buf)
		require.Nil(t, err)
		err = d0.Append(tbl.Name, shard.ShardID, buf.Bytes())
		for {
			err = d0.Append(tbl.Name, shard.ShardID, buf.Bytes())
			if err == nil {
				break
			}
		}
		stdLog.Printf(" append %v", i)
		require.Nil(t, err)
	}

	var replicaID uint64
	replicas := c.RaftCluster.GetShardByID(leaderNode, shard.ShardID).Replicas
	for _, replica := range replicas {
		if replica.ContainerID == c.RaftCluster.GetStore(leaderNode).Meta().ID {
			replicaID = replica.ID
		}
	}

	var logdb logdb.LogDB
	logdb = c.RaftCluster.GetStore(leaderNode).(raftstore.LogDBGetter).GetLogDB()
	hasLog := func(index uint64) bool {
		_, _, err := logdb.IterateEntries(nil, 0, shard.ShardID, replicaID, index, index+1, math.MaxUint64)
		if err == nil {
			return true
		}
		if err == raft.ErrUnavailable {
			logutil.Infof("err is %v", err)
			return false
		}
		panic(err)
	}
	for i := 0; i < 50; i++ {
		if hasLog(3) {
			time.Sleep(1 * time.Second)
		} else {
			logutil.Infof("compaction finished")
			break
		}
		if i == 49 {
			t.Fatalf("failed to remove log entries from logdb")
		}
	}

	c.RestartNode(stopNode)
	stdLog.Printf(" node%v started.", stopNode)
	time.Sleep(20 * time.Second)

	d2 := c.CubeDrivers[stopNode]
	s0 := c.AOEStorages[leaderNode]
	s2 := c.AOEStorages[stopNode]

	// s0.Sync([]uint64{shard.ShardID})
	// s2.Sync([]uint64{shard.ShardID})
	// s0checkpointID := s0.DB.GetDBCheckpointId(aoedb.IdToNameFactory.Encode(shard.ShardID))
	// s2checkpointID := s2.DB.GetDBCheckpointId(aoedb.IdToNameFactory.Encode(shard.ShardID))
	// require.Equal(t, s0checkpointID, s2checkpointID)

	//check tables
	tbls, err := d2.TabletNames(shard.ShardID)
	require.Nil(t, err)
	require.Equal(t, 10, len(tbls))
	require.True(t, s0.IsTablesSame(s2, shard.ShardID))

	//checkbatches
	for _, tbl := range tbls {
		leaderBatches, _ := s0.ReadAll(shard.ShardID, tbl)
		batchs, err := s2.ReadAll(shard.ShardID, tbl)
		require.Nil(t, err)
		for i, batch := range batchs {
			require.Equal(t, len(leaderBatches[i].Vecs), len(batch.Vecs))
			for j, vec := range batch.Vecs {
				require.Equal(t, vec.Col, leaderBatches[i].Vecs[j].Col, "type is %v and %v", vec.Typ, leaderBatches[i].Vecs[j].Typ)
			}
		}
	}

	stdLog.Printf("call stop")
	c.Stop()
}
func TestSplit(t *testing.T) {
	os.Remove(clusterDataPath)
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	ctlgListener := catalog.NewCatalogListener()
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &storage.Options{}
			mdCfg := &storage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &storage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			opts.EventListener = ctlgListener
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cconfig.Config) {
				cfg.Worker.RaftEventWorkers = 8
				cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(10000)
				cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(10000)
				cfg.Replication.ShardSplitCheckDuration.Duration = 8 * time.Second
			}),
			raftstore.WithTestClusterLogLevel(zapcore.DebugLevel),
			raftstore.WithTestClusterDataPath(clusterDataPath)))

	c.Start()
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	stdLog.Printf("drivers all started.")
	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)

	d0 := c.CubeDrivers[0]
	catalog := catalog.NewCatalog(d0)
	ctlgListener.UpdateCatalog(catalog)

	dbid, err := catalog.CreateDatabase(0, "split_test", 0)
	require.NoError(t, err)
	tbl := MockTableInfo(0)
	tid, err := catalog.CreateTable(0, dbid, *tbl)
	require.NoError(t, err)

	sids, err := catalog.GetShardIDsByTid(tid)
	logutil.Infof("sids is %v", sids)

	totalRowsBeforeSplit := uint64(0)
	logutil.Infof("split: start insert")
	for i := 0; i < 20; i++ {
		batch := MockBatch(tbl, i, 10000)
		var buf bytes.Buffer
		err = protocol.EncodeBatch(batch, &buf)
		require.Nil(t, err)
		err = d0.Append(catalog.EncodeTabletName(sids[0], tid), sids[0], buf.Bytes())
		if err == nil {
			totalRowsBeforeSplit += 10000
		}
		time.Sleep(2 * time.Second)
		if checkSplit(c.AOEStorages[0], sids[0]) {
			break
		}
		if i == 39 {
			t.Fatalf("failed to split")
		}
	}
	//check ctlg
	time.Sleep(3 * time.Second)
	newsids, err := catalog.GetShardIDsByTid(tid)
	logutil.Infof("newsids are %v", newsids)
	for _, sid := range newsids {
		require.NotEqual(t, sids[0], sid)
	}
	logutil.Infof("split: check ctlg finished")
	//check data
	batchesAfterSplit := make([]*batch.Batch, 0)
	totalRowsAfterSplit := uint64(0)
	for _, sid := range newsids {
		batch, err := c.AOEStorages[0].ReadAll(sid, catalog.EncodeTabletName(sid, tid))
		require.NoError(t, err)
		batchesAfterSplit = append(batchesAfterSplit, batch...)
		rows, _ := c.AOEStorages[0].TotalRows(sid)
		totalRowsAfterSplit += rows
	}
	logutil.Infof("split: check data finished")

	logutil.Infof("total rows before %v after %v", totalRowsBeforeSplit, totalRowsAfterSplit)
	require.Equal(t, totalRowsBeforeSplit, totalRowsAfterSplit)
}

func checkSplit(s *aoe3.Storage, old uint64) bool {
	dbName := aoedb.IdToNameFactory.Encode(old)
	logutil.Infof("before checkSplit")
	db, err := s.DB.Store.Catalog.SimpleGetDatabaseByName(dbName)
	logutil.Infof("checkSplit, db is %v, err is %v", db, err)
	if err == nil {
		return false
	}
	return true
}
func TestAOEStorage(t *testing.T) {
	os.Remove(clusterDataPath)
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			// c.ServerConfig.ExternalServer = true
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &storage.Options{}
			mdCfg := &storage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &storage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			// raftstore.WithTestClusterNodeCount(1),
			raftstore.WithTestClusterLogLevel(zapcore.DebugLevel),
			raftstore.WithTestClusterDataPath(clusterDataPath)))

	c.Start()
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)

	stdLog.Printf("driver all started.")

	driver := c.CubeDrivers[0]

	driver.RaftStore().GetRouter().ForeachShards(uint64(pb.AOEGroup), func(shard meta.Shard) bool {
		stdLog.Printf("shard %d, peer count is %d\n", shard.ID, len(shard.Replicas))
		return true
	})

	t0 := time.Now()
	var err error
	// shardMetas, err := driver.Scan(nil, nil, 0)
	require.NoError(t, err)
	// shardMetaLen := len(shardMetas)
	//Set Test
	err = driver.Set([]byte("Hello-"), []byte("World-"))
	require.NoError(t, err, "Set fail")
	fmt.Printf("time cost for set is %d ms\n", time.Since(t0).Milliseconds())

	err = driver.SetIfNotExist([]byte("Hello_IfNotExist"), []byte("World_IfNotExist1"))
	require.NoError(t, err, "SetIfNotExist fail")

	err = driver.SetIfNotExist([]byte("Hello_IfNotExist"), []byte("World_IfNotExist2"))
	require.Equal(t, err, errors.New("key is already existed"), "SetIfNotExist wrong")

	wg := sync.WaitGroup{}
	wg.Add(1)
	driver.AsyncSet([]byte("Hello_Async"), []byte("World_Async"), func(i server.CustomRequest, data []byte, err error) {
		require.NoError(t, err, "AsyncSet Fail")
		wg.Done()
	}, nil)
	wg.Wait()
	wg.Add(1)
	driver.AsyncSetIfNotExist([]byte("Hello_AsyncSetIfNotExist"), []byte("World_AsyncSetIfNotExist1"), func(i server.CustomRequest, data []byte, err error) {
		require.NoError(t, err, "AsyncSetIfNotExist fail")
		wg.Done()
	}, nil)
	wg.Wait()
	wg.Add(1)
	driver.AsyncSetIfNotExist([]byte("Hello_AsyncSetIfNotExist"), []byte("World_AsyncSetIfNotExist2"), func(i server.CustomRequest, data []byte, err error) {
		require.Equal(t, err, errors.New("key is already existed"), "AsyncSetIfNotExist wrong")
		wg.Done()
	}, nil)
	wg.Wait()
	//Get Test
	t0 = time.Now()
	value, err := driver.Get([]byte("Hello-"))
	require.NoError(t, err, "Get Fail")
	require.Equal(t, []byte("World-"), value, "Get wrong")
	fmt.Printf("time cost for get is %d ms\n", time.Since(t0).Milliseconds())
	value, err = driver.Get([]byte("Hello_IfNotExist"))
	require.NoError(t, err, "Get2 Fail")
	require.Equal(t, []byte("World_IfNotExist1"), value, "Get2 wrong")
	value, err = driver.Get([]byte("Hello_Async"))
	require.NoError(t, err, "Get2 Fail")
	require.Equal(t, []byte("World_Async"), value, "Get3 wrong")
	value, err = driver.Get([]byte("Hello_AsyncSetIfNotExist"))
	require.NoError(t, err, "Get4 Fail")
	require.Equal(t, []byte("World_AsyncSetIfNotExist1"), value, "Get4 wrong")
	value, err = driver.Get([]byte("NotExist"))
	require.NoError(t, err, "Get NotExist Fail")
	require.Equal(t, "", string(value), "Get NotExist wrong")
	kvs, err := driver.Scan(nil, nil, 0)
	require.NoError(t, err)
	// require.Equal(t, 8+shardMetaLen, len(kvs))
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

	t0 = time.Now()
	keys, err := driver.PrefixKeys([]byte("prefix-"), 0)
	require.NoError(t, err)
	require.Equal(t, 20, len(keys))
	fmt.Printf("time cost for prefix is %d ms\n", time.Since(t0).Milliseconds())

	kvs, err = driver.PrefixScan([]byte("prefix-"), 0)
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
	shard, err := driver.GetShardPool().Alloc(uint64(pb.AOEGroup), []byte("test-1"))
	require.NoError(t, err)
	_, err = driver.AllocID([]byte("alloc_id"), 0)
	require.NoError(t, err, "AllocID fail")
	//CreateTableTest
	toShard := shard.ShardID
	stdLog.Printf(">>>toShard %d", toShard)
	err = driver.CreateTablet(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard, tableInfo)
	require.NoError(t, err)

	err = driver.CreateTablet(codec.Bytes2String(codec.EncodeKey(toShard, 101)), toShard, &aoe.TableInfo{Id: 101})
	require.NotNil(t, err)
	_, err = driver.TabletNames(toShard)
	require.NoError(t, err)
	//AppendTest
	attrs := helper.Attribute(*tableInfo)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}

	ids, err := driver.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err)
	require.Equal(t, 0, len(ids.Ids))
	ibat := mock.MockBatch(typs, blockRows)
	var buf bytes.Buffer
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	for i := 0; i < blockCnt; i++ {
		err = driver.Append(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard, buf.Bytes())
		if err != nil {
			stdLog.Printf("%v", err)
		}
		require.NoError(t, err, "Append%d fail", i)
		segmentedIndex, err := driver.GetSegmentedId(toShard)
		require.NoError(t, err)
		stdLog.Printf("[Debug]call GetSegmentedId after write %d batch, result is %d", i, segmentedIndex)
	}
	ids, err = driver.GetSegmentIds(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err)
	stdLog.Printf("[Debug]SegmentIds is %v\n", ids)
	require.Equal(t, segmentCnt, len(ids.Ids))
	//CreateIndexTest
	err = driver.CreateIndex(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), idxInfo, toShard)
	require.NoError(t, err)
	err = driver.CreateIndex(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), idxInfo, toShard)
	require.NotNil(t, err)
	//DropIndexTest
	err = driver.DropIndex(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), idxInfo.Name, toShard)
	require.NoError(t, err)
	err = driver.DropIndex(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), idxInfo.Name, toShard)
	require.NotNil(t, err)
	//DropTableTest
	_, err = driver.DropTablet(codec.Bytes2String(codec.EncodeKey(toShard, tableInfo.Id)), toShard)
	require.NoError(t, err, "DropTablet fail")
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
			opts := &storage.Options{}
			mdCfg := &storage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &storage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}), testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterNodeCount(1),
			raftstore.WithTestClusterRecreate(false),
			raftstore.WithTestClusterLogLevel(zapcore.ErrorLevel),
			raftstore.WithTestClusterDataPath("./test")))
	defer func() {
		logutil.Debug(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	c.Start()
	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)

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
