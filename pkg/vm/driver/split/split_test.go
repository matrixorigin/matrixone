package split

import (
	"bytes"
	"fmt"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cconfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	aoe2 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"strconv"

	//"github.com/matrixorigin/matrixone/pkg/vm/driver/test"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	stdLog "log"
	"testing"
	"time"
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

func TestSplit(t *testing.T) {
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	ctlgListener := catalog.NewCatalogListener()
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe2.Storage, error) {
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
			return aoe2.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cconfig.Config) {
				cfg.Worker.RaftEventWorkers = 8
				cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(5000)
				cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(5000)
				cfg.Replication.ShardSplitCheckDuration.Duration = 8 * time.Second
			}),
			raftstore.WithTestClusterLogLevel(zapcore.ErrorLevel),
			raftstore.WithTestClusterDataPath(clusterDataPath)))
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> removeall")
		//os.RemoveAll(clusterDataPath)
	}()
	c.Start()
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		//c.Stop()
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
	for i := 0; i < 10; i++ {
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
		if i == 9 {
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

func checkSplit(s *aoe2.Storage, old uint64) bool {
	dbName := aoedb.IdToNameFactory.Encode(old)
	logutil.Infof("before checkSplit")
	db, err := s.DB.Store.Catalog.SimpleGetDatabaseByName(dbName)
	logutil.Infof("checkSplit, db is %v, err is %v", db, err)
	if err == nil {
		return false
	}
	return true
}
