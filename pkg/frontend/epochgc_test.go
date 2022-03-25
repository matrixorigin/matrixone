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

package frontend

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"

	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	aoeStorage "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"

	stdLog "log"

	"go.uber.org/zap/zapcore"

	cconfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	aoe3 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/testutil"
)

var DC *DebugCounter = NewDebugCounter(32)

var preAllocShardNum = uint64(20)

func TestEpochGC(t *testing.T) {
	log.SetLevelByString("info")
	log.SetHighlighting(false)

	defer func() {
		err := cleanupTmpDir()
		if err != nil {
			t.Errorf("delete cube temp dir failed %v", err)
		}
	}()

	//go DC.DCRoutine()

	nodeCnt := 3

	pcis := make([]*PDCallbackImpl, nodeCnt)
	cf := make([]*CloseFlag, nodeCnt)
	ppu := NewPDCallbackParameterUnit(1, 1, 1, 1, true, 10000)
	for i := 0; i < nodeCnt; i++ {
		pcis[i] = NewPDCallbackImpl(ppu)
		pcis[i].Id = i
		cf[i] = &CloseFlag{}
		go testPCI(i, cf[i], pcis[i])
	}

	// c, err := NewTestClusterStore(t, true, nil, pcis, nodeCnt)
	// if err != nil {
	// 	t.Errorf("new cube failed %v", err)
	// 	return
	// }

	// c.Applications[0].Start()
	// c.Applications[1].Start()
	// c.Applications[2].Start()
	// defer c.Stop()
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = preAllocShardNum
			c.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = pcis[node]
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &aoeStorage.Options{}
			mdCfg := &aoeStorage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &aoeStorage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &aoeStorage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cconfig.Config) {
				cfg.Worker.RaftEventWorkers = 8
			}),
			// raftstore.WithTestClusterNodeCount(1),
			raftstore.WithTestClusterLogLevel(zapcore.InfoLevel),
			raftstore.WithTestClusterDataPath("./test")))

	c.Start()
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()

	time.Sleep(3 * time.Second)

	c.CubeDrivers[0].Close()

	fmt.Println("-------------------close node 0----------------")

	time.Sleep(3 * time.Second)
	for i := 0; i < nodeCnt; i++ {
		cf[i].Close()
	}

	DC.Cf.Close()
}

const (
	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
)

/*
test:
boot server:
% sudo go test -run TestEpochGCWithMultiServer -v
client:
% cd pkg/server
% mysql -h 127.0.0.1 -P 6002 -udump -p
mysql> source pathto/xxx.sql
*/
//func TestEpochGCWithMultiServer(t *testing.T) {
//	log.SetLevelByString("error")
//	log.SetHighlighting(false)
//	cube_prophet_util.SetLogger(log.NewLoggerWithPrefix("prophet"))
//
//	defer func() {
//		err := cleanupTmpDir()
//		if err != nil {
//			t.Errorf("delete cube temp dir failed %v", err)
//		}
//	}()
//
//	//go DC.DCRoutine()
//
//	nodeCnt := 3
//	pci_id := 0
//	var pcis []*PDCallbackImpl
//	ppu := NewPDCallbackParameterUnit(5, 20, 20, 20, true)
//
//	c := testutil2.NewTestAOECluster(t,
//		func(node int) *config2.Config {
//			c := &config2.Config{}
//			c.ClusterConfig.PreAllocatedGroupNum = 20
//			c.ServerConfig.ExternalServer = true
//			return c
//		},
//		testutil2.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe2.Storage, error) {
//			opts := &storage.Options{}
//			mdCfg := &storage.MetaCfg{
//				SegmentMaxBlocks: blockCntPerSegment,
//				BlockMaxRows:     blockRows,
//			}
//			opts.CacheCfg = &storage.CacheCfg{
//				IndexCapacity:  blockRows * blockCntPerSegment * 80,
//				InsertCapacity: blockRows * uint64(colCnt) * 2000,
//				DataCapacity:   blockRows * uint64(colCnt) * 2000,
//			}
//			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
//				Interval: time.Duration(1) * time.Second,
//			}
//			opts.Meta.Conf = mdCfg
//			return aoe2.NewStorageWithOptions(path, opts)
//		}),
//		testutil2.WithTestAOEClusterUsePebble(),
//		testutil2.WithTestAOEClusterRaftClusterOptions(
//			raftstore.WithTestClusterLogLevel("error"),
//			raftstore.WithTestClusterDataPath("./test"),
//			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cConfig.Config) {
//				pci_id++
//				pci := NewPDCallbackImpl(ppu)
//				pci.Id = pci_id
//				pcis = append(pcis, pci)
//				cfg.Customize.CustomStoreHeartbeatDataProcessor = pci
//			})))
//	c.Start()
//	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)
//
//	defer func() {
//		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
//		c.Stop()
//	}()
//
//	catalog := catalog3.NewCatalog(c.CubeDrivers[0])
//	eng := aoe_engine.New(catalog)
//
//	for i := 0; i < nodeCnt; i++ {
//		pcis[i].SetRemoveEpoch(func(epoch uint64) {
//			_, err := catalog.RemoveDeletedTable(epoch)
//			if err != nil {
//				fmt.Printf("catalog remove ddl async failed. error :%v \n", err)
//			}
//		})
//	}
//
//	server_cnt := 2
//	var svs []*MOServer = nil
//	for i := 0; i < Min(server_cnt, Min(len(testPorts), nodeCnt)); i++ {
//		hm := host.New(1 << 40)
//		gm := guest.New(1<<40, hm)
//		proc := process.New(gm)
//		{
//			proc.Id = "0"
//			proc.Lim.Size = 10 << 32
//			proc.Lim.BatchRows = 10 << 32
//			proc.Lim.PartitionRows = 10 << 32
//			proc.Refer = make(map[string]uint64)
//		}
//		log := logger.New(os.Stderr, fmt.Sprintf("rpc%v:", i))
//		log.SetLevel(logger.WARN)
//		srv, err := rpcserver.New(fmt.Sprintf("127.0.0.1:%v", 20000+i+100), 1<<30, logutil.GetGlobalLogger())
//		if err != nil {
//			log.Fatal(err)
//		}
//		hp := handler.New(eng, proc)
//		srv.Register(hp.Process)
//		go srv.Run()
//
//		svr, err := getMOserver(testConfigFile, testPorts[i], pcis[i], eng)
//		if err != nil {
//			t.Error(err)
//			return
//		}
//		svs = append(svs, svr)
//
//		err = svr.Start()
//		if err != nil {
//			t.Errorf("start server: %v", err)
//			return
//		}
//	}
//
//	time.Sleep(1 * time.Minute)
//
//	fmt.Println("-------------------close node 0----------------")
//	//test performance
//	c.CubeDrivers[0].Close()
//
//	time.Sleep(10 * time.Minute)
//
//	//DC.Cf.Close()
//	select {}
//
//}

func testPCI(id int, f *CloseFlag, pci *PDCallbackImpl) {
	f.Open()
	for f.IsOpened() {
		v := rand.Uint64() % 20
		ep, _ := pci.IncQueryCountAtCurrentEpoch(v)
		if ep == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		DC.Set(id, v)
		time.Sleep(500 * time.Millisecond)
		if rand.Uint32()&0x1 == 0x1 {
			pci.AddMeta(ep, NewMeta(ep, META_TYPE_TABLE, v))
		}
		time.Sleep(500 * time.Millisecond)
		pci.DecQueryCountAtEpoch(ep, v)
	}
}

func Test_Multi_Server(t *testing.T) {
	var svs []*MOServer = nil
	ppu := NewPDCallbackParameterUnit(5, 20, 20, 20, false, 10000)
	for _, port := range testPorts {
		sv, err := getMOserver(testConfigFile, port, NewPDCallbackImpl(ppu), nil)
		if err != nil {
			t.Error(err)
			return
		}

		svs = append(svs, sv)

		err = sv.Start()
		require.NoError(t, err)
	}

	time.Sleep(1 * time.Second)

	var dbs []*sql.DB = nil
	for i, port := range testPorts {
		dbs = append(dbs, open_db(t, port))
		require.True(t, dbs[i] != nil)
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < len(testPorts); i++ {
		close_db(t, dbs[i])
	}

	for _, sv := range svs {
		err := sv.Stop()
		require.NoError(t, err)
	}
}

func Test_openfile(t *testing.T) {
	f, err := os.OpenFile(testConfigFile, os.O_RDONLY|os.O_CREATE, 0777)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = ioutil.ReadAll(f)
	if err != nil {
		t.Error(err)
		return
	}

	defer f.Close()

}

func run_pci(id uint64, close *CloseFlag, pci *PDCallbackImpl,
	kv storage.Storage) {
	close.Open()
	var buf [8]byte
	c := uint64(0)
	cell := time.Duration(10)
	for close.IsOpened() {
		fmt.Printf("++++%d++++loop again\n", id)
		err := pci.Start(kv)
		if err != nil {
			fmt.Printf("A %v\n", err)
		}
		time.Sleep(cell * time.Millisecond)

		for k := 0; k < 3; k++ {
			c++
			binary.BigEndian.PutUint64(buf[:], c)
			rsp, err := pci.HandleHeartbeatReq(id, buf[:], kv)
			if err != nil {
				fmt.Printf("B %v\n", err)
			}
			time.Sleep(cell * time.Millisecond)

			err = pci.HandleHeartbeatRsp(rsp)
			if err != nil {
				fmt.Printf("C %v\n", err)
			}

			ep, _ := pci.IncQueryCountAtCurrentEpoch(1)
			time.Sleep(cell * time.Millisecond)
			pci.DecQueryCountAtEpoch(ep, 1)
		}

		for j := 0; j < 3; j++ {
			err = pci.Stop(kv)
			if err != nil {
				fmt.Printf("D %v\n", err)
			}

			time.Sleep(cell * time.Millisecond)
		}

		time.Sleep(cell * time.Millisecond)
	}
	fmt.Printf("%d to exit \n", id)
}

func Test_PCI_stall(t *testing.T) {
	ppu := NewPDCallbackParameterUnit(1, 1, 1, 1, false, 10000)
	pci := NewPDCallbackImpl(ppu)

	kv := storage.NewTestStorage()

	cnt := 5
	var closeHandle []*CloseFlag = make([]*CloseFlag, cnt)
	for i := 0; i < cnt; i++ {
		closeHandle[i] = &CloseFlag{}
		go run_pci(uint64(i), closeHandle[i], pci, kv)
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < cnt; i++ {
		closeHandle[i].Close()
	}
	time.Sleep(2 * time.Second)
}

func Test_DeleteDDLPermanentlyRoutine(t *testing.T) {
	convey.Convey("DeleteDDLPermanentlyRoutine succ", t, func() {
		sci := &PDCallbackImpl{}
		max_ep := 1
		sci.removeEpoch = func(epoch uint64) {}
		sci.enableLog = true
		sci.DeleteDDLPermanentlyRoutine(uint64(max_ep))

		buf := sci.CollectData()
		convey.So(buf, convey.ShouldResemble, make([]byte, 8))

		f := func(uint64) {}
		sci.SetRemoveEpoch(f)

		ret := sci.GetClusterMinimumRemovableEpoch()
		convey.So(ret, convey.ShouldEqual, 0)

		mt := &Meta{1, 2, 3}
		sci.ddlQueue = make(map[uint64][]*Meta)
		sci.AddMeta(1, mt)
		tar := make(map[uint64][]*Meta)
		tar[1] = []*Meta{mt}

		convey.So(sci.ddlQueue, convey.ShouldResemble, tar)

		tar[1] = []*Meta{mt, mt}
		sci.AddMeta(1, mt)
		convey.So(sci.ddlQueue, convey.ShouldResemble, tar)

		convey.So(sci.removeEpochMetasUnsafe(2), convey.ShouldBeNil)

		convey.So(sci.removeEpochMetasUnsafe(1), convey.ShouldResemble, tar[1])

		convey.So(sci.AddEpochInfo(1, 2), convey.ShouldEqual, 0)

		sci.server_epoch = 1
		sci.epoch_info = make(map[uint64]uint64)
		convey.So(sci.AddEpochInfo(1, 2), convey.ShouldEqual, 1)
		convey.So(sci.epoch_info, convey.ShouldResemble, map[uint64]uint64{1: 2})

		convey.So(sci.SetEpochInfo(0, 1), convey.ShouldEqual, 0)
		convey.So(sci.SetEpochInfo(1, 1), convey.ShouldEqual, 1)
	})
}
