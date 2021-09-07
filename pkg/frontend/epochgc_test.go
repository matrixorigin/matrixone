package frontend

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	stdLog "log"
	"math/rand"
	catalog3 "matrixone/pkg/catalog"
	mo_config "matrixone/pkg/config"
	"matrixone/pkg/logger"
	"matrixone/pkg/logutil"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/sql/handler"
	aoe2 "matrixone/pkg/vm/driver/aoe"
	config2 "matrixone/pkg/vm/driver/config"
	testutil2 "matrixone/pkg/vm/driver/testutil"
	"matrixone/pkg/vm/engine"
	aoe_engine "matrixone/pkg/vm/engine/aoe/engine"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"testing"
	"time"

	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	cube_prophet_util "github.com/matrixorigin/matrixcube/components/prophet/util"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
)

var DC *DebugCounter = NewDebugCounter(32)

func TestEpochGC(t *testing.T) {
	log.SetLevelByString("error")
	log.SetHighlighting(false)
	cube_prophet_util.SetLogger(log.NewLoggerWithPrefix("prophet"))

	defer func() {
		err := cleanupTmpDir()
		if err != nil {
			t.Errorf("delete cube temp dir failed %v", err)
		}
	}()

	go DC.DCRoutine()

	nodeCnt := 3

	pcis := make([]*PDCallbackImpl, nodeCnt)
	cf := make([]*CloseFlag, nodeCnt)
	ppu := NewPDCallbackParameterUnit(5, 20, 20, 20, false)
	for i := 0; i < nodeCnt; i++ {
		pcis[i] = NewPDCallbackImpl(ppu)
		pcis[i].Id = i
		cf[i] = &CloseFlag{}
		go testPCI(i, cf[i], pcis[i])
	}

	c, err := NewTestClusterStore(t, true, nil, pcis, nodeCnt)
	if err != nil {
		t.Errorf("new cube failed %v", err)
		return
	}

	defer c.Stop()

	time.Sleep(1 * time.Minute)

	c.Applications[0].Close()

	fmt.Println("-------------------close node 0----------------")

	time.Sleep(1 * time.Minute)
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
func TestEpochGCWithMultiServer(t *testing.T) {
	log.SetLevelByString("error")
	log.SetHighlighting(false)
	cube_prophet_util.SetLogger(log.NewLoggerWithPrefix("prophet"))

	defer func() {
		err := cleanupTmpDir()
		if err != nil {
			t.Errorf("delete cube temp dir failed %v", err)
		}
	}()

	//go DC.DCRoutine()

	nodeCnt := 3
	pci_id := 0
	var pcis []*PDCallbackImpl
	ppu := NewPDCallbackParameterUnit(5, 20, 20, 20, true)

	c := testutil2.NewTestAOECluster(t,
		func(node int) *config2.Config {
			c := &config2.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			c.ServerConfig.ExternalServer = true
			return c
		},
		testutil2.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe2.Storage, error) {
			opts := &e.Options{}
			mdCfg := &e.MetaCfg{
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
			return aoe2.NewStorageWithOptions(path, opts)
		}),
		testutil2.WithTestAOEClusterUsePebble(),
		testutil2.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterLogLevel("error"),
			raftstore.WithTestClusterDataPath("./test"),
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cConfig.Config) {
				pci_id++
				pci := NewPDCallbackImpl(ppu)
				pci.Id = pci_id
				pcis = append(pcis, pci)
				cfg.Customize.CustomStoreHeartbeatDataProcessor = pci
			})))
	c.Start()
	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)

	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()

	catalog := catalog3.NewCatalog(c.CubeDrivers[0])
	eng := aoe_engine.New(&catalog)

	for i := 0; i < nodeCnt; i++ {
		pcis[i].SetRemoveEpoch(func(epoch uint64) {
			_, err := catalog.RemoveDeletedTable(epoch)
			if err != nil {
				fmt.Printf("catalog remove ddl async failed. error :%v \n", err)
			}
		})
	}

	server_cnt := 2
	var svs []*MOServer = nil
	for i := 0; i < Min(server_cnt, Min(len(testPorts), nodeCnt)); i++ {
		hm := host.New(1 << 40)
		gm := guest.New(1<<40, hm)
		proc := process.New(gm, mempool.New(1<<40, 8))
		{
			proc.Id = "0"
			proc.Lim.Size = 10 << 32
			proc.Lim.BatchRows = 10 << 32
			proc.Lim.PartitionRows = 10 << 32
			proc.Refer = make(map[string]uint64)
		}
		log := logger.New(os.Stderr, fmt.Sprintf("rpc%v:", i))
		log.SetLevel(logger.WARN)
		srv, err := rpcserver.New(fmt.Sprintf("127.0.0.1:%v", 20000+i+100), 1<<30, logutil.L())
		if err != nil {
			log.Fatal(err)
		}
		hp := handler.New(eng, proc)
		srv.Register(hp.Process)
		go srv.Run()

		svr, err := get_server(testConfigFile, testPorts[i], pcis[i], eng)
		if err != nil {
			t.Error(err)
			return
		}
		svs = append(svs, svr)

		err = svr.Start()
		if err != nil {
			t.Errorf("start server: %v", err)
			return
		}
	}

	time.Sleep(1 * time.Minute)

	fmt.Println("-------------------close node 0----------------")
	//test performance
	c.CubeDrivers[0].Close()

	time.Sleep(10 * time.Minute)

	//DC.Cf.Close()
	select {}

}

func testPCI(id int, f *CloseFlag, pci *PDCallbackImpl) {
	f.Open()
	for f.IsOpened() {
		v := rand.Uint64() % 20
		ep, _ := pci.IncQueryCountAtCurrentEpoch(v)
		if ep == 0 {
			continue
		}
		DC.Set(id, v)
		time.Sleep(1000 * time.Millisecond)
		if rand.Uint32()&0x1 == 0x1 {
			pci.AddMeta(ep, NewMeta(ep, META_TYPE_TABLE, v))
		}
		time.Sleep(1000 * time.Millisecond)
		pci.DecQueryCountAtEpoch(ep, v)
	}
}

var testPorts = []int{6002, 6003, 6004}
var testConfigFile = "./test/system_vars_config.toml"

func get_server(configFile string, port int, pd *PDCallbackImpl, eng engine.Engine) (*MOServer, error) {
	sv := &mo_config.SystemVariables{}

	//before anything using the configuration
	if err := sv.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n", err)
		return nil, err
	}

	if err := mo_config.LoadvarsConfigFromFile(configFile, sv); err != nil {
		fmt.Printf("error:%v\n", err)
		return nil, err
	}

	fmt.Println("Shutdown The *MOServer With Ctrl+C | Ctrl+\\.")

	hostMmu := host.New(sv.GetHostMmuLimitation())
	mempool := mempool.New(int(sv.GetMempoolMaxSize()), int(sv.GetMempoolFactor()))

	fmt.Println("Using Dump Storage Engine and Cluster Nodes.")

	//test storage engine
	storageEngine := eng

	//test cluster nodes
	clusterNodes := metadata.Nodes{}

	pu := mo_config.NewParameterUnit(sv, hostMmu, mempool, storageEngine, clusterNodes)

	address := fmt.Sprintf("%s:%d", sv.GetHost(), port)
	sver := NewMOServer(address, pu, pd)
	return sver, nil
}

func Test_Multi_Server(t *testing.T) {
	var svs []*MOServer = nil
	ppu := NewPDCallbackParameterUnit(5, 20, 20, 20, false)
	for _, port := range testPorts {
		sv, err := get_server(testConfigFile, port, NewPDCallbackImpl(ppu), nil)
		if err != nil {
			t.Error(err)
			return
		}

		svs = append(svs, sv)

		//go sv.Loop()
	}

	time.Sleep(2 * time.Minute)
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
	cell := time.Duration(100)
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

			time.Sleep(cell * time.Millisecond)
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
	ppu := NewPDCallbackParameterUnit(5, 20, 20, 20, false)
	pci := NewPDCallbackImpl(ppu)

	kv := storage.NewTestStorage()

	cnt := 5
	var closeHandle []*CloseFlag = make([]*CloseFlag, cnt)
	for i := 0; i < cnt; i++ {
		closeHandle[i] = &CloseFlag{}
		go run_pci(uint64(i), closeHandle[i], pci, kv)
	}
	time.Sleep(1 * time.Minute)
	for i := 0; i < cnt; i++ {
		closeHandle[i].Close()
	}
	time.Sleep(1 * time.Minute)
}
