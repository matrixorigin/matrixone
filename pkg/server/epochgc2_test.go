package server

import (
	"fmt"
	"github.com/fagongzi/log"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cube_config "github.com/matrixorigin/matrixcube/config"
	cube_server "github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	stdLog "log"
	"math/rand"
	"matrixone/pkg/client"
	mo_config "matrixone/pkg/config"
	"matrixone/pkg/logger"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/sql/handler"
	"matrixone/pkg/vm/engine"
	aoe_catalog "matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/dist"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	aoe_dist_config "matrixone/pkg/vm/engine/aoe/dist/config"
	aoe_engine "matrixone/pkg/vm/engine/aoe/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"sync"
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
	err = os.Mkdir(tmpDir, os.ModeDir)
	return err
}

func cleanupTmpDir() error {
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}
	return nil
}

type TestCluster struct {
	T            *testing.T
	Applications []dist.Storage
	Storages  	[]*daoe.Storage
}

var DC *client.DebugCounter = client.NewDebugCounter(32)

func newTestClusterStore(t *testing.T, reCreate bool,
	f func(path string) (storage.DataStorage, error),
	pcis []*client.PDCallbackImpl, nodeCnt int) (*TestCluster, error) {
	if reCreate {
		stdLog.Printf("clean target dir")
		if err := recreateTestTempDir(); err != nil {
			return nil, err
		}
	}
	c := &TestCluster{T: t}
	c.Storages = make([]*daoe.Storage,nodeCnt)
	var wg sync.WaitGroup
	for i := 0; i < nodeCnt; i++ {
		metaStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		pebbleDataStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i))
		var aoeDataStorage storage.DataStorage
		if err != nil {
			return nil, err
		}
		if f == nil {
			c.Storages[i],err = daoe.NewStorage(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
			aoeDataStorage = c.Storages[i]
		} else {
			aoeDataStorage, err = f(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		}

		if err != nil {
			return nil, err
		}
		cfg := aoe_dist_config.Config{}
		cfg.ServerConfig = cube_server.Cfg{
			Addr: fmt.Sprintf("127.0.0.1:809%d", i),
		}
		cfg.ClusterConfig = aoe_dist_config.ClusterConfig{
			PreAllocatedGroupNum: 20,
		}
		cfg.CubeConfig = cube_config.Config{
			DataPath: fmt.Sprintf("%s/node-%d", tmpDir, i),
			RaftAddr: fmt.Sprintf("127.0.0.1:1000%d", i),
			ClientAddr: fmt.Sprintf("127.0.0.1:2000%d", i),
			Replication: cube_config.ReplicationConfig{
				ShardHeartbeatDuration: typeutil.NewDuration(time.Millisecond * 100),
				StoreHeartbeatDuration: typeutil.NewDuration(time.Second),
			},
			Raft: cube_config.RaftConfig{
				TickInterval: typeutil.NewDuration(time.Millisecond * 600),
				MaxEntryBytes: 300 * 1024 * 1024,
			},
			Prophet: pConfig.Config{
				Name: fmt.Sprintf("node-%d", i),
				StorageNode: true,
				RPCAddr: fmt.Sprintf("127.0.0.1:3000%d", i),
				EmbedEtcd: pConfig.EmbedEtcdConfig{
					ClientUrls: fmt.Sprintf("http://127.0.0.1:4000%d", i),
					PeerUrls: fmt.Sprintf("http://127.0.0.1:5000%d", i),
				},
				Schedule: pConfig.ScheduleConfig{
					EnableJointConsensus: true,
				},
			},
		}

		if i < len(pcis){
			cfg.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = pcis[i]
		}

		if i != 0 {
			cfg.CubeConfig.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, err := dist.NewStorageWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, cfg)
			if err != nil {
				log.Fatal("create failed with %+v", err)
			}
			c.Applications = append(c.Applications, a)
		}()
		if i == 0 {
			time.Sleep(3 * time.Second)
		}
		time.Sleep(2 * time.Second)
	}
	wg.Wait()
	return c, nil
}

func (c *TestCluster) stop() {
	for _, s := range c.Applications {
		s.Close()
	}
}
/*
func TestEpochGC(t *testing.T) {
	log.SetLevelByString("error")
	log.SetHighlighting(false)
	util.SetLogger(log.NewLoggerWithPrefix("prophet"))

	defer func() {
		err := cleanupTmpDir()
		if err != nil {
			t.Errorf("delete cube temp dir failed %v",err)
		}
	}()

	go DC.DCRoutine()

	nodeCnt := 3

	pcis := make([]*client.PDCallbackImpl, nodeCnt)
	cf := make([]*client.CloseFlag, nodeCnt)
	for i := 0 ; i < nodeCnt; i++ {
		pcis[i] = client.NewPDCallbackImpl(3000, 10,10)
		pcis[i].Id = i
		cf[i] = &client.CloseFlag{}
		go testPCI(i,cf[i],pcis[i])
	}


	c, err := newTestClusterStore(t,true,nil, pcis, nodeCnt)
	if err != nil {
		t.Errorf("new cube failed %v",err)
		return
	}

	defer c.stop()

	time.Sleep(1 * time.Minute)

	c.Applications[0].Close()

	fmt.Println("-------------------close node 0----------------")

	time.Sleep(1 * time.Minute)
	for i := 0 ; i < nodeCnt; i++ {
		cf[i].Close()
	}

	DC.Cf.Close()
}
*/
func TestEpochGCWithMultiServer(t *testing.T) {
	log.SetLevelByString("error")
	log.SetHighlighting(false)
	util.SetLogger(log.NewLoggerWithPrefix("prophet"))

	defer func() {
		err := cleanupTmpDir()
		if err != nil {
			t.Errorf("delete cube temp dir failed %v",err)
		}
	}()

	//go DC.DCRoutine()

	nodeCnt := 3

	pcis := make([]*client.PDCallbackImpl, nodeCnt)
	for i := 0 ; i < nodeCnt; i++ {
		pcis[i] = client.NewPDCallbackImpl(100, 10,10)
		pcis[i].Id = i
	}

	c, err := newTestClusterStore(t,true,nil, pcis, nodeCnt)
	if err != nil {
		t.Errorf("new cube failed %v",err)
		return
	}

	defer c.stop()

	catalog := aoe_catalog.DefaultCatalog(c.Applications[0])
	eng := aoe_engine.Mock(&catalog)

	server_cnt := 1
	var svs []Server = nil
	for i := 0 ; i < client.Min(server_cnt, client.Min(len(testPorts),nodeCnt)) ; i++ {
		db := c.Storages[i].DB
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
		srv, err := rpcserver.New(fmt.Sprintf("127.0.0.1:%v", 20000+i+100), 1<<30, log)
		if err != nil {
			log.Fatal(err)
		}
		hp := handler.New(db, proc)
		srv.Register(hp.Process)
		go srv.Run()

		svr, err := get_server(testConfigFile, testPorts[i], pcis[i], eng)
		if err != nil {
			t.Error(err)
			return
		}
		svs = append(svs,svr)

		go svr.Loop()
	}

	time.Sleep(2 * time.Minute)

	//test performance
	c.Applications[0].Close()

	fmt.Println("-------------------close node 0----------------")

	time.Sleep(5 * time.Minute)

	//DC.Cf.Close()
}

func testPCI(id int,f*client.CloseFlag, pci *client.PDCallbackImpl) {
	f.Open()
	for f.IsOpened() {
		v := rand.Uint64() % 20
		ep, _ := pci.IncQueryCountAtCurrentEpoch(v)
		if ep == 0 {
			continue
		}
		DC.Set(id,v)
		time.Sleep(1000 * time.Millisecond)
		if rand.Uint32() & 0x1 == 0x1 {
			pci.AddMeta(ep,client.NewMeta(ep,client.META_TYPE_TABLE,v))
		}
		time.Sleep(1000 * time.Millisecond)
		pci.DecQueryCountAtEpoch(ep,v)
	}
}

var testPorts = []int{6002,6003,6004}
var testConfigFile = "./test/system_vars_config.toml"

func get_server(configFile string, port int, pd *client.PDCallbackImpl, eng engine.Engine) (Server, error) {
	sv := &mo_config.SystemVariables{}

	//before anything using the configuration
	if err := sv.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n",err)
		return nil,err
	}

	if err := mo_config.LoadvarsConfigFromFile(configFile, sv); err != nil {
		fmt.Printf("error:%v\n",err)
		return nil,err
	}

	fmt.Println("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	hostMmu := host.New(sv.GetHostMmuLimitation())

	fmt.Println("Using Dump Storage Engine and Cluster Nodes.")

	//test storage engine
	storageEngine := eng

	//test cluster nodes
	clusterNodes := metadata.Nodes{}

	pu := mo_config.NewParameterUnit(sv, hostMmu, storageEngine, clusterNodes)

	address := fmt.Sprintf("%s:%d", sv.GetHost(), port)
	sver := NewServer(address, pu, pd)
	return sver,nil
}

/*
func Test_Multi_Server(t *testing.T) {
	var svs []Server = nil
	for _, port := range testPorts{
		sv, err := get_server(testConfigFile, port, client.NewPDCallbackImpl(1000, 10, 10), nil)
		if err != nil {
			t.Error(err)
			return
		}

		svs = append(svs,sv)

		go sv.Loop()
	}

	time.Sleep(2 * time.Minute)
}

func Test_openfile(t *testing.T) {
	f,err := os.OpenFile(testConfigFile,os.O_RDONLY | os.O_CREATE,0777)
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
*/
