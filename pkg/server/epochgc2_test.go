package server

import (
	"fmt"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"math/rand"
	"matrixone/pkg/client"
	mo_config "matrixone/pkg/config"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/engine/memEngine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/host"
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
	err = os.Mkdir(tmpDir, os.ModePerm)
	return err
}

func cleanupTmpDir() error {
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}
	return nil
}

type testCluster struct {
	t            *testing.T
	applications []dist.Storage
}

var DC *client.DebugCounter = client.NewDebugCounter(32)

func newTestClusterStore(t *testing.T, pcis []*client.PDCallbackImpl, nodeCnt int) (*testCluster, error) {
	if err := recreateTestTempDir(); err != nil {
		return nil, err
	}
	c := &testCluster{t: t}
	for i := 0; i < nodeCnt; i++ {
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
			if i < len(pcis){
				cfg.Customize.CustomStoreHeartbeatDataProcessor = pcis[i]
			}

		}, server.Cfg{
			Addr: fmt.Sprintf("127.0.0.1:908%d", i),
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
		pcis[i] = client.NewPDCallbackImpl(3000)
		pcis[i].Id = i
		cf[i] = &client.CloseFlag{}
		go testPCI(i,cf[i],pcis[i])
	}


	c, err := newTestClusterStore(t, pcis, nodeCnt)
	if err != nil {
		t.Errorf("new cube failed %v",err)
		return
	}

	defer c.stop()

	time.Sleep(1 * time.Minute)

	c.applications[0].Close()

	fmt.Println("-------------------close node 0----------------")

	time.Sleep(1 * time.Minute)
	for i := 0 ; i < nodeCnt; i++ {
		cf[i].Close()
	}

	DC.Cf.Close()
}

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

	go DC.DCRoutine()

	nodeCnt := 5

	pcis := make([]*client.PDCallbackImpl, nodeCnt)
	for i := 0 ; i < nodeCnt; i++ {
		pcis[i] = client.NewPDCallbackImpl(100)
		pcis[i].Id = i
	}

	c, err := newTestClusterStore(t, pcis, nodeCnt)
	if err != nil {
		t.Errorf("new cube failed %v",err)
		return
	}

	defer c.stop()

	server_cnt := 3
	var svs []Server = nil
	for i := 0 ; i < client.Min(server_cnt, client.Min(len(testPorts),nodeCnt)) ; i++ {
		svr, err := get_server(testConfigFile,testPorts[i],pcis[i])
		if err != nil {
			t.Error(err)
			return
		}
		svs = append(svs,svr)

		go svr.Loop()
	}

	time.Sleep(2 * time.Minute)

	c.applications[0].Close()

	fmt.Println("-------------------close node 0----------------")

	time.Sleep(5 * time.Minute)

	DC.Cf.Close()
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

func get_server(configFile string,port int,pd *client.PDCallbackImpl)(Server,error) {
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
	storageEngine := memEngine.NewTestEngine()

	//test cluster nodes
	clusterNodes := metadata.Nodes{}

	pu := mo_config.NewParameterUnit(sv, hostMmu, storageEngine, clusterNodes)

	address := fmt.Sprintf("%s:%d", sv.GetHost(), port)
	sver := NewServer(address, pu, pd)
	return sver,nil
}

func Test_Multi_Server(t *testing.T) {
	var svs []Server = nil
	for _, port := range testPorts{
		sv, err := get_server(testConfigFile, port, client.NewPDCallbackImpl(1000))
		if err != nil {
			t.Error(err)
			return
		}

		svs = append(svs,sv)

		go sv.Loop()
	}

	time.Sleep(2 * time.Minute)
}

func Test_mysql_client(t *testing.T) {

}