package test

import (
	"fmt"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"io/ioutil"
	"math/rand"
	"matrixone/pkg/client"
	mo_config "matrixone/pkg/config"
	"matrixone/pkg/logger"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/server"
	"matrixone/pkg/sql/handler"
	"matrixone/pkg/vm/engine"
	aoe_catalog "matrixone/pkg/vm/engine/aoe/catalog"
	aoe_engine "matrixone/pkg/vm/engine/aoe/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"testing"
	"time"
)

var DC *client.DebugCounter = client.NewDebugCounter(32)

func TestEpochGC(t *testing.T) {
	log.SetLevelByString("error")
	log.SetHighlighting(false)
	util.SetLogger(log.NewLoggerWithPrefix("prophet"))

	defer func() {
		err := cleanupTmpDir()
		if err != nil {
			t.Errorf("delete cube temp dir failed %v", err)
		}
	}()

	go DC.DCRoutine()

	nodeCnt := 3

	pcis := make([]*client.PDCallbackImpl, nodeCnt)
	cf := make([]*client.CloseFlag, nodeCnt)
	ppu := client.NewPDCallbackParameterUnit(5, 20, 20, 20)
	for i := 0; i < nodeCnt; i++ {
		pcis[i] = client.NewPDCallbackImpl(ppu)
		pcis[i].Id = i
		cf[i] = &client.CloseFlag{}
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
	util.SetLogger(log.NewLoggerWithPrefix("prophet"))

	defer func() {
		err := cleanupTmpDir()
		if err != nil {
			t.Errorf("delete cube temp dir failed %v", err)
		}
	}()

	//go DC.DCRoutine()

	nodeCnt := 3

	pcis := make([]*client.PDCallbackImpl, nodeCnt)
	ppu := client.NewPDCallbackParameterUnit(5, 20, 20, 20)
	for i := 0; i < nodeCnt; i++ {
		pcis[i] = client.NewPDCallbackImpl(ppu)
		pcis[i].Id = i
	}

	c, err := NewTestClusterStore(t, true, nil, pcis, nodeCnt)
	if err != nil {
		t.Errorf("new cube failed %v", err)
		return
	}

	defer c.Stop()

	catalog := aoe_catalog.DefaultCatalog(c.Applications[0])
	eng := aoe_engine.Mock(&catalog)

	for i := 0; i < nodeCnt; i++ {
		pcis[i].SetCatalogService(&catalog)
	}

	server_cnt := 2
	var svs []server.Server = nil
	for i := 0; i < client.Min(server_cnt, client.Min(len(testPorts), nodeCnt)); i++ {
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
		hp := handler.New(eng, proc)
		srv.Register(hp.Process)
		go srv.Run()

		svr, err := get_server(testConfigFile, testPorts[i], pcis[i], eng)
		if err != nil {
			t.Error(err)
			return
		}
		svs = append(svs, svr)

		go svr.Loop()
	}

	time.Sleep(2 * time.Minute)

	//test performance
	//c.Applications[0].Close()

	//fmt.Println("-------------------close node 0----------------")

	time.Sleep(5 * time.Minute)

	//DC.Cf.Close()
}

func testPCI(id int, f *client.CloseFlag, pci *client.PDCallbackImpl) {
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
			pci.AddMeta(ep, client.NewMeta(ep, client.META_TYPE_TABLE, v))
		}
		time.Sleep(1000 * time.Millisecond)
		pci.DecQueryCountAtEpoch(ep, v)
	}
}

var testPorts = []int{6002, 6003, 6004}
var testConfigFile = "./system_vars_config.toml"

func get_server(configFile string, port int, pd *client.PDCallbackImpl, eng engine.Engine) (server.Server, error) {
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

	fmt.Println("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	hostMmu := host.New(sv.GetHostMmuLimitation())
	mempool := mempool.New(int(sv.GetMempoolMaxSize()), int(sv.GetMempoolFactor()))

	fmt.Println("Using Dump Storage Engine and Cluster Nodes.")

	//test storage engine
	storageEngine := eng

	//test cluster nodes
	clusterNodes := metadata.Nodes{}

	pu := mo_config.NewParameterUnit(sv, hostMmu, mempool, storageEngine, clusterNodes)

	address := fmt.Sprintf("%s:%d", sv.GetHost(), port)
	sver := server.NewServer(address, pu, pd)
	return sver, nil
}

func Test_Multi_Server(t *testing.T) {
	var svs []server.Server = nil
	ppu := client.NewPDCallbackParameterUnit(5, 20, 20, 20)
	for _, port := range testPorts {
		sv, err := get_server(testConfigFile, port, client.NewPDCallbackImpl(ppu), nil)
		if err != nil {
			t.Error(err)
			return
		}

		svs = append(svs, sv)

		go sv.Loop()
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
