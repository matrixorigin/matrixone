package main

import (
	"flag"
	"fmt"
	"github.com/fagongzi/log"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	stdLog "log"
	"matrixone/pkg/config"
	"matrixone/pkg/frontend"
	"matrixone/pkg/logger"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/sql/handler"
	"matrixone/pkg/util/signal"
	aoe_catalog "matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/dist"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	dconfig "matrixone/pkg/vm/engine/aoe/dist/config"
	aoe_engine "matrixone/pkg/vm/engine/aoe/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"strconv"
	"time"
)

var (
	catalog aoe_catalog.Catalog
	mo      *frontend.MOServer
	pci     *frontend.PDCallbackImpl
)

func createMOServer(callback *frontend.PDCallbackImpl) {
	address := fmt.Sprintf("%s:%d", config.GlobalSystemVariables.GetHost(), config.GlobalSystemVariables.GetPort())
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes)
	mo = frontend.NewMOServer(address, pu, callback)
}

func runMOServer() error {
	return mo.Start()
}

func serverShutdown(isgraceful bool) {
	mo.Stop()
}

func registerSignalHandlers() {
	signal.SetupSignalHandler(serverShutdown)
}

func cleanup() {
}

func recreateDir(dir string) (err error) {
	err = os.RemoveAll(dir)
	if err != nil {
		return err
	}
	err = os.MkdirAll(dir, os.ModeDir)
	return err
}

/**
call the catalog service to remove the epoch
*/
func removeEpoch(epoch uint64) {
	_, err := catalog.RemoveDeletedTable(epoch)
	if err != nil {
		fmt.Printf("catalog remove ddl failed. error :%v \n", err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s configFile\n", os.Args[0])
		os.Exit(-1)
	}
	flag.Parse()

	//close cube print info
	log.SetLevelByString("error")
	log.SetHighlighting(false)
	util.SetLogger(log.NewLoggerWithPrefix("prophet"))

	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n", err)
		return
	}

	if err := config.LoadvarsConfigFromFile(os.Args[1], &config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n", err)
		return
	}

	fmt.Println("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())
	config.Mempool = mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor()))

	if !config.GlobalSystemVariables.GetDumpEnv() {
		fmt.Println("Using AOE Storage Engine, 3 Cluster Nodes, 1 SQL Server.")
		Host := config.GlobalSystemVariables.GetHost()
		NodeId := config.GlobalSystemVariables.GetNodeID()
		strNodeId := strconv.FormatInt(NodeId, 10)

		ppu := frontend.NewPDCallbackParameterUnit(
			int(config.GlobalSystemVariables.GetPeriodOfEpochTimer()),
			int(config.GlobalSystemVariables.GetPeriodOfPersistence()),
			int(config.GlobalSystemVariables.GetPeriodOfDDLDeleteTimer()),
			int(config.GlobalSystemVariables.GetTimeoutOfHeartbeat()))

		pci = frontend.NewPDCallbackImpl(ppu)
		pci.Id = int(NodeId)

		stdLog.Printf("clean target dir")

		targetDir := config.GlobalSystemVariables.GetCubeDir() + strNodeId
		if err := recreateDir(targetDir); err != nil {
			return
		}

		metaStorage, err := pebble.NewStorage(targetDir + "/pebble/meta")
		pebbleDataStorage, err := pebble.NewStorage(targetDir + "/pebble/data")
		var aoeDataStorage *daoe.Storage

		aoeDataStorage, err = daoe.NewStorage(targetDir + "/aoe")

		cfg := dconfig.Config{}
		cfg.ServerConfig = server.Cfg{
			//Addr: fmt.Sprintf("127.0.0.1:8092"),
			ExternalServer: true,
		}
		cfg.ClusterConfig = dconfig.ClusterConfig{
			PreAllocatedGroupNum: 20,
		}
		cfg.CubeConfig = cConfig.Config{
			DataPath:   targetDir + "/node",
			RaftAddr:   fmt.Sprintf("%s:%d", Host, config.GlobalSystemVariables.GetRaftAddrPort()),
			ClientAddr: fmt.Sprintf("%s:%d", Host, config.GlobalSystemVariables.GetClientAddrPort()),
			Replication: cConfig.ReplicationConfig{
				ShardHeartbeatDuration: typeutil.NewDuration(time.Millisecond * 100),
				StoreHeartbeatDuration: typeutil.NewDuration(time.Second),
			},
			Raft: cConfig.RaftConfig{
				TickInterval:  typeutil.NewDuration(time.Millisecond * 600),
				MaxEntryBytes: 300 * 1024 * 1024,
			},
			Prophet: pConfig.Config{
				Name:        "node" + strNodeId,
				StorageNode: true,
				RPCAddr:     fmt.Sprintf("%s:%d", Host, config.GlobalSystemVariables.GetProphetRPCAddrPort()),
				EmbedEtcd: pConfig.EmbedEtcdConfig{
					ClientUrls: fmt.Sprintf("http://%s:%d", Host, config.GlobalSystemVariables.GetProphetClientUrlPort()),
					PeerUrls:   fmt.Sprintf("http://%s:%d", Host, config.GlobalSystemVariables.GetProphetPeerUrlPort()),
				},
				Schedule: pConfig.ScheduleConfig{
					EnableJointConsensus: true,
				},
			},
		}

		cfg.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = pci

		if cfg.CubeConfig.Prophet.EmbedEtcd.ClientUrls != config.GlobalSystemVariables.GetProphetEmbedEtcdJoinAddr() {
			cfg.CubeConfig.Prophet.EmbedEtcd.Join = config.GlobalSystemVariables.GetProphetEmbedEtcdJoinAddr()
		}

		a, err := dist.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, &cfg)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = a.Start()
		if err != nil {
			fmt.Println(err)
			return
		}
		catalog = aoe_catalog.DefaultCatalog(a)
		eng := aoe_engine.Mock(&catalog)
		pci.SetRemoveEpoch(removeEpoch)

		hm := config.HostMmu
		gm := guest.New(1<<40, hm)
		proc := process.New(gm, config.Mempool)
		{
			proc.Id = "0"
			proc.Lim.Size = config.GlobalSystemVariables.GetProcessLimitationSize()
			proc.Lim.BatchRows = config.GlobalSystemVariables.GetProcessLimitationBatchRows()
			proc.Lim.PartitionRows = config.GlobalSystemVariables.GetProcessLimitationPartitionRows()
			proc.Refer = make(map[string]uint64)
		}
		log := logger.New(os.Stderr, "rpc"+strNodeId+": ")
		log.SetLevel(logger.WARN)
		srv, err := rpcserver.New(fmt.Sprintf("%s:%d", Host, 20100+NodeId), 1<<30, log)
		if err != nil {
			log.Fatal(err)
		}
		hp := handler.New(eng, proc)
		srv.Register(hp.Process)
		go srv.Run()
		//test storage engine
		config.StorageEngine = eng

		//test cluster nodes
		config.ClusterNodes = metadata.Nodes{}
	} else {
		panic("The Official Storage Engine and Cluster Nodes are in the developing.")

		//TODO:
		config.StorageEngine = nil

		config.ClusterNodes = nil
	}

	createMOServer(pci)
	err := runMOServer()
	if err != nil {
		fmt.Println(err)
		return
	}
	//registerSignalHandlers()

	select {}
	cleanup()
	os.Exit(0)
}
