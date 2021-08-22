package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/pebble"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/server"
	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
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
	"matrixone/pkg/vm/engine/aoe/dist/pb"
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
	log.SetLevelByString("info")
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

		metaStorage, err := cPebble.NewStorage(targetDir+"/pebble/meta", &pebble.Options{
			FS: vfs.NewPebbleFS(vfs.Default),
		})
		pebbleDataStorage, err := cPebble.NewStorage(targetDir+"/pebble/data", &pebble.Options{
			FS: vfs.NewPebbleFS(vfs.Default),
		})
		var aoeDataStorage *daoe.Storage

		aoeDataStorage, err = daoe.NewStorage(targetDir + "/aoe")

		cfg := dconfig.Config{}
		_, err = toml.DecodeFile(os.Args[1], &cfg.CubeConfig)
		if err != nil {
			panic(err)
		}

		_, err = toml.DecodeFile(os.Args[1], &cfg.ClusterConfig)
		if err != nil {
			panic(err)
		}
		cfg.ServerConfig = server.Cfg{
			ExternalServer: true,
		}

		cfg.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = pci

		if cfg.CubeConfig.Prophet.EmbedEtcd.ClientUrls != config.GlobalSystemVariables.GetProphetEmbedEtcdJoinAddr() {
			cfg.CubeConfig.Prophet.EmbedEtcd.Join = config.GlobalSystemVariables.GetProphetEmbedEtcdJoinAddr()
		}

		a, err := dist.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, &cfg)
		if err != nil {
			panic(err)
		}
		err = a.Start()
		if err != nil {
			panic(err)
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

		err = waitClusterStartup(a, 10*time.Second, int(cfg.CubeConfig.Prophet.Replication.MaxReplicas), int(cfg.ClusterConfig.PreAllocatedGroupNum))

		if err != nil {
			panic(err)
		}

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
		panic(err)
	}
	//registerSignalHandlers()

	select {}
	cleanup()
	os.Exit(0)
}

func waitClusterStartup(driver dist.CubeDriver, timeout time.Duration, maxReplicas int, minimalAvailableShard int) error {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			return errors.New("wait for available shard timeout")
		default:
			router := driver.RaftStore().GetRouter()
			if router != nil {
				nodeCnt := 0
				shardCnt := 0
				router.ForeachShards(uint64(pb.AOEGroup), func(shard *bhmetapb.Shard) bool {
					fmt.Printf("shard %d, peer count is %d\n", shard.ID, len(shard.Peers))
					shardCnt++
					if len(shard.Peers) > nodeCnt {
						nodeCnt = len(shard.Peers)
					}
					return true
				})
				if nodeCnt >= maxReplicas && shardCnt >= minimalAvailableShard {
					fmt.Println("ClusterStatus is ok now")
					return nil
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}
