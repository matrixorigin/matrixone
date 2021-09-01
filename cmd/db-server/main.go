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
	catalog3 "matrixone/pkg/catalog"
	"matrixone/pkg/config"
	"matrixone/pkg/frontend"
	"matrixone/pkg/logger"
	"matrixone/pkg/logutil"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/sql/handler"
	aoe_engine "matrixone/pkg/vm/engine/aoe/engine"
	engine "matrixone/pkg/vm/engine/aoe/storage"
	dist2 "matrixone/pkg/vm/engine/dist"
	"matrixone/pkg/vm/engine/dist/aoe"
	config2 "matrixone/pkg/vm/engine/dist/config"
	pb2 "matrixone/pkg/vm/engine/dist/pb"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var (
	catalog catalog3.Catalog
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
	//	signal.SetupSignalHandler(serverShutdown)
}

func waitSignal() {
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT)
	<-sigchan
}

func cleanup() {
	fmt.Println("\rBye!")
}

func recreateDir(dir string) (err error) {
	//err = os.RemoveAll(dir)
	//if err != nil {
	//	return err
	//}
	mask := syscall.Umask(0)
	defer syscall.Umask(mask)
	err = os.MkdirAll(dir, os.FileMode(0755))
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
		panic(err)
	}

	if err := config.LoadvarsConfigFromFile(os.Args[1], &config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	fmt.Println("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())
	config.Mempool = mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor()))

	logutil.SetupLogger(os.Args[1])

	Host := config.GlobalSystemVariables.GetHost()
	NodeId := config.GlobalSystemVariables.GetNodeID()
	strNodeId := strconv.FormatInt(NodeId, 10)

	ppu := frontend.NewPDCallbackParameterUnit(int(config.GlobalSystemVariables.GetPeriodOfEpochTimer()), int(config.GlobalSystemVariables.GetPeriodOfPersistence()), int(config.GlobalSystemVariables.GetPeriodOfDDLDeleteTimer()), int(config.GlobalSystemVariables.GetTimeoutOfHeartbeat()), config.GlobalSystemVariables.GetEnableEpochLogging())

	pci = frontend.NewPDCallbackImpl(ppu)
	pci.Id = int(NodeId)

	targetDir := config.GlobalSystemVariables.GetCubeDirPrefix() + strNodeId
	if err := recreateDir(targetDir); err != nil {
		panic(err)
	}

	metaStorage, err := cPebble.NewStorage(targetDir+"/pebble/meta", &pebble.Options{
		FS: vfs.NewPebbleFS(vfs.Default),
	})
	pebbleDataStorage, err := cPebble.NewStorage(targetDir+"/pebble/data", &pebble.Options{
		FS: vfs.NewPebbleFS(vfs.Default),
	})
	var aoeDataStorage *aoe.Storage

	opt := engine.Options{}
	_, err = toml.DecodeFile(os.Args[1], &opt)
	if err != nil {
		panic(err)
	}
	aoeDataStorage, err = aoe.NewStorageWithOptions(targetDir+"/aoe", &opt)

	cfg := config2.Config{}
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

	a, err := dist2.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, &cfg)
	if err != nil {
		fmt.Printf("Create cube driver failed, %v", err)
		panic(err)
	}
	err = a.Start()
	if err != nil {
		fmt.Printf("Start cube driver failed, %v", err)
		panic(err)
	}
	catalog = catalog3.DefaultCatalog(a)
	eng := aoe_engine.New(&catalog)
	pci.SetRemoveEpoch(removeEpoch)

	hm := config.HostMmu
	gm := guest.New(1<<40, hm)
	proc := process.New(gm, config.Mempool)
	{
		proc.Id = "0"
		proc.Lim.Size = config.GlobalSystemVariables.GetProcessLimitationSize()
		proc.Lim.BatchRows = config.GlobalSystemVariables.GetProcessLimitationBatchRows()
		proc.Lim.PartitionRows = config.GlobalSystemVariables.GetProcessLimitationPartitionRows()
		proc.Lim.BatchSize = config.GlobalSystemVariables.GetProcessLimitationBatchSize()
		proc.Refer = make(map[string]uint64)
	}
	log := logger.New(os.Stderr, "rpc"+strNodeId+": ")
	log.SetLevel(logger.WARN)
	srv, err := rpcserver.New(fmt.Sprintf("%s:%d", Host, 20100+NodeId), 1<<30, log)
	if err != nil {
		fmt.Printf("Create rpcserver failed, %v", err)
		panic(err)
	}
	hp := handler.New(eng, proc)
	srv.Register(hp.Process)

	err = waitClusterStartup(a, 300*time.Second, int(cfg.CubeConfig.Prophet.Replication.MaxReplicas), int(cfg.ClusterConfig.PreAllocatedGroupNum))

	if err != nil {
		fmt.Printf("wait cube cluster startup failed, %v", err)
		panic(err)
	}

	go srv.Run()
	//test storage engine
	config.StorageEngine = eng

	//test cluster nodes
	config.ClusterNodes = metadata.Nodes{}

	createMOServer(pci)
	err = runMOServer()
	if err != nil {
		fmt.Printf("Start MOServer failed, %v", err)
		panic(err)
	}
	//registerSignalHandlers()

	waitSignal()
	srv.Stop()
	serverShutdown(true)
	a.Close()
	aoeDataStorage.Close()
	metaStorage.Close()
	pebbleDataStorage.Close()

	cleanup()
	os.Exit(0)
}

func waitClusterStartup(driver dist2.CubeDriver, timeout time.Duration, maxReplicas int, minimalAvailableShard int) error {
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
				router.ForeachShards(uint64(pb2.AOEGroup), func(shard *bhmetapb.Shard) bool {
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
