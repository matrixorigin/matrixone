package main

import (
	"errors"
	"flag"
	"fmt"
	"matrixone/pkg/catalog"
	"matrixone/pkg/config"
	"matrixone/pkg/frontend"
	"matrixone/pkg/logutil"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/sql/handler"
	"matrixone/pkg/vm/driver"
	aoeDriver "matrixone/pkg/vm/driver/aoe"
	dConfig "matrixone/pkg/vm/driver/config"
	"matrixone/pkg/vm/driver/pb"
	aoeEngine "matrixone/pkg/vm/engine/aoe/engine"
	aoeStorage "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/pebble"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/server"
	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	c   *catalog.Catalog
	mo  *frontend.MOServer
	pci *frontend.PDCallbackImpl
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
	_, err := c.RemoveDeletedTable(epoch)
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

	logutil.SetupMOLogger(os.Args[1])
	log.SetLevelByString(config.GlobalSystemVariables.GetCubeLogLevel())

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
	var aoeDataStorage *aoeDriver.Storage

	opt := aoeStorage.Options{}
	_, err = toml.DecodeFile(os.Args[1], &opt)
	if err != nil {
		panic(err)
	}
	aoeDataStorage, err = aoeDriver.NewStorageWithOptions(targetDir+"/aoe", &opt)

	cfg := dConfig.Config{}
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

	a, err := driver.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, &cfg)
	if err != nil {
		fmt.Printf("Create cube driver failed, %v", err)
		panic(err)
	}
	err = a.Start()
	if err != nil {
		fmt.Printf("Start cube driver failed, %v", err)
		panic(err)
	}
	c = catalog.NewCatalog(a)
	eng := aoeEngine.New(c)
	pci.SetRemoveEpoch(removeEpoch)

	hm := config.HostMmu
	gm := guest.New(1<<40, hm)
	proc := process.New(gm)
	{
		proc.Id = "0"
		proc.Lim.Size = config.GlobalSystemVariables.GetProcessLimitationSize()
		proc.Lim.BatchRows = config.GlobalSystemVariables.GetProcessLimitationBatchRows()
		proc.Lim.PartitionRows = config.GlobalSystemVariables.GetProcessLimitationPartitionRows()
		proc.Lim.BatchSize = config.GlobalSystemVariables.GetProcessLimitationBatchSize()
		proc.Refer = make(map[string]uint64)
	}
	/*	log := logger.New(os.Stderr, "rpc"+strNodeId+": ")
		log.SetLevel(logger.WARN)*/
	srv, err := rpcserver.New(fmt.Sprintf("%s:%d", Host, 20100+NodeId), 1<<30, logutil.GetGlobalLogger())
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
	//test storage aoe_storage
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

func waitClusterStartup(driver driver.CubeDriver, timeout time.Duration, maxReplicas int, minimalAvailableShard int) error {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			return errors.New("wait for available shard timeout")
		default:
			router := driver.RaftStore().GetRouter()
			if router != nil {
				nodeCnt := maxReplicas
				shardCnt := 0
				router.ForeachShards(uint64(pb.AOEGroup), func(shard *bhmetapb.Shard) bool {
					fmt.Printf("shard %d, peer count is %d\n", shard.ID, len(shard.Peers))
					shardCnt++
					if len(shard.Peers) < nodeCnt {
						nodeCnt = len(shard.Peers)
					}
					return true
				})
				if nodeCnt >= maxReplicas && shardCnt >= minimalAvailableShard {
					kvNodeCnt := maxReplicas
					kvCnt := 0
					router.ForeachShards(uint64(pb.KVGroup), func(shard *bhmetapb.Shard) bool {
						kvCnt++
						if len(shard.Peers) < kvNodeCnt {
							kvNodeCnt = len(shard.Peers)
						}
						return true
					})
					if kvCnt >= 1 && kvNodeCnt >= maxReplicas {
						fmt.Println("ClusterStatus is ok now")
						return nil
					}

				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}
