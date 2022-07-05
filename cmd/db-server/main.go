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

package main

import (
	"errors"
	"flag"
	"fmt"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"

	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv"
	cPebble "github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/rpcserver"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/handler"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	aoeDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	dConfig "github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	kvDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	aoeEngine "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/engine"
	aoeStorage "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	InitialValuesExit       = 1
	LoadConfigExit          = 2
	RecreateDirExit         = 3
	DecodeAoeConfigExit     = 4
	CreateAoeExit           = 5
	DecodeCubeConfigExit    = 6
	DecodeClusterConfigExit = 7
	CreateCubeExit          = 8
	StartCubeExit           = 9
	CreateRPCExit           = 10
	WaitCubeStartExit       = 11
	StartMOExit             = 12
	CreateTpeExit           = 13
	RunRPCExit              = 14
	ShutdownExit            = 15
	CreateTaeExit           = 16
	InitCatalogExit         = 17
)

var (
	c   *catalog.Catalog
	mo  *frontend.MOServer
	pci *frontend.PDCallbackImpl
)

var (
	GoVersion    = ""
	BranchName   = ""
	LastCommitId = ""
	BuildTime    = ""
	MoVersion    = ""
)

func createMOServer(callback *frontend.PDCallbackImpl) {
	address := fmt.Sprintf("%s:%d", config.GlobalSystemVariables.GetHost(), config.GlobalSystemVariables.GetPort())
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes, config.ClusterCatalog)
	mo = frontend.NewMOServer(address, pu, callback)
	if config.GlobalSystemVariables.GetEnableMetric() {
		ieFactory := func() ie.InternalExecutor {
			return frontend.NewIternalExecutor(pu, callback)
		}
		metric.InitMetric(ieFactory, pu, callback.Id, metric.ALL_IN_ONE_MODE)
	}
	frontend.InitServerVersion(MoVersion)
}

func runMOServer() error {
	return mo.Start()
}

func serverShutdown(isgraceful bool) error {
	return mo.Stop()
}

func waitSignal() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT)
	<-sigchan
}

func cleanup() {
	fmt.Println("\rBye!")
}

func recreateDir(dir string) (err error) {
	mask := syscall.Umask(0)
	defer syscall.Umask(mask)
	err = os.MkdirAll(dir, os.FileMode(0755))
	return err
}

/**
call the catalog service to remove the epoch
*/
func removeEpoch(epoch uint64) {
	//logutil.Infof("removeEpoch %d",epoch)
	var err error
	if c != nil {
		_, err = c.RemoveDeletedTable(epoch)
		if err != nil {
			fmt.Printf("catalog remove ddl failed. error :%v \n", err)
		}
	}
}

type aoeHandler struct {
	cube       driver.CubeDriver
	port       int64
	kvStorage  storage.DataStorage
	aoeStorage storage.DataStorage
	eng        engine.Engine
}

type tpeHandler struct {
	aoe *aoeHandler
}

type taeHandler struct {
	eng engine.Engine
	tae *db.DB
}

func initAoe(configFilePath string) *aoeHandler {
	targetDir := config.GlobalSystemVariables.GetStorePath()
	if err := recreateDir(targetDir); err != nil {
		logutil.Infof("Recreate dir error:%v\n", err)
		os.Exit(RecreateDirExit)
	}

	cfg := parseConfig(configFilePath, targetDir)

	//aoe : kvstorage config
	_, kvStorage := getKVDataStorage(targetDir, cfg)

	//aoe : catalog
	catalogListener := catalog.NewCatalogListener()
	aoeStorage := getAOEDataStorage(configFilePath, targetDir, catalogListener, cfg)

	//aoe cube driver
	a, err := driver.NewCubeDriverWithOptions(kvStorage, aoeStorage, &cfg)
	if err != nil {
		logutil.Infof("Create cube driver failed, %v", err)
		os.Exit(CreateCubeExit)
	}
	err = a.Start()
	if err != nil {
		logutil.Infof("Start cube driver failed, %v", err)
		os.Exit(StartCubeExit)
	}

	//aoe: address for computation
	addr := cfg.CubeConfig.AdvertiseClientAddr
	if len(addr) != 0 {
		logutil.Infof("compile init address from cube AdvertiseClientAddr %s", addr)
	} else {
		logutil.Infof("compile init address from cube ClientAddr %s", cfg.CubeConfig.ClientAddr)
		addr = cfg.CubeConfig.ClientAddr
	}

	//put the node info to the computation
	compile.InitAddress(addr)

	//aoe: catalog
	c = catalog.NewCatalog(a)
	config.ClusterCatalog = c
	catalogListener.UpdateCatalog(c)
	cngineConfig := aoeEngine.EngineConfig{}
	_, err = toml.DecodeFile(configFilePath, &cngineConfig)
	if err != nil {
		logutil.Infof("Decode cube config error:%v\n", err)
		os.Exit(DecodeCubeConfigExit)
	}

	eng := aoeEngine.New(c, &cngineConfig)

	err = waitClusterStartup(a, 300*time.Second, int(cfg.CubeConfig.Prophet.Replication.MaxReplicas), int(cfg.ClusterConfig.PreAllocatedGroupNum))

	if err != nil {
		logutil.Infof("wait cube cluster startup failed, %v", err)
		os.Exit(WaitCubeStartExit)
	}

	//test storage aoe_storage
	config.StorageEngine = eng

	li := strings.LastIndex(cfg.CubeConfig.ClientAddr, ":")
	if li == -1 {
		logutil.Infof("There is no port in client addr")
		os.Exit(LoadConfigExit)
	}
	cubePort, err := strconv.ParseInt(string(cfg.CubeConfig.ClientAddr[li+1:]), 10, 32)
	if err != nil {
		logutil.Infof("Invalid port")
		os.Exit(LoadConfigExit)
	}
	return &aoeHandler{
		cube:       a,
		port:       cubePort,
		kvStorage:  kvStorage,
		aoeStorage: aoeStorage,
		eng:        eng,
	}
}

func closeAoe(aoe *aoeHandler) {
	aoe.kvStorage.Close()
	aoe.aoeStorage.Close()
	aoe.cube.Close()
}

func initTae() *taeHandler {
	targetDir := config.GlobalSystemVariables.GetStorePath()
	if err := recreateDir(targetDir); err != nil {
		logutil.Infof("Recreate dir error:%v\n", err)
		os.Exit(RecreateDirExit)
	}

	tae, err := db.Open(targetDir+"/tae", nil)
	if err != nil {
		logutil.Infof("Open tae failed. error:%v", err)
		os.Exit(CreateTaeExit)
	}

	eng := moengine.NewEngine(tae)

	//test storage aoe_storage
	config.StorageEngine = eng

	return &taeHandler{
		eng: eng,
		tae: tae,
	}
}

func closeTae(tae *taeHandler) {
	_ = tae.tae.Close()
}

func main() {
	// if the argument passed in is "--version", return version info and exit
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println("MatrixOne build info:")
		fmt.Printf("  The golang version used to build this binary: %s\n", GoVersion)
		fmt.Printf("  Git branch name: %s\n", BranchName)
		fmt.Printf("  Last git commit ID: %s\n", LastCommitId)
		fmt.Printf("  Buildtime: %s\n", BuildTime)
		fmt.Printf("  Current Matrixone version: %s\n", MoVersion)
		os.Exit(0)
	}

	flag.Parse()
	args := flag.Args()

	handleDebugFlags()

	if len(args) < 1 {
		fmt.Printf("Usage: %s configFile\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(-1)
	}

	configFilePath := args[0]
	logutil.SetupMOLogger(configFilePath)

	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		logutil.Infof("Initial values error:%v\n", err)
		os.Exit(InitialValuesExit)
	}

	if err := config.LoadvarsConfigFromFile(configFilePath, &config.GlobalSystemVariables); err != nil {
		logutil.Infof("Load config error:%v\n", err)
		os.Exit(LoadConfigExit)
	}

	//just initialize the tae after configuration has been loaded
	if len(args) == 2 && args[1] == "initdb" {
		fmt.Println("Initialize the TAE engine ...")
		taeWrapper := initTae()
		err := frontend.InitDB(taeWrapper.eng)
		if err != nil {
			logutil.Infof("Initialize catalog failed. error:%v", err)
			os.Exit(InitCatalogExit)
		}
		fmt.Println("Initialize the TAE engine Done")
		closeTae(taeWrapper)
		os.Exit(0)
	}

	if *cpuProfilePathFlag != "" {
		stop := startCPUProfile()
		defer stop()
	}
	if *allocsProfilePathFlag != "" {
		defer writeAllocsProfile()
	}

	logutil.Infof("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())

	Host := config.GlobalSystemVariables.GetHost()
	NodeId := config.GlobalSystemVariables.GetNodeID()

	ppu := frontend.NewPDCallbackParameterUnit(int(config.GlobalSystemVariables.GetPeriodOfEpochTimer()), int(config.GlobalSystemVariables.GetPeriodOfPersistence()), int(config.GlobalSystemVariables.GetPeriodOfDDLDeleteTimer()), int(config.GlobalSystemVariables.GetTimeoutOfHeartbeat()), config.GlobalSystemVariables.GetEnableEpochLogging(), math.MaxInt64)

	//aoe : epochgc ?
	pci = frontend.NewPDCallbackImpl(ppu)
	pci.Id = int(NodeId)
	pci.SetRemoveEpoch(removeEpoch)

	engineName := config.GlobalSystemVariables.GetStorageEngine()
	var port int64
	port = config.GlobalSystemVariables.GetPortOfRpcServerInComputationEngine()

	var aoe *aoeHandler
	var tae *taeHandler
	if engineName == "aoe" {
		aoe = initAoe(configFilePath)
		port = aoe.port
	} else if engineName == "tae" {
		fmt.Println("Initialize the TAE engine ...")
		tae = initTae()
		err := frontend.InitDB(tae.eng)
		if err != nil {
			logutil.Infof("Initialize catalog failed. error:%v", err)
			os.Exit(InitCatalogExit)
		}
		fmt.Println("Initialize the TAE engine Done")
	} else {
		logutil.Errorf("undefined engine %s", engineName)
		os.Exit(LoadConfigExit)
	}

	srv, err := rpcserver.New(fmt.Sprintf("%s:%d", Host, port+100), 1<<30, logutil.GetGlobalLogger())
	if err != nil {
		logutil.Infof("Create rpcserver failed, %v", err)
		os.Exit(CreateRPCExit)
	}
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(mheap.New(gm))
	hp := handler.New(config.StorageEngine, proc)
	srv.Register(hp.Process)

	go func() {
		if err := srv.Run(); err != nil {
			logutil.Infof("Start rpcserver failed, %v", err)
			os.Exit(RunRPCExit)
		}
	}()

	createMOServer(pci)

	err = runMOServer()
	if err != nil {
		logutil.Infof("Start MOServer failed, %v", err)
		os.Exit(StartMOExit)
	}

	waitSignal()
	//srv.Stop()
	if err := serverShutdown(true); err != nil {
		logutil.Infof("Server shutdown failed, %v", err)
		os.Exit(ShutdownExit)
	}

	cleanup()

	if engineName == "aoe" {
		closeAoe(aoe)
	} else if engineName == "tae" {
		closeTae(tae)
	}
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
				router.ForeachShards(uint64(pb.AOEGroup), func(shard metapb.Shard) bool {
					fmt.Printf("shard %d, peer count is %d\n", shard.ID, len(shard.Replicas))
					shardCnt++
					if len(shard.Replicas) < nodeCnt {
						nodeCnt = len(shard.Replicas)
					}
					return true
				})
				if nodeCnt >= maxReplicas && shardCnt >= minimalAvailableShard {
					kvNodeCnt := maxReplicas
					kvCnt := 0
					router.ForeachShards(uint64(pb.KVGroup), func(shard metapb.Shard) bool {
						kvCnt++
						if len(shard.Replicas) < kvNodeCnt {
							kvNodeCnt = len(shard.Replicas)
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

func parseConfig(configFilePath, targetDir string) dConfig.Config {
	cfg := dConfig.Config{}
	_, err := toml.DecodeFile(configFilePath, &cfg.CubeConfig)
	if err != nil {
		logutil.Infof("Decode cube config error:%v\n", err)
		os.Exit(DecodeCubeConfigExit)
	}
	_, err = toml.DecodeFile(configFilePath, &cfg.FeaturesConfig)
	if err != nil {
		logutil.Infof("Decode cube config error:%v\n", err)
		os.Exit(DecodeCubeConfigExit)
	}

	cfg.CubeConfig.DataPath = targetDir + "/cube"
	_, err = toml.DecodeFile(configFilePath, &cfg.ClusterConfig)
	if err != nil {
		logutil.Infof("Decode cluster config error:%v\n", err)
		os.Exit(DecodeClusterConfigExit)
	}

	if !config.GlobalSystemVariables.GetDisablePCI() {
		cfg.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = pci
	}
	cfg.CubeConfig.Logger = logutil.GetGlobalLogger()
	return cfg
}

func getKVDataStorage(targetDir string, cfg dConfig.Config) (*cPebble.Storage, storage.DataStorage) {
	kvs, err := cPebble.NewStorage(targetDir+"/pebble/data", nil, &pebble.Options{
		FS:                          vfs.NewPebbleFS(vfs.Default),
		MemTableSize:                1024 * 1024 * 128,
		MemTableStopWritesThreshold: 4,
	})
	if err != nil {
		logutil.Infof("create kv data storage error, %v\n", err)
		os.Exit(CreateAoeExit)
	}

	kvBase := kv.NewBaseStorage(kvs, vfs.Default)
	return kvs, kv.NewKVDataStorage(kvBase, kvDriver.NewkvExecutor(kvs),
		kv.WithLogger(cfg.CubeConfig.Logger),
		kv.WithFeature(cfg.FeaturesConfig.KV.Feature()))
}

func getAOEDataStorage(configFilePath, targetDir string,
	catalogListener *catalog.CatalogListener,
	cfg dConfig.Config) storage.DataStorage {
	var aoeDataStorage *aoeDriver.Storage
	opt := aoeStorage.Options{}
	_, err := toml.DecodeFile(configFilePath, &opt)
	if err != nil {
		logutil.Infof("Decode aoe config error:%v\n", err)
		os.Exit(DecodeAoeConfigExit)
	}

	opt.EventListener = catalogListener
	aoeDataStorage, err = aoeDriver.NewStorageWithOptions(targetDir+"/aoe",
		cfg.FeaturesConfig.AOE.Feature(), &opt)
	if err != nil {
		logutil.Infof("Create aoe driver error, %v\n", err)
		os.Exit(CreateAoeExit)
	}
	return aoeDataStorage
}
