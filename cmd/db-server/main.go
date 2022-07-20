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
	"flag"
	"fmt"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"os"
	"os/signal"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/rpcserver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
)

const (
	InitialValuesExit = 1
	LoadConfigExit    = 2
	RecreateDirExit   = 3
	CreateRPCExit     = 10
	StartMOExit       = 12
	RunRPCExit        = 14
	ShutdownExit      = 15
	CreateTaeExit     = 16
	InitCatalogExit   = 17
)

var (
	mo *frontend.MOServer
)

var (
	GoVersion    = ""
	BranchName   = ""
	LastCommitId = ""
	BuildTime    = ""
	MoVersion    = ""
)

func createMOServer() {
	address := fmt.Sprintf("%s:%d", config.GlobalSystemVariables.GetHost(), config.GlobalSystemVariables.GetPort())
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes)
	mo = frontend.NewMOServer(address, pu)
	if config.GlobalSystemVariables.GetEnableMetric() {
		ieFactory := func() ie.InternalExecutor {
			return frontend.NewIternalExecutor(pu)
		}
		metric.InitMetric(ieFactory, pu, 0, metric.ALL_IN_ONE_MODE)
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

type taeHandler struct {
	eng engine.Engine
	tae *db.DB
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

	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		logutil.Infof("Initial values error:%v\n", err)
		os.Exit(InitialValuesExit)
	}

	if err := config.LoadvarsConfigFromFile(configFilePath, &config.GlobalSystemVariables); err != nil {
		logutil.Infof("Load config error:%v\n", err)
		os.Exit(LoadConfigExit)
	}

	logConf := logutil.LogConfig{
		Level:      config.GlobalSystemVariables.GetLogLevel(),
		Format:     config.GlobalSystemVariables.GetLogFormat(),
		Filename:   config.GlobalSystemVariables.GetLogFilename(),
		MaxSize:    int(config.GlobalSystemVariables.GetLogMaxSize()),
		MaxDays:    int(config.GlobalSystemVariables.GetLogMaxDays()),
		MaxBackups: int(config.GlobalSystemVariables.GetLogMaxBackups()),
	}

	logutil.SetupMOLogger(&logConf)

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
	engineName := config.GlobalSystemVariables.GetStorageEngine()
	port := config.GlobalSystemVariables.GetPortOfRpcServerInComputationEngine()

	var tae *taeHandler
	if engineName == "tae" {
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

	go func() {
		if err := srv.Run(); err != nil {
			logutil.Infof("Start rpcserver failed, %v", err)
			os.Exit(RunRPCExit)
		}
	}()

	createMOServer()

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

	if engineName == "tae" {
		closeTae(tae)
	}
}
