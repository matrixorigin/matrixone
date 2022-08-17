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
	"context"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
	"os/signal"
	"syscall"

	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"

	"github.com/google/gops/agent"
)

const (
	LoadConfigExit  = 2
	RecreateDirExit = 3
	//	CreateRPCExit     = 10
	StartMOExit = 12
	//	RunRPCExit        = 14
	ShutdownExit    = 15
	CreateTaeExit   = 16
	InitCatalogExit = 17
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

func createMOServer(inputCtx context.Context, pu *config.ParameterUnit) {
	address := fmt.Sprintf("%s:%d", pu.SV.Host, pu.SV.Port)

	moServerCtx := context.WithValue(inputCtx, config.ParameterUnitKey, pu)
	mo = frontend.NewMOServer(moServerCtx, address, pu)
	{
		// init trace/log/error framework
		if _, err := trace.Init(moServerCtx,
			trace.WithMOVersion(MoVersion),
			trace.WithNode(0, trace.NodeTypeNode),
			trace.EnableTracer(!pu.SV.DisableTrace),
			trace.WithBatchProcessMode(pu.SV.TraceBatchProcessor),
			trace.DebugMode(pu.SV.EnableTraceDebug),
			trace.WithSQLExecutor(func() ie.InternalExecutor {
				return frontend.NewInternalExecutor(pu)
			}),
		); err != nil {
			panic(err)
		}
	}

	if !pu.SV.DisableMetric {
		ieFactory := func() ie.InternalExecutor {
			return frontend.NewInternalExecutor(pu)
		}
		metric.InitMetric(moServerCtx, ieFactory, pu, 0, metric.ALL_IN_ONE_MODE)
	}
	frontend.InitServerVersion(MoVersion)
}

func runMOServer() error {
	return mo.Start()
}

func serverShutdown(isgraceful bool) error {
	// flush trace/log/error framework
	if err := trace.Shutdown(trace.DefaultContext()); err != nil {
		logutil.Errorf("Shutdown trace err: %v", err)
	}
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

func initTae(pu *config.ParameterUnit) *taeHandler {
	targetDir := pu.SV.StorePath
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
	pu.StorageEngine = eng

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

	params := &config.FrontendParameters{}
	pu := config.NewParameterUnit(params, nil, nil, nil, nil)

	//before anything using the configuration
	_, err := toml.DecodeFile(configFilePath, params)
	if err != nil {
		os.Exit(LoadConfigExit)
	}

	logConf := logutil.LogConfig{
		Level:       params.LogLevel,
		Format:      params.LogFormat,
		Filename:    params.LogFilename,
		MaxSize:     int(params.LogMaxSize),
		MaxDays:     int(params.LogMaxDays),
		MaxBackups:  int(params.LogMaxBackups),
		EnableStore: !params.DisableTrace,
	}

	logutil.SetupMOLogger(&logConf)

	rootCtx := context.Background()
	cancelMoServerCtx, cancelMoServerFunc := context.WithCancel(rootCtx)

	//just initialize the tae after configuration has been loaded
	if len(args) == 2 && args[1] == "initdb" {
		fmt.Println("Initialize the TAE engine ...")
		taeWrapper := initTae(pu)
		err := frontend.InitDB(cancelMoServerCtx, taeWrapper.eng)
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

	pu.HostMmu = host.New(params.HostMmuLimitation)

	var tae *taeHandler
	fmt.Println("Initialize the TAE engine ...")
	tae = initTae(pu)
	err = frontend.InitDB(cancelMoServerCtx, tae.eng)
	if err != nil {
		logutil.Infof("Initialize catalog failed. error:%v", err)
		os.Exit(InitCatalogExit)
	}
	fmt.Println("Initialize the TAE engine Done")

	if err := agent.Listen(agent.Options{}); err != nil {
		logutil.Errorf("listen gops agent failed: %s", err)
		os.Exit(StartMOExit)
	}

	createMOServer(cancelMoServerCtx, pu)

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

	agent.Close()

	//cancel mo server
	cancelMoServerFunc()

	cleanup()

	closeTae(tae)
}
