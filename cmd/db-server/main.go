package main

import (
	"flag"
	"fmt"
	"matrixone/pkg/client"
	"matrixone/pkg/config"
	"matrixone/pkg/server"
	"matrixone/pkg/util/signal"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/host"
	"os"
)

var (
	svr      server.Server
)

func createServer() {
	address := fmt.Sprintf("%s:%d", config.GlobalSystemVariables.GetHost(), config.GlobalSystemVariables.GetPort())
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes)
	ppu := client.NewPDCallbackParameterUnit(
		int(config.GlobalSystemVariables.GetPeriodOfEpochTimer()),
		int(config.GlobalSystemVariables.GetPeriodOfPersistence()),
		int(config.GlobalSystemVariables.GetPeriodOfDDLDeleteTimer()),
		int(config.GlobalSystemVariables.GetTimeoutOfHeartbeat()))
	svr = server.NewServer(address, pu, client.NewPDCallbackImpl(ppu))
}

func runServer() {
	svr.Loop()
}

func serverShutdown(isgraceful bool) {
	svr.Quit()
}

func registerSignalHandlers() {
	signal.SetupSignalHandler(serverShutdown)
}

func cleanup() {
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s configFile\n", os.Args[0])
		os.Exit(-1)
	}
	flag.Parse()

	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n",err)
		return
	}

	if err := config.LoadvarsConfigFromFile(os.Args[1], &config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n",err)
		return
	}

	fmt.Println("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())
	config.Mempool = mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor()))

	if ! config.GlobalSystemVariables.GetDumpEnv() {
		fmt.Println("Using AOE Storage Engine, 3 Cluster Nodes, 1 SQL Server.")

		//test storage engine
		config.StorageEngine = nil

		//test cluster nodes
		config.ClusterNodes = metadata.Nodes{}
	}else{
		panic("The Official Storage Engine and Cluster Nodes are in the developing.")

		//TODO:
		config.StorageEngine = nil

		config.ClusterNodes = nil
	}

	createServer()
	registerSignalHandlers()
	runServer()
	cleanup()
	os.Exit(0)
}