package main

import (
	"flag"
	"fmt"
	"matrixone/pkg/config"
	"matrixone/pkg/server"
	"matrixone/pkg/util/signal"
	"matrixone/pkg/vm/engine/memEngine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/host"
	"os"
)

var (
	svr      server.Server
)

func createServer() {
	address := fmt.Sprintf("%s:%d", config.GlobalSystemVariables.GetHost(), config.GlobalSystemVariables.GetPort())
	svr = server.NewServer(address)
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
	config.GlobalSystemVariables.LoadInitialValues()
	config.LoadvarsConfigFromFile(os.Args[1], &config.GlobalSystemVariables)

	fmt.Println("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())

	if config.GlobalSystemVariables.GetDumpEnv() {
		//test storage engine
		config.StorageEngine = memEngine.NewTestEngine()

		//test cluster nodes
		config.ClusterNodes = metadata.Nodes{}
	}else{
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