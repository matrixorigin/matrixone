package test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/config"
	"matrixone/pkg/server"
	"matrixone/pkg/util/signal"
	catalog2 "matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	"matrixone/pkg/vm/engine/aoe/engine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/host"
	"os"
	"testing"
	"time"
)

var (
	svr      server.Server
	configPath = "./system_vars_config.toml"
)

func TestServer(t *testing.T) {

	c, err := testutil.NewTestClusterStore(t, nil)
	require.NoError(t, err)
	defer c.Stop()

	time.Sleep(2 * time.Second)

	require.NoError(t, err)
	stdLog.Printf("app all started.")

	catalog := catalog2.DefaultCatalog(c.Applications[0])
	aoeEngine := engine.Mock(&catalog)

	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n",err)
		return
	}

	if err := config.LoadvarsConfigFromFile(configPath, &config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n",err)
		return
	}

	fmt.Println("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())

	if ! config.GlobalSystemVariables.GetDumpEnv() {
		fmt.Println("Using Dump Storage Engine and Cluster Nodes.")
		//test storage engine
		config.StorageEngine = aoeEngine

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