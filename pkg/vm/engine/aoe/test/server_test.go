package test

import (
	"fmt"
	"github.com/fagongzi/log"
	putil "github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/stretchr/testify/require"
	stdLog "log"
	"matrixone/pkg/client"
	"matrixone/pkg/config"
	"matrixone/pkg/logger"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/server"
	"matrixone/pkg/sql/handler"
	"matrixone/pkg/util/signal"
	catalog2 "matrixone/pkg/vm/engine/aoe/catalog"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/testutil"
	"matrixone/pkg/vm/engine/aoe/engine"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"testing"
	"time"
)

var (
	svr        server.Server
	configPath = "./system_vars_config.toml"
)

func TestServer(t *testing.T) {

	log.SetHighlighting(false)
	log.SetLevelByString("error")
	putil.SetLogger(log.NewLoggerWithPrefix("prophet"))
	c, err := testutil.NewTestClusterStore(t, true, func(path string) (*daoe.Storage, error) {
		opts := &e.Options{}
		mdCfg := &md.Configuration{
			Dir:              path,
			SegmentMaxBlocks: blockCntPerSegment,
			BlockMaxRows:     blockRows,
		}
		opts.CacheCfg = &e.CacheCfg{
			IndexCapacity:  blockRows * blockCntPerSegment * 80,
			InsertCapacity: blockRows * uint64(colCnt) * 100,
			DataCapacity:   blockRows * uint64(colCnt) * 100,
		}
		opts.MetaCleanerCfg = &e.MetaCleanerCfg{
			Interval: time.Duration(1) * time.Second,
		}
		opts.Meta.Conf = mdCfg
		return daoe.NewStorageWithOptions(path, opts)
	})
	require.NoError(t, err)
	defer c.Stop()

	time.Sleep(2 * time.Second)

	require.NoError(t, err)
	stdLog.Printf("app all started.")

	catalog := catalog2.DefaultCatalog(c.Applications[0])
	aoeEngine := engine.Mock(&catalog)

	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n", err)
		return
	}

	if err := config.LoadvarsConfigFromFile(configPath, &config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n", err)
		return
	}

	fmt.Println("Shutdown The Server With Ctrl+C | Ctrl+\\.")

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())
	config.Mempool = mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor()))

	if !config.GlobalSystemVariables.GetDumpEnv() {
		fmt.Println("Using Dump Storage Engine and Cluster Nodes.")
		//test storage engine
		config.StorageEngine = aoeEngine

		//test cluster nodes
		config.ClusterNodes = metadata.Nodes{}
	} else {
		panic("The Official Storage Engine and Cluster Nodes are in the developing.")

		//TODO:
		config.StorageEngine = nil

		config.ClusterNodes = nil
	}
	{
		for i := 0; i < 3; i++ {
			db := c.AOEDBs[i]
			hm := host.New(1 << 40)
			gm := guest.New(1<<40, hm)
			proc := process.New(gm, mempool.New(1<<40, 8))
			{
				proc.Id = "0"
				proc.Lim.Size = 10 << 32
				proc.Lim.BatchRows = 10 << 32
				proc.Lim.PartitionRows = 10 << 32
				proc.Refer = make(map[string]uint64)
			}
			log := logger.New(os.Stderr, fmt.Sprintf("rpc%v:", i))
			log.SetLevel(logger.WARN)
			srv, err := rpcserver.New(fmt.Sprintf("127.0.0.1:%v", 20000+i+100), 1<<30, log)
			if err != nil {
				log.Fatal(err)
			}
			hp := handler.New(db.DB, proc)
			srv.Register(hp.Process)
			go srv.Run()
		}
	}
	createServer()
	registerSignalHandlers()
	runServer()
	cleanup()
	os.Exit(0)

}

func createServer() {
	address := fmt.Sprintf("%s:%d", config.GlobalSystemVariables.GetHost(), config.GlobalSystemVariables.GetPort())
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes)
	ppu := client.NewPDCallbackParameterUnit(5,20,20,20)
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
