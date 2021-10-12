package frontend

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"matrixone/pkg/config"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/host"
	"sync"
	"testing"
	"time"
)

func create_test_server() *MOServer {
	//before anything using the configuration
	if err := config.GlobalSystemVariables.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	if err := config.LoadvarsConfigFromFile("../../system_vars_config.toml",
		&config.GlobalSystemVariables); err != nil {
		fmt.Printf("error:%v\n", err)
		panic(err)
	}

	config.HostMmu = host.New(config.GlobalSystemVariables.GetHostMmuLimitation())
	config.Mempool = mempool.New(/*int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor())*/)
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes, nil)

	ppu := NewPDCallbackParameterUnit(int(config.GlobalSystemVariables.GetPeriodOfEpochTimer()), int(config.GlobalSystemVariables.GetPeriodOfPersistence()), int(config.GlobalSystemVariables.GetPeriodOfDDLDeleteTimer()), int(config.GlobalSystemVariables.GetTimeoutOfHeartbeat()), config.GlobalSystemVariables.GetEnableEpochLogging(), math.MaxInt64)
	pci := NewPDCallbackImpl(ppu)
	pci.Id = 0

	address := fmt.Sprintf("%s:%d", config.GlobalSystemVariables.GetHost(), config.GlobalSystemVariables.GetPort())
	return NewMOServer(address, pu, pci)
}

func Test_Closed(t *testing.T) {
	mo := create_test_server()
	wg := sync.WaitGroup{}
	wg.Add(1)
	cf := &CloseFlag{}
	go func() {
		cf.Open()
		defer wg.Done()

		err := mo.Start()
		require.NoError(t, err)

		for cf.IsOpened() {
		}
	}()

	time.Sleep(100 * time.Millisecond)
	db := open_db(t,6001)
	time.Sleep(100*time.Millisecond)
	close_db(t,db)
	cf.Close()

	err := mo.Stop()
	require.NoError(t, err)
	wg.Wait()
}