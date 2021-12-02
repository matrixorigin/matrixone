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

package frontend

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/kv"
	cPebble "github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/rpcserver"
	"github.com/matrixorigin/matrixone/pkg/sql/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	aoe2 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	kvDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/kv"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	stdLog "log"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	tmpDir = "./cube-test"
)

type TestCluster struct {
	T            *testing.T
	Applications []driver.CubeDriver
	AOEDBs       []*aoe2.Storage
}

func NewTestClusterStore(t *testing.T, reCreate bool,
	f func(path string) (*aoe2.Storage, error),
	pcis []*PDCallbackImpl, nodeCnt int) (*TestCluster, error) {
	if reCreate {
		stdLog.Printf("clean target dir")
		if err := recreateTestTempDir(); err != nil {
			return nil, err
		}
	}
	c := &TestCluster{T: t}
	var wg sync.WaitGroup
	for i := 0; i < nodeCnt; i++ {
		kvs, err := cPebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i), nil, &pebble.Options{
			FS: vfs.NewPebbleFS(vfs.Default),
		})
		kvBase := kv.NewBaseStorage(kvs,vfs.Default)
		pebbleDataStorage := kv.NewKVDataStorage(kvBase, kvDriver.NewkvExecutor(kvs))
		var aoeDataStorage *aoe2.Storage
		if err != nil {
			return nil, err
		}
		if f == nil {
			aoeDataStorage, err = aoe2.NewStorage(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		} else {
			aoeDataStorage, err = f(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		}

		if err != nil {
			return nil, err
		}
		cfg := config.Config{}
		cfg.ServerConfig = server.Cfg{}
		cfg.ClusterConfig = config.ClusterConfig{
			PreAllocatedGroupNum: 20,
		}
		cfg.CubeConfig = cConfig.Config{
			DataPath:   fmt.Sprintf("%s/node-%d", tmpDir, i),
			RaftAddr:   fmt.Sprintf("127.0.0.1:1000%d", i),
			ClientAddr: fmt.Sprintf("127.0.0.1:2000%d", i),
			Replication: cConfig.ReplicationConfig{
				ShardHeartbeatDuration: typeutil.NewDuration(time.Millisecond * 100),
				StoreHeartbeatDuration: typeutil.NewDuration(time.Second),
			},
			Raft: cConfig.RaftConfig{
				TickInterval:  typeutil.NewDuration(time.Millisecond * 600),
				MaxEntryBytes: 300 * 1024 * 1024,
			},
			Prophet: pConfig.Config{
				Name:        fmt.Sprintf("node-%d", i),
				StorageNode: true,
				RPCAddr:     fmt.Sprintf("127.0.0.1:3000%d", i),
				EmbedEtcd: pConfig.EmbedEtcdConfig{
					ClientUrls: fmt.Sprintf("http://127.0.0.1:4000%d", i),
					PeerUrls:   fmt.Sprintf("http://127.0.0.1:5000%d", i),
				},
				Schedule: pConfig.ScheduleConfig{
					EnableJointConsensus: true,
				},
			},
		}

		if i < len(pcis) {
			cfg.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = pcis[i]
		}

		if i != 0 {
			cfg.CubeConfig.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, err := driver.NewCubeDriverWithOptions(pebbleDataStorage, aoeDataStorage, &cfg)
			if err != nil {
				fmt.Printf("create failed with %v", err)
			}
			c.AOEDBs = append(c.AOEDBs, aoeDataStorage)
			c.Applications = append(c.Applications, a)
		}()
		if i == 0 {
			time.Sleep(3 * time.Second)
		}
		time.Sleep(2 * time.Second)
	}
	wg.Wait()
	return c, nil
}

func (c *TestCluster) Stop() {
	for _, s := range c.Applications {
		s.Close()
	}
}

func recreateTestTempDir() (err error) {
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return err
	}
	err = os.MkdirAll(tmpDir, 0755)
	return err
}

func cleanupTmpDir() error {
	return os.RemoveAll(tmpDir)
}

type FrontendStub struct{
	eng engine.Engine
	srv rpcserver.Server
	mo *MOServer
	kvForEpochgc storage.Storage
	wg sync.WaitGroup
	cf *CloseFlag
	pci *PDCallbackImpl
	proc *process.Process
}

func NewFrontendStub() (*FrontendStub,error) {
	e, srv, err, proc := getMemEngineAndComputationEngine()
	if err != nil {
		return nil,err
	}

	pci := getPCI()
	mo, err := getMOserver("./test/system_vars_config.toml",
		6002, pci , e)

	if err != nil {
		return nil, err
	}

	err = mo.Start()
	if err != nil {
		return nil, err
	}

	return &FrontendStub{
		eng: e,
		srv: srv,
		mo: mo,
		kvForEpochgc: storage.NewTestStorage(),
		cf:&CloseFlag{},
		pci:pci,
		proc:proc,
	},nil
}

func StartFrontendStub(fs *FrontendStub) error {
	return fs.pci.Start(fs.kvForEpochgc)
}

func CloseFrontendStub(fs *FrontendStub) error {
	err := fs.mo.Stop()
	if err != nil {
		return err
	}

	fs.srv.Stop()
	return fs.pci.Stop(fs.kvForEpochgc)
}

var testPorts = []int{6002, 6003, 6004}
var testConfigFile = "./test/system_vars_config.toml"

func getMemEngineAndComputationEngine() (engine.Engine, rpcserver.Server, error, *process.Process) {
	e, err := testutil.NewTestEngine()
	if err != nil {
		return nil, nil, err, nil
	}
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(mheap.New(gm))
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
	}

	srv, err := testutil.NewTestServer(e, proc)
	if err != nil {
		return nil, nil, err, nil
	}

	go srv.Run()
	return e, srv, err, proc
}

func getMOserver(configFile string, port int, pd *PDCallbackImpl, eng engine.Engine) (*MOServer, error) {
	pu,err := getParameterUnit(configFile,eng)
	if err != nil {
		return nil,err
	}

	address := fmt.Sprintf("%s:%d", pu.SV.GetHost(), port)
	sver := NewMOServer(address, pu, pd)
	return sver, nil
}

func getPCI()(*PDCallbackImpl) {
	ppu := NewPDCallbackParameterUnit(1, 1, 1, 1, false, 10000)
	return NewPDCallbackImpl(ppu)
}

func getParameterUnit(configFile string, eng engine.Engine) (*mo_config.ParameterUnit, error) {
	sv := &mo_config.SystemVariables{}

	//before anything using the configuration
	if err := sv.LoadInitialValues(); err != nil {
		fmt.Printf("error:%v\n", err)
		return nil, err
	}

	if err := mo_config.LoadvarsConfigFromFile(configFile, sv); err != nil {
		fmt.Printf("error:%v\n", err)
		return nil, err
	}

	hostMmu := host.New(sv.GetHostMmuLimitation())
	mempool := mempool.New( /*int(sv.GetMempoolMaxSize()), int(sv.GetMempoolFactor())*/ )

	fmt.Println("Using Dump Storage Engine and Cluster Nodes.")

	pu := mo_config.NewParameterUnit(sv, hostMmu, mempool, eng, engine.Nodes{}, nil)

	return pu,nil
}

type ChannelProtocolStub struct {
	eng engine.Engine
	srv rpcserver.Server
	pci *PDCallbackImpl
	pu *mo_config.ParameterUnit
	cps *ChannelProtocolServer
	kvForEpochgc storage.Storage
}

func NewChannelProtocolStub () (*ChannelProtocolStub,error) {
	e, srv, err, _ := getMemEngineAndComputationEngine()
	if err != nil {
		return nil,err
	}

	pci := getPCI()
	pu, err := getParameterUnit(testConfigFile,e)
	if err != nil {
		return nil,err
	}

	cps := NewChannelProtocolServer(pu,pci)

	return &ChannelProtocolStub{
		eng: e,
		srv: srv,
		pci: pci,
		pu:  pu,
		cps: cps,
		kvForEpochgc: storage.NewTestStorage(),
	},nil
}

func StartChannelProtocolStub(cps *ChannelProtocolStub) error {
	return cps.pci.Start(cps.kvForEpochgc)
}

func CloseChannelProtocolStub(cps *ChannelProtocolStub) error {
	return cps.pci.Stop(cps.kvForEpochgc)
}