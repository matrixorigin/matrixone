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
	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
	stdLog "log"
	mo_config "matrixone/pkg/config"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/sql/testutil"
	"matrixone/pkg/vm/driver"
	aoe2 "matrixone/pkg/vm/driver/aoe"
	"matrixone/pkg/vm/driver/config"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
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
		metaStorage, err := cPebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i), &pebble.Options{
			FS: vfs.NewPebbleFS(vfs.Default),
		})
		if err != nil {
			return nil, err
		}
		pebbleDataStorage, err := cPebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i), &pebble.Options{
			FS: vfs.NewPebbleFS(vfs.Default),
		})
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
		cfg.ServerConfig = server.Cfg{
			Addr: fmt.Sprintf("127.0.0.1:809%d", i),
		}
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
			a, err := driver.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, &cfg)
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
}

func NewFrontendStub() (*FrontendStub,error) {
	e, err := testutil.NewTestEngine()
	if err != nil {
		return nil, err
	}
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm)
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
		proc.Refer = make(map[string]uint64)
	}

	srv, err := testutil.NewTestServer(e, proc)
	if err != nil {
		return nil, err
	}

	go srv.Run()

	ppu := NewPDCallbackParameterUnit(1, 1, 1, 1, false, 10000)
	pci := NewPDCallbackImpl(ppu)
	mo, err := get_server("./test/system_vars_config.toml",
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

func get_server(configFile string, port int, pd *PDCallbackImpl, eng engine.Engine) (*MOServer, error) {
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

	fmt.Println("Shutdown The *MOServer With Ctrl+C | Ctrl+\\.")

	hostMmu := host.New(sv.GetHostMmuLimitation())
	mempool := mempool.New( /*int(sv.GetMempoolMaxSize()), int(sv.GetMempoolFactor())*/ )

	fmt.Println("Using Dump Storage Engine and Cluster Nodes.")

	//test storage engine
	storageEngine := eng

	//test cluster nodes
	clusterNodes := metadata.Nodes{}

	pu := mo_config.NewParameterUnit(sv, hostMmu, mempool, storageEngine, clusterNodes, nil)

	address := fmt.Sprintf("%s:%d", sv.GetHost(), port)
	sver := NewMOServer(address, pu, pd)
	return sver, nil
}