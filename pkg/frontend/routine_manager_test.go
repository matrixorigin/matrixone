// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

func create_test_server() *MOServer {
	//before anything using the configuration
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
	_, err := toml.DecodeFile("test/system_vars_config.toml", pu.SV)
	if err != nil {
		panic(err)
	}

	pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
	pu.Mempool = mempool.New( /*int(config.GlobalSystemVariables.GetMempoolMaxSize()), int(config.GlobalSystemVariables.GetMempoolFactor())*/ )

	address := fmt.Sprintf("%s:%d", pu.SV.Host, pu.SV.Port)
	moServerCtx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	return NewMOServer(moServerCtx, address, pu)
}

func Test_Closed(t *testing.T) {
	mo := create_test_server()
	mo.rm.SetSkipCheckUser(true)
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
	db := open_db(t, 6001)
	time.Sleep(100 * time.Millisecond)
	close_db(t, db)
	cf.Close()

	err := mo.Stop()
	require.NoError(t, err)
	wg.Wait()
}
