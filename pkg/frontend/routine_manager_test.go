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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
)

func Test_Closed(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	registerConn(clientConn)
	pu, _ := getParameterUnit("test/system_vars_config.toml", nil, nil)
	pu.SV.SkipCheckUser = true
	setGlobalPu(pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	temp, _ := NewRoutineManager(ctx)
	setGlobalRtMgr(temp)
	mo := createInnerServer()
	wg := sync.WaitGroup{}
	wg.Add(1)
	cf := &CloseFlag{}
	go func() {
		defer wg.Done()
		mo.handleConn(serverConn)
	}()

	time.Sleep(100 * time.Millisecond)
	db, err := openDbConn(t, 6001)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	cf.Close()

	closeDbConn(t, db)
	wg.Wait()
	err = mo.Stop()
	require.NoError(t, err)
	serverConn.Close()
	clientConn.Close()
	wg.Wait()

}
