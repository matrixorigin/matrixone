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
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
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

var _ net.Conn = new(testConn)

type testConn struct {
}

func (tc *testConn) Read(b []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (tc *testConn) Write(b []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (tc *testConn) Close() error {
	return moerr.NewInternalErrorNoCtx("must be error")
}

func (tc *testConn) LocalAddr() net.Addr {
	//TODO implement me
	panic("implement me")
}

func (tc *testConn) RemoteAddr() net.Addr {
	//TODO implement me
	panic("implement me")
}

func (tc *testConn) SetDeadline(t time.Time) error {
	//TODO implement me
	panic("implement me")
}

func (tc *testConn) SetReadDeadline(t time.Time) error {
	//TODO implement me
	panic("implement me")
}

func (tc *testConn) SetWriteDeadline(t time.Time) error {
	//TODO implement me
	panic("implement me")
}

func TestRoutineManager_killClients(t *testing.T) {
	type fields struct {
		ctx              context.Context
		clients          map[*Conn]*Routine
		routinesByConnID map[uint32]*Routine
		tlsConfig        *tls.Config
		accountRoutine   *AccountRoutineManager
		baseService      BaseService
		sessionManager   *queryservice.SessionManager
	}

	clients := make(map[*Conn]*Routine)
	for i := 0; i < 3; i++ {
		conn := &Conn{
			id: uint64(i),
		}
		if i == 2 {
			conn.conn = &testConn{}
		}
		clients[conn] = nil

	}

	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "t1",
			fields: fields{
				clients: clients,
			},
		},
	}
	var rm1 *RoutineManager
	rm1.killClients()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm := &RoutineManager{
				ctx:              tt.fields.ctx,
				clients:          tt.fields.clients,
				routinesByConnID: tt.fields.routinesByConnID,
				tlsConfig:        tt.fields.tlsConfig,
				accountRoutine:   tt.fields.accountRoutine,
				baseService:      tt.fields.baseService,
				sessionManager:   tt.fields.sessionManager,
			}
			rm.killClients()
		})
	}
}
