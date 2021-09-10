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
	"log"
	"matrixone/pkg/config"
	"matrixone/pkg/logutil"
	"sync/atomic"

	"github.com/fagongzi/goetty"
)

//ID counter for the new connection
var initConnectionID uint32 = 1000

// MOServer MatrixOne Server
type MOServer struct {
	addr string
	app  goetty.NetApplication
}

func (mo *MOServer) Start() error {
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("Server Listening on : %s \n", mo.addr)
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	fmt.Printf("++++++++++++++++++++++++++++++++++++++++++++++++\n")
	return mo.app.Start()
}

func (mo *MOServer) Stop() error {
	return mo.app.Stop()
}

func nextConnectionID() uint32 {
	return atomic.AddUint32(&initConnectionID, 1)
}

func NewMOServer(addr string, pu *config.ParameterUnit, pdHook *PDCallbackImpl) *MOServer {
	encoder, decoder := NewSqlCodec()
	rm := NewRoutineManager(pu, pdHook)
	// TODO asyncFlushBatch
	app, err := goetty.NewTCPApplication(addr, rm.Handler,
		goetty.WithAppSessionOptions(
			goetty.WithCodec(encoder, decoder),
			goetty.WithLogger(logutil.L()),
			goetty.WithEnableAsyncWrite(64)),
		goetty.WithAppSessionAware(rm))
	if err != nil {
		log.Panicf("start server failed with %+v", err)
	}

	return &MOServer{
		addr: addr,
		app:  app,
	}
}
