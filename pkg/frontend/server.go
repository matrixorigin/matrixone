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
	"context"
	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"sync/atomic"
	"syscall"
)

// RelationName counter for the new connection
var initConnectionID uint32 = 1000

// MOServer MatrixOne Server
type MOServer struct {
	addr     string
	uaddr    string
	app      goetty.NetApplication
	app_unix goetty.NetApplication
	rm       *RoutineManager
}

func (mo *MOServer) Start() error {
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("Server Listening on : %s ", mo.addr)
	if mo.app_unix != nil {
		logutil.Infof("Server Listening on : %s ", mo.uaddr)
	}
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")
	logutil.Infof("++++++++++++++++++++++++++++++++++++++++++++++++")

	if mo.app_unix != nil {
		mo.app_unix.Start()
	}
	return mo.app.Start()
}

func (mo *MOServer) Stop() error {
	if mo.app_unix != nil {
		mo.app_unix.Stop()
		syscall.Unlink(mo.uaddr)
	}
	return mo.app.Stop()
}

func nextConnectionID() uint32 {
	return atomic.AddUint32(&initConnectionID, 1)
}

func NewMOServer(ctx context.Context, addr string, pu *config.ParameterUnit) *MOServer {
	codec := NewSqlCodec()
	rm, err := NewRoutineManager(ctx, pu)
	if err != nil {
		logutil.Panicf("start server failed with %+v", err)
	}
	// TODO asyncFlushBatch
	app, err := goetty.NewApplication(addr, rm.Handler,
		goetty.WithAppLogger(logutil.GetGlobalLogger()),
		goetty.WithAppSessionOptions(
			goetty.WithSessionCodec(codec),
			goetty.WithSessionLogger(logutil.GetGlobalLogger()),
			goetty.WithSessionRWBUfferSize(1024*1024, 1024*1024)),
		goetty.WithAppSessionAware(rm))
	if err != nil {
		logutil.Panicf("start server failed with %+v", err)
	}

	app_unix, err := goetty.NewApplication("unix://"+pu.SV.UAddr, rm.Handler,
		goetty.WithAppLogger(logutil.GetGlobalLogger()),
		goetty.WithAppSessionOptions(
			goetty.WithSessionCodec(codec),
			goetty.WithSessionLogger(logutil.GetGlobalLogger()),
			goetty.WithSessionRWBUfferSize(1024*1024, 1024*1024)),
		goetty.WithAppSessionAware(rm))
	if err != nil {
		logutil.Infof("start server on unix domain socket %v failed with %+v", pu.SV.UAddr, err)
	}

	return &MOServer{
		addr:     addr,
		app:      app,
		uaddr:    pu.SV.UAddr,
		app_unix: app_unix,
		rm:       rm,
	}
}
