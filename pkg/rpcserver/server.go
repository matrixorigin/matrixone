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

package rpcserver

import (
	"github.com/cloudwego/kitex/pkg/klog"
	ksvr "github.com/cloudwego/kitex/server"
	"github.com/matrixorigin/matrixone/pkg/rpcserver/klogger"
	"github.com/matrixorigin/matrixone/pkg/rpcserver/message"
	"github.com/matrixorigin/matrixone/pkg/rpcserver/rpchandler"
	"net"
)

func New(addr string, handler message.RPCHandler) (Server, error) {
	// init conn pool
	InitConn()
	address := &net.UnixAddr{Net: "tcp", Name: addr}
	klog.SetLogger(klogger.New())
	svr := rpchandler.NewServer(handler, ksvr.WithServiceAddr(address))
	s := new(server)
	s.Server = svr
	return s, nil
}

func (s *server) Run() error {
	return s.Server.Run()
}

func (s *server) Stop() error {
	return s.Server.Stop()
}
