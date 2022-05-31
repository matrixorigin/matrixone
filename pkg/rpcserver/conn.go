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
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/connpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/rpcserver/rpchandler"
	"time"
)

// conn pool for per address
var pool map[string]rpchandler.Client

func InitConn() {
	pool = make(map[string]rpchandler.Client)
}

// for conn map, the size of map increase and use memory
func AcquireConn(addr string) *rpchandler.Client {
	conn, ok := pool[addr]
	if !ok {
		maxIdlePerAddress := int(config.GlobalSystemVariables.GetKitexClientMaxIdlePerAddress())
		maxIdleGlobal := int(config.GlobalSystemVariables.GetKitexClientMaxIdleGlobal())
		maxIdleTimeout := time.Duration(config.GlobalSystemVariables.GetKitexClientMaxIdleTimeout()) * time.Second
		conn = rpchandler.MustNewClient("rpcserver.rpchandler.kitex",
			client.WithHostPorts(addr),
			// will use config
			// now use default config
			client.WithLongConnection(
				connpool.IdleConfig{MaxIdlePerAddress: maxIdlePerAddress, MaxIdleGlobal: maxIdleGlobal, MaxIdleTimeout: maxIdleTimeout}),
		)
		pool[addr] = conn
	}
	return &conn
}
