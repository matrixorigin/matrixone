// Copyright 2024 Matrix Origin
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

package testutil

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type testClientSession struct {
	tailReceiveQueue chan morpc.Message
}

func (cs *testClientSession) SessionCtx() context.Context {
	//TODO implement me
	panic("implement me")
}

func (cs *testClientSession) Close() error { return nil }
func (cs *testClientSession) Write(ctx context.Context, response morpc.Message) error {
	cs.tailReceiveQueue <- response
	return nil
}

func (cs *testClientSession) AsyncWrite(response morpc.Message) error { return nil }
func (cs *testClientSession) CreateCache(ctx context.Context, cacheID uint64) (morpc.MessageCache, error) {
	return nil, nil
}
func (cs *testClientSession) DeleteCache(cacheID uint64)                          {}
func (cs *testClientSession) GetCache(cacheID uint64) (morpc.MessageCache, error) { return nil, nil }
func (cs *testClientSession) RemoteAddress() string                               { return "" }

func newTestClientSession(dst chan morpc.Message) *testClientSession {
	return &testClientSession{
		tailReceiveQueue: dst,
	}
}

type TestLogtailServer struct {
	*service.LogtailServer
}

func NewMockLogtailServer(
	ctx context.Context, tae *db.DB, cfg *options.LogtailServerCfg, rt runtime.Runtime,
	rpcServerFactory func(string, string, *service.LogtailServer, ...morpc.ServerOption) (morpc.RPCServer, error)) (*TestLogtailServer, error) {

	ls := &TestLogtailServer{}

	logtailer := taelogtail.NewLogtailer(ctx, tae.BGCheckpointRunner, tae.LogtailMgr, tae.Catalog)
	server, err := service.NewLogtailServer("", cfg, logtailer, rt, rpcServerFactory)
	if err != nil {
		return nil, err
	}

	ls.LogtailServer = server

	return ls, nil
}

func defaultLogtailConfig() *options.LogtailServerCfg {
	return options.NewDefaultLogtailServerCfg()
}
