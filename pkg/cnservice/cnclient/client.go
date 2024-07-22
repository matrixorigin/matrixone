// Copyright 2021 - 2022 Matrix Origin
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

package cnclient

import (
	"fmt"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
)

func (cfg *PipelineConfig) fill() {
	if cfg.MaxSenderNumber <= 0 {
		cfg.MaxSenderNumber = dfMaxSenderNumber
	}
	if cfg.ReadBufferSize < 0 {
		cfg.ReadBufferSize = dfClientReadBufferSize
	}
	if cfg.WriteBufferSize < 0 {
		cfg.WriteBufferSize = dfClientWriteBufferSize
	}
	if cfg.TimeOutForEachConnect <= 0 {
		cfg.TimeOutForEachConnect = dfConnectTimeout
	}
}

func GetPipelineClient(
	sid string,
) PipelineClient {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.PipelineClient)
	if !ok {
		return nil
	}
	return v.(PipelineClient)
}

type pipelineClient struct {
	localServiceAddress string
	config              *PipelineConfig
	client              morpc.RPCClient
}

func NewPipelineClient(
	sid string,
	localServiceAddress string,
	cfg *PipelineConfig,
) (PipelineClient, error) {
	logger := logutil.GetGlobalLogger().Named("cn-backend")

	cfg.fill()

	c := &pipelineClient{
		localServiceAddress: localServiceAddress,
		config:              cfg,
	}

	codec := morpc.NewMessageCodec(
		sid,
		func() morpc.Message { return AcquireMessage() },
		morpc.WithCodecMaxBodySize(int(cfg.RPC.MaxMessageSize)),
	)
	factory := morpc.NewGoettyBasedBackendFactory(
		codec,
		morpc.WithBackendGoettyOptions(
			goetty.WithSessionRWBUfferSize(
				cfg.ReadBufferSize,
				cfg.WriteBufferSize,
			),
			goetty.WithSessionReleaseMsgFunc(
				func(v any) {
					m := v.(morpc.RPCMessage)
					if !m.InternalMessage() {
						ReleaseMessage(m.Message.(*pipeline.Message))
					}
				},
			),
		),
		morpc.WithBackendReadTimeout(defaultRPCTimeout),
		morpc.WithBackendConnectTimeout(cfg.TimeOutForEachConnect),
		morpc.WithBackendLogger(logger),
	)

	cli, err := morpc.NewClient(
		"pipeline-client",
		factory,
		morpc.WithClientMaxBackendPerHost(cfg.MaxSenderNumber),
		morpc.WithClientLogger(logger),
	)
	if err != nil {
		return nil, err
	}

	c.client = cli
	return c, nil
}

func (c *pipelineClient) NewStream(backend string) (morpc.Stream, error) {
	if backend == c.localServiceAddress {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("remote run pipeline in local: %s", backend))
	}
	return c.client.NewStream(backend, true)
}

func (c *pipelineClient) Raw() morpc.RPCClient {
	return c.client
}

func (c *pipelineClient) Close() error {
	return c.client.Close()
}
