// Copyright 2021 - 2024 Matrix Origin
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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

const (
	// TODO: need to find out why rpc timeout, or move heartbeat check to another way.
	defaultRPCTimeout = 120 * time.Second

	dfMaxSenderNumber       = 100000
	dfConnectTimeout        = 5 * time.Second
	dfClientReadBufferSize  = 1 << 10
	dfClientWriteBufferSize = 1 << 10
)

var (
	messagePool = sync.Pool{
		New: func() any {
			return &pipeline.Message{}
		},
	}
)

// PipelineConfig a config to init a CNClient
type PipelineConfig struct {
	// MaxSenderNumber is the max number of backends per host for compute node service.
	MaxSenderNumber int
	// TimeOutForEachConnect is the out time for each tcp connect.
	TimeOutForEachConnect time.Duration
	// related buffer size.
	ReadBufferSize  int
	WriteBufferSize int
	// RPC rpc config
	RPC rpc.Config
}

// PipelineClient pipeline client
type PipelineClient interface {
	NewStream(backend string) (morpc.Stream, error)
	Raw() morpc.RPCClient
	Close() error
}

func AcquireMessage() *pipeline.Message {
	return messagePool.Get().(*pipeline.Message)
}

func ReleaseMessage(msg *pipeline.Message) {
	msg.Reset()
	messagePool.Put(msg)
}
