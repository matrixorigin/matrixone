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

package main

import (
	"context"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/examples/message"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	addr = "unix:///tmp/pingpong.sock"
	file = "/tmp/pingpong.sock"
)

func main() {
	if err := os.RemoveAll(file); err != nil {
		panic(err)
	}

	if err := startServer(); err != nil {
		panic(err)
	}

	bf := morpc.NewGoettyBasedBackendFactory(newCodec())
	cli, err := morpc.NewClient("example-rpc", bf, morpc.WithClientMaxBackendPerHost(1))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f, err := cli.Send(ctx, addr, &message.ExampleMessage{MsgID: 1, Content: "hello"}, morpc.SyncWrite)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	resp, err := f.Get()
	if err != nil {
		panic(err)
	}

	logutil.Infof("%s", resp.DebugString())
}

func startServer() error {
	s, err := morpc.NewRPCServer("test-unix-server", addr, newCodec())
	if err != nil {
		return err
	}
	s.RegisterRequestHandler(func(ctx context.Context, request morpc.RPCMessage, sequence uint64, cs morpc.ClientSession) error {
		// write request back to client
		return cs.Write(ctx, request.Message, morpc.SyncWrite)
	})

	return s.Start()
}

func newCodec() morpc.Codec {
	return morpc.NewMessageCodec(func() morpc.Message { return &message.ExampleMessage{} })
}
