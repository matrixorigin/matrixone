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
	"fmt"
	"log"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/examples/message"
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

	bf := morpc.NewGoettyBasedBackendFactory(newCodec(), morpc.WithBackendConnectWhenCreate())
	cli, err := morpc.NewClient(bf, morpc.WithClientMaxBackendPerHost(1))
	if err != nil {
		panic(err)
	}

	st, err := cli.NewStream(addr)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := st.Close(); err != nil {
			panic(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	if err := st.Send(ctx, &message.ExampleMessage{MsgID: st.ID(), Content: "first message"}); err != nil {
		panic(err)
	}

	ch, err := st.Receive()
	if err != nil {
		panic(err)
	}

	for m := range ch {
		if m == nil {
			return
		}
		log.Printf("%s", m.DebugString())
	}
}

func startServer() error {
	s, err := morpc.NewRPCServer("test-unix-server", addr, newCodec())
	if err != nil {
		return err
	}
	s.RegisterRequestHandler(func(ctx context.Context, request morpc.Message, _ uint64, cs morpc.ClientSession) error {
		// send more message back
		go func() {
			for i := 0; i < 10; i++ {
				if err := cs.Write(ctx, &message.ExampleMessage{MsgID: request.GetID(), Content: fmt.Sprintf("stream-%d", i)}); err != nil {
					panic(err)
				}
			}
		}()
		return nil
	})

	return s.Start()
}

func newCodec() morpc.Codec {
	return morpc.NewMessageCodec(func() morpc.Message { return &message.ExampleMessage{} })
}
