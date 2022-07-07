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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/rpcserver/message"

	"github.com/fagongzi/goetty"
)

type hello struct {
	cmd int
}

func TestServer(t *testing.T) {
	srv, err := New("127.0.0.1:8080", 1<<30, logutil.GetGlobalLogger())
	if err != nil {
		t.Fatal(err)
	}
	h := new(hello)
	h.cmd = srv.Register(h.process)
	err = srv.Run()
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
}

func (h *hello) process(_ uint64, val interface{}, conn goetty.IOSession) error {
	for i := 0; i < 10; i++ {
		conn.WriteAndFlush(&message.Message{
			Data: val.(*message.Message).Data,
		})
	}
	return nil
}
