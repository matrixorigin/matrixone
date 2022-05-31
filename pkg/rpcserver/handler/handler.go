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

package handler

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/rpcserver/message"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(engine engine.Engine, proc *process.Process) *Handler {
	return &Handler{
		engine: engine,
		proc:   proc,
	}
}

func (hp *Handler) Process(req *message.Message, stream message.RPCHandler_ProcessServer) (err error) {
	ps, _, err := protocol.DecodeScope(req.Data)
	if err != nil {
		return err
	}
	s := recoverScope(ps, hp.proc)
	s.Instructions[len(s.Instructions)-1] = vm.Instruction{
		Op: vm.Output,
		Arg: &output.Argument{
			Data: stream,
			Func: writeBack,
		},
	}
	if err := s.ParallelRun(hp.engine); err != nil {
		stream.Send(&message.Message{Code: []byte(err.Error())})
	}
	return stream.Send(&message.Message{Sid: 1})
}

func writeBack(u interface{}, bat *batch.Batch) error {
	var buf bytes.Buffer

	stream := u.(message.RPCHandler_ProcessServer)
	if bat == nil || len(bat.Zs) == 0 {
		return nil
	}
	if err := protocol.EncodeBatch(bat, &buf); err != nil {
		return err
	}
	return stream.Send(&message.Message{Data: buf.Bytes()})
}
