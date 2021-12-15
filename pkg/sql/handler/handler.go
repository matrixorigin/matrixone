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

	"github.com/fagongzi/goetty"
)

func New(engine engine.Engine, proc *process.Process) *Handler {
	return &Handler{
		engine: engine,
		proc:   proc,
	}
}

func (hp *Handler) Process(_ uint64, val interface{}, conn goetty.IOSession) error {
	ps, _, err := protocol.DecodeScope(val.(*message.Message).Data)
	if err != nil {
		return err
	}
	s := recoverScope(ps, hp.proc)
	s.Instructions[len(s.Instructions)-1] = vm.Instruction{
		Code: vm.Output,
		Arg: &output.Argument{
			Data: conn,
			Func: writeBack,
		},
	}
	if err := s.MergeRun(hp.engine); err != nil {
		conn.WriteAndFlush(&message.Message{Code: []byte(err.Error())})
	}
	return conn.WriteAndFlush(&message.Message{Sid: 1})
}

func writeBack(u interface{}, bat *batch.Batch) error {
	var buf bytes.Buffer

	conn := u.(goetty.IOSession)
	if bat == nil {
		return nil
	}
	if err := protocol.EncodeBatch(bat, &buf); err != nil {
		return err
	}
	return conn.WriteAndFlush(&message.Message{Data: buf.Bytes()})
}
