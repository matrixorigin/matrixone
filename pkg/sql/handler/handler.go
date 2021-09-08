package handler

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/rpcserver/message"
	"matrixone/pkg/sql/colexec/output"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/process"

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
	s.Ins[len(s.Ins)-1] = vm.Instruction{
		Op: vm.Output,
		Arg: &output.Argument{
			Func: writeBack,
			Data: &userdata{conn: conn},
		},
	}
	if err := s.MergeRun(hp.engine); err != nil {
		conn.WriteAndFlush(&message.Message{Code: []byte(err.Error())})
	}
	return nil
}

func writeBack(u interface{}, bat *batch.Batch) error {
	var buf bytes.Buffer

	up := u.(*userdata)
	if bat == nil && up.conn != nil {
		defer func() { up.conn = nil }()
		return up.conn.WriteAndFlush(&message.Message{Sid: 1})
	}
	if err := protocol.EncodeBatch(bat, &buf); err != nil {
		return err
	}
	return up.conn.WriteAndFlush(&message.Message{Data: buf.Bytes()})
}
