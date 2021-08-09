package handler

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/rpcserver/message"
	"matrixone/pkg/sql/colexec/output"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/laoe"
	"matrixone/pkg/vm/process"

	"github.com/fagongzi/goetty"
)

func New(db *db.DB, proc *process.Process) *Handler {
	return &Handler{
		db:   db,
		proc: proc,
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
			Data: conn,
			Func: writeBack,
		},
	}
	e := laoe.New(hp.db, s.Segments(make(map[string]map[string][]engine.SegmentInfo)))
	if err := s.MergeRun(e); err != nil {
		conn.WriteAndFlush(&message.Message{Code: []byte(err.Error())})
	}
	return nil
}

func writeBack(u interface{}, bat *batch.Batch) error {
	var buf bytes.Buffer

	conn := u.(goetty.IOSession)
	if bat == nil {
		return conn.WriteAndFlush(&message.Message{Sid: 1})
	}
	if err := protocol.EncodeBatch(bat, &buf); err != nil {
		return err
	}
	return conn.WriteAndFlush(&message.Message{Data: buf.Bytes()})
}
