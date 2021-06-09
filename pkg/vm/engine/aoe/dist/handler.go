package dist

import (
	"encoding/json"
	"github.com/fagongzi/goetty/codec"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

type cmdType uint64

const (
	Set cmdType = iota
	SetWithTTL
	Incr
	Del
	Get
	PrefixScan
	Scan
)

func (h *aoeStorage) init() {
	h.AddWriteFunc(uint64(Set), h.set)
	h.AddWriteFunc(uint64(Incr), h.incr)
	h.AddReadFunc(uint64(Get), h.get)

}

func (h *aoeStorage) BuildRequest(req *raftcmdpb.Request, i interface{}) error {

	op := i.(Args)

	if _, ok := h.cmds[op.Op]; !ok {
		return ErrCMDNotSupport
	}

	req.Key = op.Args[0]
	req.CustemType = op.Op
	req.Type = h.cmds[op.Op]
	cmd, err := json.Marshal(op)
	if err != nil {
		return err
	}
	req.Cmd = cmd
	return nil
}

func (h *aoeStorage) Codec() (codec.Encoder, codec.Decoder) {
	return nil, nil
}

// AddReadFunc add read handler func
func (h *aoeStorage) AddReadFunc(cmdType uint64, cb command.ReadCommandFunc) {
	h.cmds[cmdType] = raftcmdpb.CMDType_Read
	h.store.RegisterReadFunc(cmdType, cb)
}

// AddWriteFunc add write handler func
func (h *aoeStorage) AddWriteFunc(cmdType uint64, cb command.WriteCommandFunc) {
	h.cmds[cmdType] = raftcmdpb.CMDType_Write
	h.store.RegisterWriteFunc(cmdType, cb)
}
