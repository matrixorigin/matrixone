package catalogkv

import (
	"encoding/json"
	"github.com/fagongzi/goetty/codec"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"strconv"
)

var (
	statusResp = []byte("OK")
)

type handler struct {
	store raftstore.Store
	types map[uint64]raftcmdpb.CMDType
}

// NewHandler returns a pebble server handler
func NewHandler(store raftstore.Store) server.Handler {
	h := &handler{
		store: store,
		types: make(map[uint64]raftcmdpb.CMDType),
	}
	return h
}

func (h *handler) initSupportCMDs() {
	h.AddWriteFunc(set, h.set)
	h.AddReadFunc(get, h.get)
}

func (h *handler) BuildRequest(req *raftcmdpb.Request, msg interface{}) error {
	op := msg.(*request)
	cmdType, err := strconv.ParseUint(op.Op, 10, 64)
	if _, ok := h.types[cmdType]; !ok || err != nil {
		return ErrCMDNotSupport
	}
	req.Key = []byte(op.Key)
	req.CustemType = cmdType
	req.Type = h.types[cmdType]
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	req.Cmd = data
	return nil
}

func (h *handler) Codec() (codec.Encoder, codec.Decoder) {
	return nil, nil
}

// AddReadFunc add read handler func
func (h *handler) AddReadFunc(cmdType uint64, cb command.ReadCommandFunc) {
	h.types[cmdType] = raftcmdpb.CMDType_Read
	h.store.RegisterReadFunc(cmdType, cb)
}

// AddWriteFunc add write handler func
func (h *handler) AddWriteFunc(cmdType uint64, cb command.WriteCommandFunc) {
	h.types[cmdType] = raftcmdpb.CMDType_Write
	h.store.RegisterWriteFunc(cmdType, cb)
}

func (h *handler) set(shard bhmetapb.Shard, req *raftcmdpb.Request, c command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()
	cmd := request{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	err = h.getPebbleByGroup(shard.Group).Set(req.Key, []byte(cmd.Value))
	if err != nil {
		resp.Value = errorResp(err)
		return 0, 0, resp
	}
	writtenBytes := uint64(len(req.Key) + len(cmd.Value))
	changedBytes := int64(writtenBytes)
	resp.Value = statusResp
	return writtenBytes, changedBytes, resp
}

func (h *handler) get(shard bhmetapb.Shard, req *raftcmdpb.Request, c command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()
	cmd := request{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	value, err := h.getPebbleByGroup(shard.Group).Get(req.Key)
	if err != nil {
		resp.Value = errorResp(err)
		return resp, 500
	}
	resp.Value = value
	return resp, 0
}

func (h *handler) getPebbleByGroup(group uint64) *pebble.Storage {
	return h.store.DataStorageByGroup(group, 500).(*pebble.Storage)
}

func errorResp(err error) []byte {
	return []byte(err.Error())
}

type request struct {
	Op    string `json:"json:op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}
