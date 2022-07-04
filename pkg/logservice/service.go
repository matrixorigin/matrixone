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

package logservice

import (
	"context"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/lni/dragonboat/v4/logger"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	plog = logger.GetLogger("LogService")
)

const (
	LogServiceRPCName = "logservice-rpc"
)

type Lsn = uint64

type LogRecord = pb.LogRecord

// TODO: move this to a better place
func firstError(err1 error, err2 error) error {
	if err1 != nil {
		return err1
	}
	return err2
}

// RPCRequest is request message type used in morpc
type RPCRequest struct {
	pb.Request
	payload []byte
	pool    *sync.Pool
}

var _ morpc.PayloadMessage = (*RPCRequest)(nil)

func (r *RPCRequest) Release() {
	if r.pool != nil {
		r.payload = nil
		r.pool.Put(r)
	}
}

func (r *RPCRequest) SetID(id uint64) {
	r.RequestID = id
}

func (r *RPCRequest) GetID() uint64 {
	return r.RequestID
}

func (r *RPCRequest) DebugString() string {
	return ""
}

func (r *RPCRequest) GetPayloadField() []byte {
	return r.payload
}

func (r *RPCRequest) SetPayloadField(data []byte) {
	r.payload = data
}

// RPCResponse is response message type used in morpc
type RPCResponse struct {
	pb.Response
	payload []byte
	pool    *sync.Pool
}

var _ morpc.PayloadMessage = (*RPCResponse)(nil)

func (r *RPCResponse) Release() {
	if r.pool != nil {
		r.payload = nil
		r.pool.Put(r)
	}
}

func (r *RPCResponse) SetID(id uint64) {
	r.RequestID = id
}

func (r *RPCResponse) GetID() uint64 {
	return r.RequestID
}

func (r *RPCResponse) DebugString() string {
	return ""
}

func (r *RPCResponse) GetPayloadField() []byte {
	return r.payload
}

func (r *RPCResponse) SetPayloadField(data []byte) {
	r.payload = data
}

// Service is the top layer component of a log service node. It manages the
// underlying log store which in turn manages all log shards including the
// HAKeeper shard. The Log Service component communicates with LogService
// clients owned by DN nodes and the HAKeeper service via network, it can
// be considered as the interface layer of the LogService.
type Service struct {
	cfg      Config
	store    *store
	server   morpc.RPCServer
	pool     *sync.Pool
	respPool *sync.Pool
}

func NewService(cfg Config) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.Fill()
	plog.Infof("calling newLogStore")
	store, err := newLogStore(cfg)
	if err != nil {
		plog.Errorf("failed to create log store %v", err)
		return nil, err
	}
	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &RPCRequest{pool: pool}
	}
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}

	mf := func() morpc.Message {
		return pool.Get().(*RPCRequest)
	}
	codec := morpc.NewMessageCodec(mf, 16*1024)
	server, err := morpc.NewRPCServer(LogServiceRPCName, cfg.ServiceListenAddress, codec,
		morpc.WithServerGoettyOptions(goetty.WithReleaseMsgFunc(func(i interface{}) {
			respPool.Put(i)
		})))
	if err != nil {
		return nil, err
	}

	plog.Infof("store created")
	service := &Service{
		cfg:      cfg,
		store:    store,
		server:   server,
		pool:     pool,
		respPool: respPool,
	}
	server.RegisterRequestHandler(service.handleRPCRequest)

	// TODO: before making the service available to the outside world, restore all
	// replicas already known to the local store
	if err := server.Start(); err != nil {
		plog.Errorf("failed to start the server %v", err)
		if err := store.Close(); err != nil {
			plog.Errorf("failed to close the store, %v", err)
		}
		return nil, err
	}
	plog.Infof("server started")
	return service, nil
}

func (s *Service) Close() (err error) {
	err = firstError(err, s.server.Close())
	err = firstError(err, s.store.Close())
	return err
}

func (s *Service) ID() string {
	return s.store.ID()
}

func (s *Service) handleRPCRequest(req morpc.Message,
	seq uint64, cs morpc.ClientSession) error {
	rr, ok := req.(*RPCRequest)
	if !ok {
		panic("unexpected message type")
	}
	defer rr.Release()
	resp, records := s.handle(rr.Request, rr.GetPayloadField())
	var recs []byte
	if len(records.Records) > 0 {
		plog.Infof("total recs: %d", len(records.Records))
		recs = MustMarshal(&records)
	}
	resp.RequestID = rr.RequestID
	response := s.respPool.Get().(*RPCResponse)
	response.Response = resp
	response.payload = recs
	return cs.Write(response,
		morpc.SendOptions{Timeout: time.Duration(rr.Request.Timeout)})
}

func (s *Service) handle(req pb.Request,
	payload []byte) (pb.Response, pb.LogRecordResponse) {
	switch req.Method {
	case pb.CREATE:
		panic("not implemented")
	case pb.DESTROY:
		panic("not implemented")
	case pb.APPEND:
		return s.handleAppend(req, payload), pb.LogRecordResponse{}
	case pb.READ:
		return s.handleRead(req)
	case pb.TRUNCATE:
		return s.handleTruncate(req), pb.LogRecordResponse{}
	case pb.GET_TRUNCATE:
		return s.handleGetTruncatedIndex(req), pb.LogRecordResponse{}
	case pb.CONNECT:
		return s.handleConnect(req), pb.LogRecordResponse{}
	case pb.CONNECT_RO:
		return s.handleConnectRO(req), pb.LogRecordResponse{}
	case pb.LOG_HEARTBEAT:
		return s.handleLogHeartbeat(req), pb.LogRecordResponse{}
	case pb.DN_HEARTBEAT:
		return s.handleDNHeartbeat(req), pb.LogRecordResponse{}
	default:
		panic("unknown method type")
	}
}

func getResponse(req pb.Request) pb.Response {
	return pb.Response{Method: req.Method}
}

func (s *Service) handleConnect(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	if err := s.store.GetOrExtendDNLease(ctx, r.ShardID, r.DNID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleConnectRO(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	// we only check whether the specified shard is available
	if _, err := s.store.GetTruncatedIndex(ctx, r.ShardID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleAppend(req pb.Request, payload []byte) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	lsn, err := s.store.Append(ctx, r.ShardID, payload)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.Index = lsn
	}
	return resp
}

func (s *Service) handleRead(req pb.Request) (pb.Response, pb.LogRecordResponse) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	records, lsn, err := s.store.QueryLog(ctx, r.ShardID, r.Index, r.MaxSize)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.LastIndex = lsn
	}
	return resp, pb.LogRecordResponse{Records: records}
}

func (s *Service) handleTruncate(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	if err := s.store.TruncateLog(ctx, r.ShardID, r.Index); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleGetTruncatedIndex(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	index, err := s.store.GetTruncatedIndex(ctx, r.ShardID)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.Index = index
	}
	return resp
}

// TODO: add tests to see what happens when request is sent to non hakeeper stores
func (s *Service) handleLogHeartbeat(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	hb := req.LogHeartbeat
	resp := getResponse(req)
	if err := s.store.AddLogStoreHeartbeat(ctx, hb); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	if cb, err := s.store.GetCommandBatch(ctx, hb.UUID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = cb
	}

	return resp
}

func (s *Service) handleDNHeartbeat(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	hb := req.DNHeartbeat
	resp := getResponse(req)
	if err := s.store.AddDNStoreHeartbeat(ctx, hb); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}
	if cb, err := s.store.GetCommandBatch(ctx, hb.UUID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = cb
	}

	return resp
}
