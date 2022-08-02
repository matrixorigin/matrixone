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

/*
Package logservice implement MO's LogService component.
*/
package logservice

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/lni/dragonboat/v4/logger"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
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
	stopper  *stopper.Stopper
	haClient LogHAKeeperClient
}

func NewService(cfg Config) (*Service, error) {
	cfg.Fill()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	store, err := newLogStore(cfg)
	if err != nil {
		plog.Errorf("failed to create log store %v", err)
		return nil, err
	}
	if err := store.loadMetadata(); err != nil {
		return nil, err
	}
	if err := store.startReplicas(); err != nil {
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
	// TODO: check and fix all these magic numbers
	codec := morpc.NewMessageCodec(mf, 16*1024)
	server, err := morpc.NewRPCServer(LogServiceRPCName, cfg.ServiceListenAddress, codec,
		morpc.WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(i interface{}) {
			respPool.Put(i)
		})))
	if err != nil {
		return nil, err
	}
	service := &Service{
		cfg:      cfg,
		store:    store,
		server:   server,
		pool:     pool,
		respPool: respPool,
		stopper:  stopper.NewStopper("log-service"),
	}
	server.RegisterRequestHandler(service.handleRPCRequest)
	// TODO: before making the service available to the outside world, restore all
	// replicas already known to the local store
	if err := server.Start(); err != nil {
		plog.Errorf("failed to start the server %v", err)
		if err := store.close(); err != nil {
			plog.Errorf("failed to close the store, %v", err)
		}
		return nil, err
	}
	// start the heartbeat worker
	if !cfg.DisableWorkers {
		if err := service.stopper.RunNamedTask("log-heartbeat-worker", func(ctx context.Context) {
			plog.Infof("logservice heartbeat worker started")
			service.heartbeatWorker(ctx)
		}); err != nil {
			return nil, err
		}
	}
	return service, nil
}

func (s *Service) Start() error {
	return nil
}

func (s *Service) Close() (err error) {
	s.stopper.Stop()
	if s.haClient != nil {
		err = firstError(err, s.haClient.Close())
	}
	err = firstError(err, s.server.Close())
	if s.store != nil {
		err = firstError(err, s.store.close())
	}
	return err
}

func (s *Service) ID() string {
	return s.store.id()
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
	case pb.TSO_UPDATE:
		return s.handleTsoUpdate(req), pb.LogRecordResponse{}
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
	case pb.CN_HEARTBEAT:
		return s.handleCNHeartbeat(req), pb.LogRecordResponse{}
	case pb.DN_HEARTBEAT:
		return s.handleDNHeartbeat(req), pb.LogRecordResponse{}
	case pb.CHECK_HAKEEPER:
		return s.handleCheckHAKeeper(req), pb.LogRecordResponse{}
	case pb.GET_CLUSTER_DETAILS:
		return s.handleGetClusterDetails(req), pb.LogRecordResponse{}
	default:
		panic("unknown log service method type")
	}
}

func getResponse(req pb.Request) pb.Response {
	return pb.Response{Method: req.Method}
}

func (s *Service) handleGetClusterDetails(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	resp := getResponse(req)
	if v, err := s.store.getClusterDetails(ctx); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.ClusterDetails = v
	}
	return resp
}

func (s *Service) handleTsoUpdate(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.TsoRequest
	resp := getResponse(req)
	if v, err := s.store.tsoUpdate(ctx, r.Count); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.TsoResponse.Value = v
	}
	return resp
}

func (s *Service) handleConnect(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	if err := s.store.getOrExtendDNLease(ctx, r.ShardID, r.DNID); err != nil {
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
	if _, err := s.store.getTruncatedLsn(ctx, r.ShardID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleAppend(req pb.Request, payload []byte) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	lsn, err := s.store.append(ctx, r.ShardID, payload)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.Lsn = lsn
	}
	return resp
}

func (s *Service) handleRead(req pb.Request) (pb.Response, pb.LogRecordResponse) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	records, lsn, err := s.store.queryLog(ctx, r.ShardID, r.Lsn, r.MaxSize)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.LastLsn = lsn
	}
	return resp, pb.LogRecordResponse{Records: records}
}

func (s *Service) handleTruncate(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	if err := s.store.truncateLog(ctx, r.ShardID, r.Lsn); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleGetTruncatedIndex(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	r := req.LogRequest
	resp := getResponse(req)
	lsn, err := s.store.getTruncatedLsn(ctx, r.ShardID)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LogResponse.Lsn = lsn
	}
	return resp
}

// TODO: add tests to see what happens when request is sent to non hakeeper stores
func (s *Service) handleLogHeartbeat(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	hb := req.LogHeartbeat
	resp := getResponse(req)
	if cb, err := s.store.addLogStoreHeartbeat(ctx, hb); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = cb
	}

	return resp
}

func (s *Service) handleCNHeartbeat(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	hb := req.CNHeartbeat
	resp := getResponse(req)
	if err := s.store.addCNStoreHeartbeat(ctx, hb); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	}

	return resp
}

func (s *Service) handleDNHeartbeat(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	hb := req.DNHeartbeat
	resp := getResponse(req)
	if cb, err := s.store.addDNStoreHeartbeat(ctx, hb); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
		return resp
	} else {
		resp.CommandBatch = cb
	}

	return resp
}

func (s *Service) handleCheckHAKeeper(req pb.Request) pb.Response {
	resp := getResponse(req)
	if atomic.LoadUint64(&s.store.haKeeperReplicaID) != 0 {
		resp.IsHAKeeper = true
	}
	return resp
}
