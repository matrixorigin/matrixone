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
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/goutils/netutil"
	"github.com/lni/goutils/syncutil"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	plog = logger.GetLogger("LogService")
)

type Lsn = uint64

type LogRecord = pb.LogRecord

// Service is the top layer component of a log service node. It manages the
// underlying log store which in turn manages all log shards including the
// HAKeeper shard. The Log Service component communicates with LogService
// clients owned by DN nodes and the HAKeeper service via network, it can
// be considered as the interface layer of the LogService.
type Service struct {
	cfg         Config
	store       *store
	stopper     *syncutil.Stopper
	connStopper *syncutil.Stopper
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
	plog.Infof("store created")
	service := &Service{
		cfg:         cfg,
		store:       store,
		stopper:     syncutil.NewStopper(),
		connStopper: syncutil.NewStopper(),
	}
	// TODO: before making the service available to the outside world, restore all
	// replicas already known to the local store
	if err := service.startServer(); err != nil {
		plog.Errorf("failed to start the server %v", err)
		if err := store.Close(); err != nil {
			plog.Errorf("failed to close the store, %v", err)
		}
		return nil, err
	}
	plog.Infof("server started")
	return service, nil
}

func (s *Service) Close() error {
	s.stopper.Stop()
	s.connStopper.Stop()
	return s.store.Close()
}

func (s *Service) ID() string {
	return s.store.ID()
}

func (s *Service) startServer() error {
	listener, err := netutil.NewStoppableListener(s.cfg.ServiceListenAddress,
		nil, s.stopper.ShouldStop())
	if err != nil {
		return err
	}
	s.connStopper.RunWorker(func() {
		// sync.WaitGroup's doc mentions that
		// "Note that calls with a positive delta that occur when the counter is
		//  zero must happen before a Wait."
		// It is unclear that whether the stdlib is going complain in future
		// releases when Wait() is called when the counter is zero and Add() with
		// positive delta has never been called.
		<-s.connStopper.ShouldStop()
	})
	s.stopper.RunWorker(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if err == netutil.ErrListenerStopped {
					return
				}
				panic(err)
			}
			var once sync.Once
			closeFn := func() {
				once.Do(func() {
					if err := conn.Close(); err != nil {
						plog.Errorf("failed to close the connection, %v", err)
					}
				})
			}
			s.connStopper.RunWorker(func() {
				<-s.stopper.ShouldStop()
				closeFn()
			})
			s.connStopper.RunWorker(func() {
				s.serve(conn)
				closeFn()
			})
		}
	})
	return nil
}

func (s *Service) serve(conn net.Conn) {
	magicNum := make([]byte, len(magicNumber))
	reqBuf := make([]byte, reqBufSize)
	recvBuf := make([]byte, recvBufSize)

	for {
		err := readMagicNumber(conn, magicNum)
		if err != nil {
			if errors.Is(err, errPoisonReceived) {
				if err := sendPoisonAck(conn, poisonNumber[:]); err != nil {
					plog.Errorf("failed to send poison ack, %v", err)
				}
				return
			}
			if errors.Is(err, errBadMessage) {
				return
			}
			operr, ok := err.(net.Error)
			if ok && operr.Timeout() {
				continue
			} else {
				return
			}
		}
		req, payload, err := readRequest(conn, reqBuf, recvBuf)
		if err != nil {
			plog.Errorf("failed to read request, %v", err)
			return
		}
		// with error already encoded into the resp
		resp, records := s.handle(req, payload)
		var recs []byte
		if len(records.Records) > 0 {
			data := MustMarshal(&records)
			resp.PayloadSize = uint64(len(data))
			recs = data
		}
		if err := writeResponse(conn, resp, recs, recvBuf); err != nil {
			plog.Errorf("failed to write response, %v", err)
			return
		}
	}
}

func (s *Service) handle(req pb.Request,
	payload []byte) (pb.Response, pb.LogRecordResponse) {
	switch req.Method {
	case pb.MethodType_CREATE:
		panic("not implemented")
	case pb.MethodType_DESTROY:
		panic("not implemented")
	case pb.MethodType_APPEND:
		return s.handleAppend(req, payload), pb.LogRecordResponse{}
	case pb.MethodType_READ:
		return s.handleRead(req)
	case pb.MethodType_TRUNCATE:
		return s.handleTruncate(req), pb.LogRecordResponse{}
	case pb.MethodType_GET_TRUNCATE:
		return s.handleGetTruncatedIndex(req), pb.LogRecordResponse{}
	case pb.MethodType_CONNECT:
		return s.handleConnect(req), pb.LogRecordResponse{}
	case pb.MethodType_CONNECT_RO:
		return s.handleConnectRO(req), pb.LogRecordResponse{}
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
	resp := getResponse(req)
	if err := s.store.GetOrExtendDNLease(ctx, req.ShardID, req.DNID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleConnectRO(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	resp := getResponse(req)
	// we only check whether the specified shard is available
	if _, err := s.store.GetTruncatedIndex(ctx, req.ShardID); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleAppend(req pb.Request, payload []byte) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	resp := getResponse(req)
	lsn, err := s.store.Append(ctx, req.ShardID, payload)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.Index = lsn
	}
	return resp
}

func (s *Service) handleRead(req pb.Request) (pb.Response, pb.LogRecordResponse) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	resp := getResponse(req)
	records, lsn, err := s.store.QueryLog(ctx, req.ShardID, req.Index, req.MaxSize)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.LastIndex = lsn
	}
	return resp, pb.LogRecordResponse{Records: records}
}

func (s *Service) handleTruncate(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	resp := getResponse(req)
	if err := s.store.TruncateLog(ctx, req.ShardID, req.Index); err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	}
	return resp
}

func (s *Service) handleGetTruncatedIndex(req pb.Request) pb.Response {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout))
	defer cancel()
	resp := getResponse(req)
	index, err := s.store.GetTruncatedIndex(ctx, req.ShardID)
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = toErrorCode(err)
	} else {
		resp.Index = index
	}
	return resp
}
