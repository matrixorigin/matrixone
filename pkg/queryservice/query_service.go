// Copyright 2021 - 2023 Matrix Origin
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

package queryservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
)

// QueryService is used to send query request to another CN service.
type QueryService interface {
	// SendMessage send message to a query service.
	SendMessage(ctx context.Context, address string, req *pb.Request) (*pb.Response, error)
	// NewRequest creates a new request by cmd method.
	NewRequest(pb.CmdMethod) *pb.Request
	// Release releases the response.
	Release(*pb.Response)
	// Start starts the service.
	Start() error
	// Close closes the service.
	Close() error
}

// queryService is a query service started in CN service.
type queryService struct {
	// serviceID is the UUID of CN service.
	serviceID string
	log       *log.MOLogger
	cluster   clusterservice.MOCluster
	client    morpc.RPCClient
	handler   morpc.MessageHandler[*pb.Request, *pb.Response]
	pool      morpc.MessagePool[*pb.Request, *pb.Response]

	// sessionMgr manages the sessions, we can get sessions from it.
	sessionMgr *SessionManager
}

// NewQueryService creates a new queryService instance.
func NewQueryService(serviceID string, address string, cfg morpc.Config, sm *SessionManager) (QueryService, error) {
	serviceName := "query-service"
	rt := runtime.ProcessLevelRuntime()
	if rt == nil {
		rt = runtime.DefaultRuntime()
	}
	logger := rt.Logger().Named(serviceName)

	pool := morpc.NewMessagePool(
		func() *pb.Request { return &pb.Request{} },
		func() *pb.Response { return &pb.Response{} })

	client, err := cfg.NewClient(serviceName,
		logger.RawLogger(),
		func() morpc.Message { return pool.AcquireResponse() })
	if err != nil {
		return nil, err
	}

	h, err := morpc.NewMessageHandler(serviceName, address, cfg, pool)
	if err != nil {
		return nil, err
	}
	qs := &queryService{
		serviceID:  serviceID,
		log:        logger,
		cluster:    clusterservice.GetMOCluster(),
		client:     client,
		handler:    h,
		pool:       pool,
		sessionMgr: sm,
	}
	qs.registerHandlers()
	return qs, nil
}

func (s *queryService) registerHandlers() {
	s.handler.RegisterHandleFunc(uint32(pb.CmdMethod_ShowProcessList),
		s.handleRequest, false)
}

// SendMessage implements the QueryService interface.
func (s *queryService) SendMessage(
	ctx context.Context, address string, req *pb.Request,
) (*pb.Response, error) {
	if address == "" {
		return nil, moerr.NewInternalError(ctx, "invalid CN query address %s", address)
	}
	f, err := s.client.Send(ctx, address, req)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	v, err := f.Get()
	if err != nil {
		return nil, err
	}
	resp := v.(*pb.Response)
	return s.unwrapResponseError(resp)
}

// NewRequest implements the QueryService interface.
func (s *queryService) NewRequest(method pb.CmdMethod) *pb.Request {
	req := s.pool.AcquireRequest()
	req.CmdMethod = method
	return req
}

// Release implements the QueryService interface.
func (s *queryService) Release(resp *pb.Response) {
	s.pool.ReleaseResponse(resp)
}

// Start implements the QueryService interface.
func (s *queryService) Start() error {
	return s.handler.Start()
}

// Close implements the QueryService interface.
func (s *queryService) Close() error {
	if err := s.client.Close(); err != nil {
		return err
	}
	return s.handler.Close()
}

func (s *queryService) unwrapResponseError(resp *pb.Response) (*pb.Response, error) {
	if err := resp.UnwrapError(); err != nil {
		s.pool.ReleaseResponse(resp)
		return nil, err
	}
	return resp, nil
}
