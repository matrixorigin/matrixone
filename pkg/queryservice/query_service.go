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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/pkg/errors"
)

// QueryService is used to send query request to another CN service.
type QueryService interface {
	// ServiceID return the uuid of current CN service
	ServiceID() string
	// Start starts the service.
	Start() error
	// Close closes the service.
	Close() error
	// AddHandleFunc add message handler.
	AddHandleFunc(method pb.CmdMethod, h func(context.Context, *pb.Request, *pb.Response) error, async bool)
}

// queryService is a query service started in CN service.
type queryService struct {
	// serviceID is the UUID of CN service.
	serviceID string
	handler   morpc.MessageHandler[*pb.Request, *pb.Response]
}

// NewQueryService creates a new queryService instance.
func NewQueryService(serviceID string, address string, cfg morpc.Config) (QueryService, error) {
	serviceName := "query-service"

	pool := morpc.NewMessagePool(
		func() *pb.Request { return &pb.Request{} },
		func() *pb.Response { return &pb.Response{} })

	h, err := morpc.NewMessageHandler(serviceName, address, cfg, pool)
	if err != nil {
		return nil, err
	}
	qs := &queryService{
		serviceID: serviceID,
		handler:   h,
	}
	qs.initHandleFunc()
	return qs, nil
}

// AddHandleFunc implements the QueryService interface.
func (s *queryService) AddHandleFunc(method pb.CmdMethod, h func(context.Context, *pb.Request, *pb.Response) error, async bool) {
	s.handler.RegisterHandleFunc(uint32(method), h, async)
}

func (s *queryService) initHandleFunc() {
	s.AddHandleFunc(pb.CmdMethod_GetProtocolVersion, handleGetProtocolVersion, false)
	s.AddHandleFunc(pb.CmdMethod_SetProtocolVersion, handleSetProtocolVersion, false)
	s.AddHandleFunc(pb.CmdMethod_CoreDumpConfig, handleCoreDumpConfig, false)
}

// Start implements the QueryService interface.
func (s *queryService) Start() error {
	return s.handler.Start()
}

// Close implements the QueryService interface.
func (s *queryService) Close() error {
	return s.handler.Close()
}

func (s *queryService) ServiceID() string {
	return s.serviceID
}

type nodeResponse struct {
	nodeAddr string      //address of cn
	response interface{} //response to the request
	err      error
}

// RequestMultipleCn sends the request to multiple cn and wait the responses.
// nodes : the address of the multiple cn
// qs : QueryService
// genRequest : generate the specific Request based on the business
// handleValidResponse : valid response handler
// handleInvalidResponse : invalid response handler
func RequestMultipleCn(ctx context.Context,
	nodes []string,
	qc client.QueryClient,
	genRequest func() *pb.Request,
	handleValidResponse func(string, *pb.Response),
	handleInvalidResponse func(string),
) error {
	if genRequest == nil {
		return moerr.NewInternalError(ctx, "invalid request generate function")
	}
	if handleValidResponse == nil {
		return moerr.NewInternalError(ctx, "invalid response handle function")
	}
	nodesLeft := len(nodes)
	responseChan := make(chan nodeResponse, nodesLeft)

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	var retErr error

	for _, node := range nodes {
		// Invalid node address, ignore it.
		if len(node) == 0 {
			nodesLeft--
			continue
		}

		go func(addr string) {
			// gen request and send it
			if genRequest != nil {
				req := genRequest()
				resp, err := qc.SendMessage(ctx, addr, req)
				responseChan <- nodeResponse{nodeAddr: addr, response: resp, err: err}
			}
		}(node)
	}

	// Wait for all responses.
	for nodesLeft > 0 {
		select {
		case res := <-responseChan:
			if res.err != nil && retErr != nil {
				retErr = errors.Wrapf(res.err, "failed to get result from %s", res.nodeAddr)
			} else {
				queryResp, ok := res.response.(*pb.Response)
				if ok {
					//save response
					if handleValidResponse != nil {
						handleValidResponse(res.nodeAddr, queryResp)
					}
					if queryResp != nil {
						qc.Release(queryResp)
					}
				} else {
					if handleInvalidResponse != nil {
						handleInvalidResponse(res.nodeAddr)
					}
				}
			}
		case <-ctx.Done():
			retErr = moerr.NewInternalError(ctx, "context deadline exceeded")
		}
		nodesLeft--
	}
	return retErr
}
