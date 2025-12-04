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
	"sync"

	"github.com/lni/dragonboat/v4/logger"
	"github.com/pkg/errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
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
	AddHandleFunc(method pb.CmdMethod, h func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error, async bool)
	// SetReleaseFunc sets the release handler.
	SetReleaseFunc(resp *pb.Response, f func())
}

// queryService is a query service started in CN service.
type queryService struct {
	// serviceID is the UUID of CN service.
	serviceID string
	handler   morpc.MethodBasedServer[*pb.Request, *pb.Response]

	mu struct {
		sync.Mutex
		releaser map[*pb.Response]func()
	}
}

// NewQueryService creates a new queryService instance.
func NewQueryService(serviceID string, address string, cfg morpc.Config) (QueryService, error) {
	serviceName := "query-service"
	qs := &queryService{
		serviceID: serviceID,
	}

	qs.mu.releaser = make(map[*pb.Response]func())

	pool := morpc.NewMessagePool(
		func() *pb.Request { return &pb.Request{} },
		func() *pb.Response { return &pb.Response{} })

	h, err := morpc.NewMessageHandler(serviceID, serviceName, address, cfg, pool,
		morpc.WithHandlerRespReleaseFunc[*pb.Request, *pb.Response](func(m morpc.Message) {
			resp := m.(*pb.Response)
			if resp.CmdMethod == pb.CmdMethod_GetCacheData {
				qs.mu.Lock()
				defer qs.mu.Unlock()
				release, ok := qs.mu.releaser[resp]
				if ok {
					release()
					delete(qs.mu.releaser, resp)
				}
			}
			pool.ReleaseResponse(resp)
		}),
	)
	if err != nil {
		return nil, err
	}
	qs.handler = h
	qs.initHandleFunc()
	return qs, nil
}

// AddHandleFunc implements the QueryService interface.
func (s *queryService) AddHandleFunc(method pb.CmdMethod, h func(context.Context, *pb.Request, *pb.Response, *morpc.Buffer) error, async bool) {
	s.handler.RegisterMethod(uint32(method), h, async)
}

// SetReleaseFunc implements the QueryService interface.
func (s *queryService) SetReleaseFunc(resp *pb.Response, f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.releaser[resp] = f
}

func (s *queryService) initHandleFunc() {
	s.AddHandleFunc(pb.CmdMethod_GetProtocolVersion, s.handleGetProtocolVersion(), false)
	s.AddHandleFunc(pb.CmdMethod_SetProtocolVersion, s.handleSetProtocolVersion(), false)
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

	// If the context is already canceled, return immediately
	if err := ctx.Err(); err != nil {
		return err
	}

	// Count valid nodes (non-empty addresses)
	validNodes := 0
	for _, node := range nodes {
		if len(node) > 0 {
			validNodes++
		}
	}

	responseChan := make(chan nodeResponse, validNodes)

	var retErr error
	var successCount int
	var failedNodes []string

	// Track how many goroutines were actually started
	var wg sync.WaitGroup

	for _, node := range nodes {
		// Invalid node address, ignore it.
		if len(node) == 0 {
			continue
		}

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			// gen request and send it
			// genRequest is guaranteed to be non-nil (checked at function entry)
			req := genRequest()
			logger.GetLogger("RequestMultipleCn").Infof("[send request]%s send request %s to %s", qc.ServiceID(), req.CmdMethod.String(), addr)
			resp, err := qc.SendMessage(ctx, addr, req)
			responseChan <- nodeResponse{nodeAddr: addr, response: resp, err: err}
		}(node)
	}

	// Wait for all responses.
	// Important: Always drain all responses to avoid goroutine leaks,
	// even when context is canceled (goroutines may still write to channel)
	responsesReceived := 0
	for responsesReceived < validNodes {
		select {
		case res := <-responseChan:
			responsesReceived++
			if res.err != nil {
				// Check if the error itself is a context timeout error
				// or if the context has already timed out (handles race condition)
				// If context has timed out, prioritize timeout error over connection error
				if ctx.Err() == context.DeadlineExceeded || errors.Is(res.err, context.DeadlineExceeded) {
					// Context has timed out, prioritize timeout error
					if retErr == nil {
						retErr = moerr.NewInternalError(ctx, "RequestMultipleCn : context deadline exceeded")
					}
				} else {
					// Context has not timed out, record connection error
					// Note: if context times out later, timeout error will override connection error
					if retErr == nil {
						retErr = errors.Wrapf(res.err, "failed to get result from %s", res.nodeAddr)
					}
				}
				failedNodes = append(failedNodes, res.nodeAddr)
				// Notify caller about invalid response (network error, etc.)
				if handleInvalidResponse != nil {
					func() {
						defer func() {
							if r := recover(); r != nil {
								logger.GetLogger("RequestMultipleCn").Errorf(
									"[handler panic] %s handleInvalidResponse panicked for %s: %v",
									qc.ServiceID(), res.nodeAddr, r,
								)
							}
						}()
						handleInvalidResponse(res.nodeAddr)
					}()
				}
				continue
			}

			// Only process response when successful
			queryResp, ok := res.response.(*pb.Response)
			if ok {
				//save response
				// Protect against panic in user-provided handler
				var handlerPanicked bool
				func() {
					defer func() {
						if r := recover(); r != nil {
							handlerPanicked = true
							logger.GetLogger("RequestMultipleCn").Errorf(
								"[handler panic] %s handleValidResponse panicked for %s: %v",
								qc.ServiceID(), res.nodeAddr, r,
							)
						}
					}()
					handleValidResponse(res.nodeAddr, queryResp)
				}()
				if queryResp != nil {
					qc.Release(queryResp)
				}

				// If handler panicked, treat as failure
				if handlerPanicked {
					if retErr == nil {
						retErr = moerr.NewInternalErrorf(ctx, "handleValidResponse panicked for %s", res.nodeAddr)
					}
					failedNodes = append(failedNodes, res.nodeAddr)
					// Notify caller about invalid response (handler panic)
					if handleInvalidResponse != nil {
						func() {
							defer func() {
								if r := recover(); r != nil {
									logger.GetLogger("RequestMultipleCn").Errorf(
										"[handler panic] %s handleInvalidResponse panicked for %s: %v",
										qc.ServiceID(), res.nodeAddr, r,
									)
								}
							}()
							handleInvalidResponse(res.nodeAddr)
						}()
					}
				} else {
					successCount++
				}
			} else {
				// Response type assertion failed - this is an error condition
				if retErr == nil {
					retErr = moerr.NewInternalErrorf(ctx, "invalid response type from %s", res.nodeAddr)
				}
				failedNodes = append(failedNodes, res.nodeAddr)

				if handleInvalidResponse != nil {
					// Protect against panic in user-provided handler
					func() {
						defer func() {
							if r := recover(); r != nil {
								logger.GetLogger("RequestMultipleCn").Errorf(
									"[handler panic] %s handleInvalidResponse panicked for %s: %v",
									qc.ServiceID(), res.nodeAddr, r,
								)
							}
						}()
						handleInvalidResponse(res.nodeAddr)
					}()
				}
			}
		case <-ctx.Done():
			// Context timeout: prioritize timeout error, override previous connection error
			// Timeout is a higher-level error and should take precedence over connection errors
			if retErr != nil {
				// Log the overridden error for debugging purposes
				logger.GetLogger("RequestMultipleCn").Infof(
					"[timeout override] %s context timeout overrides previous error: %v",
					qc.ServiceID(), retErr,
				)
			}
			retErr = moerr.NewInternalError(ctx, "RequestMultipleCn : context deadline exceeded")
			// Don't add "context timeout" to failedNodes - keep it as real node addresses only
			// Timeout info is already in retErr
			// Continue receiving remaining responses to avoid goroutine leaks
			// Don't break - continue the loop to drain channel
		}
	}

	// Ensure all goroutines complete to avoid leaks
	// Even if context is canceled, goroutines may still be executing SendMessage
	// wg.Wait ensures we wait for all goroutines to finish writing to channel
	// Channel has buffer capacity = validNodes, so all goroutines can write without blocking
	//
	// After wg.Wait() returns:
	// - All goroutines have completed (wg.Done() called)
	// - All responses have been written to channel (happens before wg.Done())
	// - Main loop has received exactly validNodes responses (responsesReceived == validNodes)
	// - Therefore, channel must be empty, no need to drain
	wg.Wait()

	// Log error summary if any node failed
	// This provides aggregated view without repeating individual node errors
	// which are already logged by lower-level morpc layer
	if retErr != nil {
		logger.GetLogger("RequestMultipleCn").Errorf(
			"[request failed] %s distributed request to %d nodes: %d succeeded, %d failed, failed nodes: %v, error: %v",
			qc.ServiceID(),
			len(nodes),
			successCount,
			len(failedNodes),
			failedNodes,
			retErr,
		)
	}

	return retErr
}
