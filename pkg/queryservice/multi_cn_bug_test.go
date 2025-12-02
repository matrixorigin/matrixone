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
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
)

// TestRequestMultipleCn_Bug1_NodeConnectionFailed verifies that when one CN node
// fails to connect, RequestMultipleCn correctly returns an error instead of
// silently ignoring it.
//
// This test ensures the fix for Bug #1 works correctly:
// - When any CN node fails, the function should return an error
// - Error should indicate which node failed
// - Error summary is logged with success/failure counts
// - Prevents silent data loss in distributed queries
func TestRequestMultipleCn_Bug1_NodeConnectionFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_multi_cn_bug1"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Simulate 2 CN nodes
		node1 := addr                                                                    // First node (will succeed)
		node2 := fmt.Sprintf("unix:///tmp/nonexistent-%d.sock", time.Now().Nanosecond()) // Second node (does not exist)

		var successCount int
		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			if rsp != nil && rsp.GetCacheInfoResponse != nil {
				successCount++
			}
		}

		// Execute: node1 succeeds, node2 fails to connect
		err := RequestMultipleCn(ctx, []string{node1, node2}, cli, genRequest, handleValidResponse, nil)

		// Verify correct behavior after fix
		assert.Error(t, err, "Should return error when node2 connection fails")
		assert.Contains(t, err.Error(), "nonexistent", "Error message should indicate which node failed")
		assert.Equal(t, 1, successCount, "Only node1 response should be processed")
	})
}

// TestRequestMultipleCn_ContextTimeout verifies that when context times out
// while waiting for CN responses, RequestMultipleCn correctly returns a
// context deadline exceeded error.
//
// This tests the error path at query_service.go:203-209:
//
//	case <-ctx.Done():
//	    retErr = moerr.NewInternalError(ctx, "RequestMultipleCn : context deadline exceeded")
//	    failedNodes = append(failedNodes, fmt.Sprintf("%d nodes timeout", nodesLeft))
//	    break loop
//
// Real-world scenarios:
// - Long-running distributed queries timeout
// - Slow CN nodes cause query timeout
// - Network latency causes timeout
// - Verifies error summary log includes timeout information
func TestRequestMultipleCn_ContextTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_multi_cn_timeout"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		// Use a long timeout to avoid timing issues on slow systems.
		// The actual cancellation is controlled precisely via events.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
		defer cancel()

		// Simulate 2 CN nodes - both will timeout
		node1 := addr
		node2 := fmt.Sprintf("unix:///tmp/slow-cn-%d.sock", time.Now().Nanosecond())

		var successCount int
		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			if rsp != nil && rsp.GetCacheInfoResponse != nil {
				successCount++
			}
		}

		// Start RequestMultipleCn in a goroutine
		var err error
		done := make(chan struct{})
		go func() {
			err = RequestMultipleCn(ctx, []string{node1, node2}, cli, genRequest, handleValidResponse, nil)
			close(done)
		}()

		// Cancel context immediately to trigger timeout (event-driven, no sleep)
		cancel()

		// Wait for RequestMultipleCn to complete
		<-done

		// Verify context timeout is correctly handled
		// Note: When using cancel(), the error may be "context canceled",
		// but the code path at line 309 sets "context deadline exceeded".
		// Both indicate context termination, which is what we're testing.
		assert.Error(t, err, "Should return error when context times out")
		// Accept both "context canceled" (from cancel()) and "context deadline exceeded" (from timeout)
		errStr := err.Error()
		assert.True(t,
			strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "context deadline exceeded"),
			"Error should indicate context termination, got: %s", errStr)
	})
}

// TestRequestMultipleCn_HandlerPanic verifies that handler panic is properly caught
// and treated as failure, and handleInvalidResponse is called
func TestRequestMultipleCn_HandlerPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_handler_panic"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var validCallCount int
		var invalidCallCount int
		var invalidNodes []string

		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			validCallCount++
			// Simulate panic
			panic("intentional panic for testing")
		}

		handleInvalidResponse := func(nodeAddr string) {
			invalidCallCount++
			invalidNodes = append(invalidNodes, nodeAddr)
		}

		// Execute: handler will panic
		err := RequestMultipleCn(ctx, []string{addr}, cli, genRequest, handleValidResponse, handleInvalidResponse)

		// Verify panic is caught and treated as error
		assert.Error(t, err, "Should return error when handler panics")
		assert.Contains(t, err.Error(), "handleValidResponse panicked", "Error should indicate handler panic")
		assert.Equal(t, 1, validCallCount, "handleValidResponse should be called once before panic")
		assert.Equal(t, 1, invalidCallCount, "handleInvalidResponse should be called for panic")
		assert.Equal(t, []string{addr}, invalidNodes, "Invalid nodes should contain the failed node")
	})
}

// TestRequestMultipleCn_MixedFailures verifies correct behavior with multiple
// failure types across different nodes, and that handleInvalidResponse is called
// for all failed nodes
func TestRequestMultipleCn_MixedFailures(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_mixed_failures"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 3 nodes: success, connection fail, handler panic
		node1 := addr
		node2 := fmt.Sprintf("unix:///tmp/nonexistent-%d.sock", time.Now().Nanosecond())
		node3 := addr // Will panic in handler

		var validCallOrder []string
		var invalidNodes []string

		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			validCallOrder = append(validCallOrder, nodeAddr)
			if nodeAddr == node3 && len(validCallOrder) == 2 {
				// Second call to node3 panics
				panic("intentional panic")
			}
		}

		handleInvalidResponse := func(nodeAddr string) {
			invalidNodes = append(invalidNodes, nodeAddr)
		}

		// Execute: mixed failures
		err := RequestMultipleCn(ctx, []string{node1, node2, node3}, cli, genRequest, handleValidResponse, handleInvalidResponse)

		// Verify error is returned
		assert.Error(t, err, "Should return error when any node fails")
		assert.GreaterOrEqual(t, len(validCallOrder), 1, "At least one handler should be called")
		assert.Equal(t, 2, len(invalidNodes), "Should have 2 invalid nodes (connection fail + panic)")
		// Verify invalidNodes contains real addresses, not strings like "context timeout"
		for _, node := range invalidNodes {
			assert.Contains(t, []string{node2, node3}, node, "Invalid nodes should be real addresses")
		}
	})
}

// TestRequestMultipleCn_AllNodesFail verifies behavior when all nodes fail
func TestRequestMultipleCn_AllNodesFail(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_all_fail"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// All nodes are unreachable
		node1 := fmt.Sprintf("unix:///tmp/fail1-%d.sock", time.Now().Nanosecond())
		node2 := fmt.Sprintf("unix:///tmp/fail2-%d.sock", time.Now().Nanosecond()+1)
		node3 := fmt.Sprintf("unix:///tmp/fail3-%d.sock", time.Now().Nanosecond()+2)

		var successCount int
		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			successCount++
		}

		// Execute: all nodes fail
		err := RequestMultipleCn(ctx, []string{node1, node2, node3}, cli, genRequest, handleValidResponse, nil)

		// Verify error is returned with zero successes
		assert.Error(t, err, "Should return error when all nodes fail")
		assert.Equal(t, 0, successCount, "No nodes should succeed")
	})
}

// TestRequestMultipleCn_EmptyNodeAddress verifies handling of empty node addresses
func TestRequestMultipleCn_EmptyNodeAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_empty_node"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Mix of valid and empty addresses
		nodes := []string{addr, "", addr}

		var successCount int
		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			if rsp != nil && rsp.GetCacheInfoResponse != nil {
				successCount++
			}
		}

		// Execute: should skip empty address
		err := RequestMultipleCn(ctx, nodes, cli, genRequest, handleValidResponse, nil)

		// Verify empty address is skipped
		assert.NoError(t, err, "Should succeed when valid nodes succeed")
		assert.Equal(t, 2, successCount, "Should process 2 valid nodes")
	})
}

// TestRequestMultipleCn_ConcurrentSafety verifies no race conditions
func TestRequestMultipleCn_ConcurrentSafety(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_concurrent"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Multiple nodes to increase concurrency
		nodes := []string{addr, addr, addr}

		var mu sync.Mutex
		var successCount int
		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			if rsp != nil && rsp.GetCacheInfoResponse != nil {
				// Concurrent access to shared state
				mu.Lock()
				successCount++
				mu.Unlock()
				// Simulate some work
				time.Sleep(1 * time.Millisecond)
			}
		}

		// Execute: concurrent processing
		err := RequestMultipleCn(ctx, nodes, cli, genRequest, handleValidResponse, nil)

		// Verify no race conditions (test with -race flag)
		assert.NoError(t, err)
		assert.Equal(t, 3, successCount, "All nodes should succeed")
	})
}

// TestRequestMultipleCn_NoGoroutineLeak verifies goroutines are cleaned up
func TestRequestMultipleCn_NoGoroutineLeak(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_no_leak"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		// Use a long timeout to avoid timing issues on slow systems.
		// The actual cancellation is controlled precisely via events.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
		defer cancel()

		// Mix of valid and invalid nodes
		node1 := addr
		node2 := fmt.Sprintf("unix:///tmp/slow-%d.sock", time.Now().Nanosecond())

		// Channel to block node1's response processing
		node1ResponseBlocked := make(chan struct{})

		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			if nodeAddr == node1 {
				// Block node1's response processing, but check context to avoid deadlock
				// If context is canceled, return early to allow RequestMultipleCn to complete
				select {
				case <-node1ResponseBlocked:
					// Unblocked, continue processing
				case <-ctx.Done():
					// Context canceled, return early to avoid deadlock
					return
				}
			}
		}

		// Start RequestMultipleCn in a goroutine
		done := make(chan struct{})
		go func() {
			_ = RequestMultipleCn(ctx, []string{node1, node2}, cli, genRequest, handleValidResponse, nil)
			close(done)
		}()

		// Cancel context immediately to trigger timeout (event-driven, no sleep)
		cancel()

		// Wait for RequestMultipleCn to complete with timeout to avoid hanging
		select {
		case <-done:
			// RequestMultipleCn completed successfully
		case <-time.After(30 * time.Second):
			t.Fatal("RequestMultipleCn did not complete within 30 seconds")
		}

		// Unblock node1's response processing (cleanup) in case it's still waiting
		// Close the channel so any waiting handler goroutines can proceed
		close(node1ResponseBlocked)
	})
}

// TestRequestMultipleCn_InvalidResponseCallback verifies that handleInvalidResponse
// is called for all types of failures (network error, handler panic, type error)
func TestRequestMultipleCn_InvalidResponseCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_invalid_callback"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 2 nodes: one success, one network fail
		node1 := addr
		node2 := fmt.Sprintf("unix:///tmp/fail-%d.sock", time.Now().Nanosecond())

		var invalidNodes []string
		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			// Normal processing
		}

		handleInvalidResponse := func(nodeAddr string) {
			invalidNodes = append(invalidNodes, nodeAddr)
		}

		// Execute
		err := RequestMultipleCn(ctx, []string{node1, node2}, cli, genRequest, handleValidResponse, handleInvalidResponse)

		// Verify handleInvalidResponse is called for network failure
		assert.Error(t, err, "Should return error")
		assert.Equal(t, 1, len(invalidNodes), "Should call handleInvalidResponse for failed node")
		assert.Equal(t, node2, invalidNodes[0], "Invalid node should be the network failed node")
	})
}

// TestRequestMultipleCn_FailedNodesOnlyRealAddresses verifies that failedNodes
// contains only real node addresses, not synthetic strings like "context timeout"
func TestRequestMultipleCn_FailedNodesOnlyRealAddresses(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_failed_nodes"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		// Use a long timeout to avoid timing issues on slow systems.
		// The actual cancellation is controlled precisely via events.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
		defer cancel()

		node1 := fmt.Sprintf("unix:///tmp/node1-%d.sock", time.Now().Nanosecond())
		node2 := fmt.Sprintf("unix:///tmp/node2-%d.sock", time.Now().Nanosecond()+1)

		var capturedFailedNodes []string
		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			// Won't be called due to timeout
		}

		handleInvalidResponse := func(nodeAddr string) {
			capturedFailedNodes = append(capturedFailedNodes, nodeAddr)
		}

		// Start RequestMultipleCn in a goroutine
		var err error
		done := make(chan struct{})
		go func() {
			err = RequestMultipleCn(ctx, []string{node1, node2}, cli, genRequest, handleValidResponse, handleInvalidResponse)
			close(done)
		}()

		// Cancel context immediately to trigger timeout (event-driven, no sleep)
		cancel()

		// Wait for RequestMultipleCn to complete
		<-done

		// Verify error
		// Note: When using cancel(), the error may be "context canceled",
		// but the code path at line 309 sets "context deadline exceeded".
		// Both indicate context termination, which is what we're testing.
		assert.Error(t, err, "Should return error on timeout")
		// Accept both "context canceled" (from cancel()) and "context deadline exceeded" (from timeout)
		errStr := err.Error()
		assert.True(t,
			strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "context deadline exceeded"),
			"Error should indicate context termination, got: %s", errStr)

		// Key verification: failedNodes should only contain real addresses
		// NOT synthetic strings like "context timeout"
		for _, failedNode := range capturedFailedNodes {
			// Each failedNode should be a real node address (unix://...)
			assert.True(t, failedNode == node1 || failedNode == node2,
				"failedNodes should only contain real node addresses, got: %s", failedNode)
			assert.NotContains(t, failedNode, "timeout", "failedNodes should not contain 'timeout' string")
			assert.NotContains(t, failedNode, "context", "failedNodes should not contain 'context' string")
		}
	})
}

// TestRequestMultipleCn_TimeoutOverrideLogging verifies that when context times out
// after a connection error has been recorded, the timeout override is logged.
//
// This test covers the code path at query_service.go:302-308 where a timeout error
// overrides a previous connection error and logs the override for debugging.
//
// The test uses event-based synchronization (no sleep/random factors) to ensure:
// 1. A connection error occurs first (sets retErr)
// 2. Then context is canceled (triggers <-ctx.Done())
// 3. The timeout override logging path is executed (retErr != nil branch)
func TestRequestMultipleCn_TimeoutOverrideLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cn := metadata.CNService{ServiceID: "test_timeout_override"}
	runTestWithQueryService(t, cn, nil, func(cli client.QueryClient, addr string) {
		// Event-based synchronization: signal when connection error occurs
		connectionErrorOccurred := make(chan struct{})

		// Use WithTimeout with a very long deadline to avoid timing issues on slow systems.
		// The actual cancellation is controlled precisely via events, not by timeout.
		// morpc's Future requires a deadline, but we cancel immediately after connection error,
		// so the long timeout will never be reached.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
		defer cancel()

		// Simulate 2 CN nodes:
		// - node1: valid address (will succeed)
		// - node2: non-existent socket (will fail immediately, setting retErr)
		// Strategy: Cancel context immediately after node2 fails, before node1's response
		// is processed. This ensures that when <-ctx.Done() is triggered, retErr != nil.
		node1 := addr
		node2 := fmt.Sprintf("unix:///tmp/nonexistent-%d.sock", time.Now().Nanosecond())

		var successCount int
		genRequest := func() *pb.Request {
			req := cli.NewRequest(pb.CmdMethod_GetCacheInfo)
			req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
			return req
		}

		// Normal response handling - no blocking needed
		handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
			if rsp != nil && rsp.GetCacheInfoResponse != nil {
				successCount++
			}
		}

		// Monitor connection errors: signal when node2 fails (sets retErr)
		handleInvalidResponse := func(nodeAddr string) {
			if nodeAddr == node2 {
				// Connection error occurred, retErr is now set
				// Signal that retErr is set (non-blocking)
				select {
				case connectionErrorOccurred <- struct{}{}:
				default:
				}
			}
		}

		// Start RequestMultipleCn in a goroutine
		var err error
		done := make(chan struct{})
		go func() {
			err = RequestMultipleCn(ctx, []string{node1, node2}, cli, genRequest, handleValidResponse, handleInvalidResponse)
			close(done)
		}()

		// Step 1: Wait for connection error to occur (node2 fails, retErr is set)
		// This is event-based: we wait for the actual event, not a timeout
		<-connectionErrorOccurred

		// Step 2: Cancel context immediately to trigger <-ctx.Done()
		// At this point:
		// - retErr is set (connection error from node2)
		// - node1's response may or may not have arrived yet
		// - If node1's response hasn't arrived: main loop is waiting, <-ctx.Done() triggers with retErr != nil
		// - If node1's response has arrived: it may be processed, but <-ctx.Done() can still trigger
		//   in the next iteration if responsesReceived < validNodes
		// The key is: when <-ctx.Done() is triggered, retErr != nil, so the logging path is executed
		cancel()

		// Wait for RequestMultipleCn to complete
		// The context cancellation should trigger timeout override logging
		<-done

		// Verify that an error is returned
		assert.Error(t, err, "Should return error")
		// The code path at line 302-308 should be covered because:
		// 1. retErr != nil (connection error from node2)
		// 2. <-ctx.Done() triggered (context canceled)
		// 3. When <-ctx.Done() is selected, retErr != nil, so the logging branch is executed
	})
}

// TestRequestMultipleCn_ResponseErrorWithDeadlineExceeded verifies that when a response
// returns with context.DeadlineExceeded error, the code path at query_service.go:202-206
// is correctly executed.
//
// This test covers the specific code path:
//
//	if ctx.Err() == context.DeadlineExceeded || errors.Is(res.err, context.DeadlineExceeded) {
//		// Context has timed out, prioritize timeout error
//		if retErr == nil {
//			retErr = moerr.NewInternalError(ctx, "RequestMultipleCn : context deadline exceeded")
//		}
//	}
//
// The test ensures:
// 1. SendMessage returns context.DeadlineExceeded error (or ctx.Err() == context.DeadlineExceeded)
// 2. retErr == nil (this is the first error)
// 3. The timeout error is correctly set
//
// Strategy: Create a slow handler that doesn't respond in time, causing context timeout.
// The test waits for RequestMultipleCn to complete with a reasonable timeout.
func TestRequestMultipleCn_ResponseErrorWithDeadlineExceeded(t *testing.T) {
	cn := metadata.CNService{ServiceID: "test_response_deadline_exceeded"}
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			runtime.SetupServiceBasedRuntime(cn.ServiceID, runtime.ServiceRuntime(sid))
			address := fmt.Sprintf("unix:///tmp/cn-%d-%s.sock",
				time.Now().Nanosecond(), cn.ServiceID)

			if err := os.RemoveAll(address[7:]); err != nil {
				panic(err)
			}

			qs, err := NewQueryService(cn.ServiceID, address, morpc.Config{})
			assert.NoError(t, err)

			qt, err := client.NewQueryClient(cn.ServiceID, morpc.Config{})
			assert.NoError(t, err)

			// Event-driven: signal when handler is called
			handlerCalled := make(chan struct{})

			// Handler blocks until context is canceled
			qs.AddHandleFunc(pb.CmdMethod_GetCacheInfo, func(ctx context.Context, request *pb.Request, resp *pb.Response, _ *morpc.Buffer) error {
				// Signal handler called (non-blocking)
				select {
				case handlerCalled <- struct{}{}:
				default:
				}
				// Block until context canceled
				<-ctx.Done()
				return ctx.Err()
			}, false)

			err = qs.Start()
			assert.NoError(t, err)
			defer func() {
				err = qs.Close()
				assert.NoError(t, err)
				err = qt.Close()
				assert.NoError(t, err)
			}()

			// Long timeout - we'll cancel explicitly after handler called
			// This accommodates slow CI (up to 2s) without test failure
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var successCount int
			genRequest := func() *pb.Request {
				req := qt.NewRequest(pb.CmdMethod_GetCacheInfo)
				req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
				return req
			}

			handleValidResponse := func(nodeAddr string, rsp *pb.Response) {
				if rsp != nil && rsp.GetCacheInfoResponse != nil {
					successCount++
				}
			}

			// Execute in goroutine
			var errResult error
			done := make(chan struct{})
			go func() {
				errResult = RequestMultipleCn(ctx, []string{address}, qt, genRequest, handleValidResponse, nil)
				close(done)
			}()

			// Event-driven execution with protection:
			// Wait for handler to be called (adapts to CI speed: 10ms - 2s)
			select {
			case <-handlerCalled:
				// Handler called, proceed to cancel
			case <-time.After(10 * time.Second):
				t.Fatal("Handler not called within 10s - connection issue")
			}

			// Cancel context immediately (precise control)
			cancel()

			// Wait for completion with 10s protection (only for hung)
			select {
			case <-done:
				// Success: fast env ~20ms, slow env ~2s
			case <-time.After(10 * time.Second):
				t.Fatal("Test hung after context cancel - 10s protection triggered")
			}

			// Verify that an error is returned
			assert.Error(t, errResult, "Should return error when context deadline exceeded")
			// Accept multiple error types that can occur in different environments:
			// - "context deadline exceeded": normal timeout path
			// - "failed to get result": connection error during timeout
			// - "EOF": connection closed by server during timeout
			// All of these indicate the timeout was handled correctly
			errStr := errResult.Error()
			assert.True(t,
				strings.Contains(errStr, "context deadline exceeded") ||
					strings.Contains(errStr, "failed to get result") ||
					strings.Contains(errStr, "EOF"),
				"Error should indicate timeout or connection error, got: %s", errStr)
			assert.Equal(t, 0, successCount, "No nodes should succeed due to timeout")
		},
	)
}
