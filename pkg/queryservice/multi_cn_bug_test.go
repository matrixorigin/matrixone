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
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"

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
				t.Logf("Received response from: %s", nodeAddr)
			}
		}

		// Execute: node1 succeeds, node2 fails to connect
		err := RequestMultipleCn(ctx, []string{node1, node2}, cli, genRequest, handleValidResponse, nil)

		// Verify correct behavior after fix
		t.Logf("RequestMultipleCn returned error: %v", err)
		t.Logf("Success count: %d (node1 succeeded, node2 failed)", successCount)

		// After fix: should return error when node2 fails
		assert.Error(t, err, "Should return error when node2 connection fails")
		assert.Contains(t, err.Error(), "nonexistent", "Error message should indicate which node failed")
		assert.Equal(t, 1, successCount, "Only node1 response should be processed")

		t.Log("")
		t.Log("✅ Fix verified: Error is correctly returned when CN node fails")
		t.Log("  - 2 nodes, node2 connection failed")
		t.Log("  - Function correctly returns error (not nil)")
		t.Log("  - Error message indicates failed node")
		t.Log("  - Prevents silent data loss in distributed queries")
	})
}
