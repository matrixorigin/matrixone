// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
)

type processlistClient struct {
	qclient.QueryClient
	mu       sync.Mutex
	requests []*query.Request
	sessions []*status.Session
	err      error
}

func (c *processlistClient) ServiceID() string { return "processlist-authorization" }
func (c *processlistClient) NewRequest(m query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: m}
}
func (c *processlistClient) SendMessage(ctx context.Context, addr string, r *query.Request) (*query.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, r)
	return &query.Response{ShowProcessListResponse: &query.ShowProcessListResponse{Sessions: c.sessions}}, c.err
}
func (c *processlistClient) Release(*query.Response) {}

func TestProcesslistVisibility(t *testing.T) {
	c := &processlistClient{sessions: []*status.Session{
		{ConnID: 1, Account: "sys", User: "root"}, {ConnID: 2, Account: "sys", User: "low"},
		{ConnID: 3, Account: "acme", User: "low"}, {ConnID: 4, Account: "acme", User: "other"},
		{ConnID: 5, Account: "other", User: "low"}, nil,
	}}
	runtime.SetupServiceBasedRuntime(c.ServiceID(), runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(c.ServiceID(), nil, time.Hour, clusterservice.WithDisableRefresh(), clusterservice.WithServices([]metadata.CNService{{ServiceID: "cn1", QueryAddress: "a"}, {ServiceID: "cn2", QueryAddress: "b"}}, nil))
	defer cluster.Close()
	runtime.ServiceRuntime(c.ServiceID()).SetGlobalVariables(runtime.ClusterService, cluster)
	for _, tc := range []struct {
		account, user, role string
		ids                 []uint32
		global              bool
	}{
		{"sys", "low", "public", []uint32{2, 2}, false},
		{"sys", "root", "public", []uint32{1, 1}, false},
		{"sys", "low", "moadmin", []uint32{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}, true},
		{"acme", "low", "public", []uint32{3, 3}, false},
		{"acme", "low", "accountadmin", []uint32{3, 3, 4, 4}, false},
		{"acme", "low", "moadmin", []uint32{3, 3}, false},
		{"sys", "low", "accountadmin", []uint32{2, 2}, false},
		{"sys", "low", "", nil, false}, {"", "low", "moadmin", nil, false},
	} {
		t.Run(tc.account+"/"+tc.user+"/"+tc.role, func(t *testing.T) {
			c.requests = nil
			sessions, err := fetchSessions(context.Background(), tc.account, tc.user, tc.role, c)
			require.NoError(t, err)
			var ids []uint32
			for _, s := range sessions {
				ids = append(ids, s.ConnID)
			}
			require.ElementsMatch(t, tc.ids, ids)
			require.Len(t, c.requests, 2)
			for _, req := range c.requests {
				require.Equal(t, tc.global, req.ShowProcessListRequest.SysTenant)
				require.Equal(t, tc.account, req.ShowProcessListRequest.Tenant)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fetchSessions(ctx, "sys", "low", "public", c)
	require.Error(t, err)
}
