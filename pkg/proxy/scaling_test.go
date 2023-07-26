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

package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestDoScalingIn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var err error
	tp := newTestProxyHandler(t)
	defer tp.closeFn()

	temp := os.TempDir()
	// Construct backend CN servers.
	addr1 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr1))
	tenantName := "t1"
	cn11 := testMakeCNServer("cn11", addr1, 0, "",
		newLabelInfo(Tenant(tenantName), map[string]string{}),
	)
	li := labelInfo{
		Tenant: Tenant(tenantName),
	}
	cn11.hash, err = li.getHash()
	require.NoError(t, err)
	tp.hc.updateCN("cn11", cn11.addr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{tenantName}},
	})
	stopFn11 := startTestCNServer(t, tp.ctx, addr1, nil)
	defer func() {
		require.NoError(t, stopFn11())
	}()

	addr2 := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr2))
	cn12 := testMakeCNServer("cn12", addr2, 0, "",
		newLabelInfo(Tenant(tenantName), map[string]string{}),
	)
	cn12.hash, err = li.getHash()
	require.NoError(t, err)
	tp.hc.updateCN("cn12", cn12.addr, map[string]metadata.LabelList{
		tenantLabelKey: {Labels: []string{tenantName}},
	})
	stopFn12 := startTestCNServer(t, tp.ctx, addr2, nil)
	defer func() {
		require.NoError(t, stopFn12())
	}()
	tp.mc.ForceRefresh()
	time.Sleep(time.Millisecond * 200)

	ctx, cancel := context.WithTimeout(tp.ctx, 10*time.Second)
	defer cancel()

	ci := clientInfo{
		labelInfo: li,
		username:  "test",
		originIP:  net.ParseIP("127.0.0.1"),
	}
	cleanup1 := testStartNClients(t, tp, ci, cn11, 2)
	defer cleanup1()

	cleanup2 := testStartNClients(t, tp, ci, cn12, 2)
	defer cleanup2()

	err = tp.hc.updateCNWorkState(ctx, logpb.CNWorkState{
		UUID:  cn11.uuid,
		State: metadata.WorkState_Draining,
	})
	require.NoError(t, err)

	tick := time.NewTicker(time.Millisecond * 200)
	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "scaling in failed")
		case <-tick.C:
			tunnels1 := tp.re.connManager.getTunnelsByCNID(cn11.uuid)
			tunnels2 := tp.re.connManager.getTunnelsByCNID(cn12.uuid)
			tp.re.connManager.Lock()
			if len(tunnels1) == 0 && len(tunnels2) == 4 {
				tp.re.connManager.Unlock()
				return
			}
			tp.re.connManager.Unlock()
		}
	}
}
