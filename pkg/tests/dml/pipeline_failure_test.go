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

package dml

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Interpose only the terminal of one real stream after receiving real data.
// The same SQL connection must surface failure, then execute healthy queries.
func TestRemotePipelineFailureSQLContract(t *testing.T) {
	var invalidationErr error
	defer func() {
		if invalidationErr != nil {
			t.Errorf("discarding shared fixture: %v", invalidationErr)
			require.NoError(t, embed.CloseBaseClusterTests())
		}
	}()
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		cluster := clusterservice.GetMOCluster(cn.ServiceID())
		inventory, ok := cluster.(cnWorkStateInventory)
		require.True(t, ok)
		refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		readiness, err := waitForCNReadiness(ctx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		require.NoError(t, err)
		db := openRetestSQLDB(t, c)
		defer db.Close()
		const name = "pipeline_failure_contract"
		defer func() {
			if invalidationErr == nil {
				cleanupTestDatabases(t, db, name)
			}
		}()
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(k int not null)")
		execSQLDB(t, ctx, db, "insert into src values (1),(2)")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")
		oldForce := plan.GetForceScanOnMultiCN()
		defer plan.SetForceScanOnMultiCN(oldForce)
		plan.SetForceScanOnMultiCN(true)
		err = withCNDraining(ctx, inventory, refresher, cn.ServiceID(), []string{cn.ServiceID(), peer.ServiceID()},
			func(err error) { invalidationErr = err }, func() {
				const query = "select k from src except select k from src"
				physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
				require.NoError(t, err)
				require.Contains(t, physical.Text, readiness.peerAddr)
				require.Empty(t, queryStringRows(t, ctx, db, query))
				rt := runtime.ServiceRuntime(cn.ServiceID())
				original := cnclient.GetPipelineClient(cn.ServiceID())
				injected := &terminalFailureClient{PipelineClient: original}
				rt.SetGlobalVariables(runtime.PipelineClient, injected)
				// Restore even when an assertion terminates this callback.
				defer rt.SetGlobalVariables(runtime.PipelineClient, original)
				rows, queryErr := db.QueryContext(ctx, "select k from src")
				if queryErr == nil {
					defer rows.Close()
					for rows.Next() {
						var value int
						require.NoError(t, rows.Scan(&value))
					}
					queryErr = rows.Err()
				}
				rt.SetGlobalVariables(runtime.PipelineClient, original)
				require.Equal(t, int32(1), injected.failures.Load(), "fault must follow real remote data")
				var sqlErr *mysql.MySQLError
				require.ErrorAs(t, queryErr, &sqlErr)
				require.Equal(t, uint16(moerr.ER_QUERY_INTERRUPTED), sqlErr.Number)
				require.Equal(t, "70100", string(sqlErr.SQLState[:]))
				require.NoError(t, ctx.Err(), "user context remains live")
				require.Empty(t, queryStringRows(t, ctx, db, query))
				require.Equal(t, [][]string{{"1"}}, queryStringRows(t, ctx, db, "select 1 from src limit 1"))
				require.Equal(t, [][]string{{"1"}}, queryStringRows(t, ctx, db, "select k from src order by k limit 1"))
				require.Equal(t, [][]string{{"1"}, {"2"}}, queryStringRows(t, ctx, db, "select k from src order by k"))
			})
		require.NoError(t, err)
	})
}

type terminalFailureClient struct {
	cnclient.PipelineClient
	failures atomic.Int32
}

func (c *terminalFailureClient) NewStream(ctx context.Context, backend string) (morpc.Stream, error) {
	stream, err := c.PipelineClient.NewStream(ctx, backend)
	if err != nil {
		return nil, err
	}
	return &terminalFailureStream{Stream: stream, client: c, ctx: ctx, done: make(chan struct{})}, nil
}

type terminalFailureStream struct {
	morpc.Stream
	client *terminalFailureClient
	ctx    context.Context
	done   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func (s *terminalFailureStream) Receive() (chan morpc.Message, error) {
	source, err := s.Stream.Receive()
	if err != nil {
		return nil, err
	}
	output := make(chan morpc.Message)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(output)
		sawData := false
		for {
			var msg morpc.Message
			select {
			case <-s.done:
				return
			case <-s.ctx.Done():
				return
			case received, ok := <-source:
				if !ok || received == nil {
					return
				}
				msg = received
			}
			if m, ok := msg.(*pipeline.Message); ok {
				if !m.IsEndMessage() && len(m.Data) != 0 && !m.WaitingNextToMerge() {
					sawData = true
				}
				if m.IsEndMessage() && sawData && s.client.failures.CompareAndSwap(0, 1) {
					m.SetMoError(context.Background(), moerr.NewQueryInterrupted(context.Background()))
				}
			}
			select {
			case output <- msg:
			case <-s.done:
				return
			case <-s.ctx.Done():
				return
			}
		}
	}()
	return output, nil
}

func (s *terminalFailureStream) Close(closeConn bool) error {
	s.once.Do(func() { close(s.done) })
	err := s.Stream.Close(closeConn)
	s.wg.Wait()
	return err
}
