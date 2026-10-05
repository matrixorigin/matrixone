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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Interpose one target statement terminal without disturbing other CN requests.
// The same SQL connection must surface failure, then execute healthy queries.
func TestRemotePipelineFailureSQLContract(t *testing.T) {
	var invalidationErr error
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
				var connectionID uint64
				require.NoError(t, db.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
				defer rt.SetGlobalVariables(runtime.PipelineClient, original)
				for _, svc := range []string{cn.ServiceID(), peer.ServiceID()} {
					configRuntime := runtime.ServiceRuntime(svc)
					previous, exists := configRuntime.GetGlobalVariables(runtime.EnablePipelineStreamReuse)
					defer func() {
						if exists {
							configRuntime.SetGlobalVariables(runtime.EnablePipelineStreamReuse, previous)
						} else {
							current, _ := configRuntime.GetGlobalVariables(runtime.EnablePipelineStreamReuse)
							configRuntime.CompareAndDeleteGlobalVariables(runtime.EnablePipelineStreamReuse, current)
						}
					}()
				}
				for _, reuse := range []bool{true, false} {
					for _, svc := range []string{cn.ServiceID(), peer.ServiceID()} {
						runtime.ServiceRuntime(svc).SetGlobalVariables(runtime.EnablePipelineStreamReuse, reuse)
					}
					for _, scenario := range []struct {
						fault   string
						targets []string
					}{
						{"execution", []string{"select k from src", "select k from src union all select k from src limit 1", "select k from src union all select k from src limit 2", query}},
						{"malformed", []string{"select k from src", "select k from src union all select k from src limit 1"}},
						{"execution malformed", []string{"select k from src", "select k from src union all select k from src limit 1"}},
					} {
						for _, target := range scenario.targets {
							injected := &terminalFailureClient{PipelineClient: original, sql: target, connectionID: connectionID, allowEmpty: target != "select k from src", fault: scenario.fault}
							rt.SetGlobalVariables(runtime.PipelineClient, injected)
							// A different real remote request must neither fail nor
							// consume the target statement's one-shot fault.
							require.Equal(t, [][]string{{"2"}}, queryStringRows(t, ctx, db, "select count(*) from src"))
							require.Zero(t, injected.failures.Load())
							func() {
								rows, queryErr := db.QueryContext(ctx, target)
								if queryErr == nil {
									defer rows.Close()
									for rows.Next() {
										var value int
										require.NoError(t, rows.Scan(&value))
									}
									queryErr = rows.Err()
								}
								require.Equal(t, int32(1), injected.failures.Load(), "reuse=%t sql=%s", reuse, target)
								var sqlErr *mysql.MySQLError
								require.ErrorAs(t, queryErr, &sqlErr)
								if scenario.fault == "malformed" {
									require.Contains(t, sqlErr.Message, "unexpected end of JSON input")
								} else {
									require.Equal(t, uint16(moerr.ER_QUERY_INTERRUPTED), sqlErr.Number)
									require.Equal(t, "70100", string(sqlErr.SQLState[:]))
								}
							}()
							require.NoError(t, ctx.Err(), "user context remains live")
							require.Equal(t, [][]string{{"1"}, {"2"}}, queryStringRows(t, ctx, db, "select k from src order by k"))
							require.Equal(t, int32(1), injected.failures.Load())
							var currentConnectionID uint64
							require.NoError(t, db.QueryRowContext(ctx, "select connection_id()").Scan(&currentConnectionID))
							require.Equal(t, connectionID, currentConnectionID)
							rt.SetGlobalVariables(runtime.PipelineClient, original)
						}
					}
				}
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
	failures     atomic.Int32
	sql          string
	connectionID uint64
	allowEmpty   bool
	fault        string
	statement    atomic.Pointer[[16]byte]
}

func (c *terminalFailureClient) NewStream(ctx context.Context, backend string) (morpc.Stream, error) {
	stream, err := c.PipelineClient.NewStream(ctx, backend)
	if err != nil {
		return nil, err
	}
	return &terminalFailureStream{Stream: stream, client: c, done: make(chan struct{})}, nil
}

type terminalFailureStream struct {
	morpc.Stream
	client  *terminalFailureClient
	request atomic.Pointer[terminalFailureRequest]
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

// Each execution request gets a fresh snapshot so pooled streams cannot carry
// eligibility or observed data into the next request. Control messages retain it.
type terminalFailureRequest struct {
	method pipeline.Method
}

func (s *terminalFailureStream) Send(ctx context.Context, msg morpc.Message) error {
	if m, ok := msg.(*pipeline.Message); ok && m.GetCmd() == pipeline.Method_PipelineMessage {
		s.request.Store(nil)
		var info pipeline.ProcessInfo
		if info.Unmarshal(m.ProcInfoData) == nil && info.Sql == s.client.sql &&
			info.SessionInfo.ConnectionId == s.client.connectionID && len(info.SessionLogger.StmtId) == 16 {
			var statement [16]byte
			copy(statement[:], info.SessionLogger.StmtId)
			if statement != ([16]byte{}) {
				s.client.statement.CompareAndSwap(nil, &statement)
				if *s.client.statement.Load() == statement {
					s.request.Store(&terminalFailureRequest{method: m.GetCmd()})
				}
			}
		}
	}
	return s.Stream.Send(ctx, msg)
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
		var observed *terminalFailureRequest
		for {
			var msg morpc.Message
			select {
			case <-s.done:
				return
			case received, ok := <-source:
				if !ok || received == nil {
					return
				}
				msg = received
			}
			if m, ok := msg.(*pipeline.Message); ok {
				request := s.request.Load()
				if request != observed {
					observed = request
					sawData = false
				}
				if request != nil && m.GetID() == s.ID() && m.GetCmd() == pipeline.Method_BatchMessage &&
					len(m.Data) != 0 && !m.WaitingNextToMerge() {
					sawData = true
				}
				if request != nil && m.GetID() == s.ID() && m.GetCmd() == request.method &&
					m.IsEndMessage() && (sawData || s.client.allowEmpty) && s.client.failures.CompareAndSwap(0, 1) {
					if s.client.fault == "malformed" || s.client.fault == "execution malformed" {
						m.Analyse = []byte("{")
					}
					if s.client.fault != "malformed" {
						m.SetMoError(context.Background(), moerr.NewQueryInterrupted(context.Background()))
					}
				}
			}
			select {
			case output <- msg:
			case <-s.done:
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

// Reuse the transport mock to challenge request generations without a cluster.
func TestTerminalFailureStreamRequestIsolation(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := make(chan morpc.Message, 1)
	transport := mock_morpc.NewMockStream(ctrl)
	transport.EXPECT().Receive().Return(source, nil)
	transport.EXPECT().ID().Return(uint64(7)).AnyTimes()
	transport.EXPECT().Send(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	transport.EXPECT().Close(true).Return(nil)
	client := &terminalFailureClient{sql: "target", connectionID: 9}
	stream := &terminalFailureStream{Stream: transport, client: client, done: make(chan struct{})}
	t.Cleanup(func() { require.NoError(t, stream.Close(true)) })
	output, err := stream.Receive()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	send := func(sql string, connection uint64, statement byte) {
		id := make([]byte, 16)
		id[0] = statement
		data, err := (&pipeline.ProcessInfo{Sql: sql, SessionInfo: pipeline.SessionInfo{ConnectionId: connection}, SessionLogger: pipeline.SessionLoggerInfo{StmtId: id}}).Marshal()
		require.NoError(t, err)
		require.NoError(t, stream.Send(ctx, &pipeline.Message{Id: 7, Cmd: pipeline.Method_PipelineMessage, ProcInfoData: data}))
	}
	forward := func(m *pipeline.Message) *pipeline.Message {
		source <- m
		select {
		case got, ok := <-output:
			require.True(t, ok)
			return got.(*pipeline.Message)
		case <-time.After(time.Second):
			t.Fatal("stream adapter stopped forwarding")
			return nil
		}
	}
	end := func() *pipeline.Message {
		return &pipeline.Message{Id: 7, Cmd: pipeline.Method_PipelineMessage, Sid: pipeline.Status_MessageEnd}
	}
	data := func() { forward(&pipeline.Message{Id: 7, Cmd: pipeline.Method_BatchMessage, Data: []byte{1}}) }
	send("target", 9, 1)
	data() // A new request must not inherit this observation.
	for _, tc := range []struct {
		sql        string
		connection uint64
		statement  byte
	}{
		{"other", 9, 1}, {"target", 10, 1}, {"target", 9, 2},
	} {
		send(tc.sql, tc.connection, tc.statement)
		data()
		require.Empty(t, forward(end()).Err)
		require.Zero(t, client.failures.Load())
	}
	send("target", 9, 1)
	data()
	// Fragmented execution invalidates the old snapshot before final metadata.
	require.NoError(t, stream.Send(ctx, &pipeline.Message{Id: 7, Cmd: pipeline.Method_PipelineMessage, Sid: pipeline.Status_WaitingNext}))
	require.Empty(t, forward(end()).Err)
	send("target", 9, 1)
	require.Empty(t, forward(end()).Err, "a previous request's data cannot qualify this terminal")
	data()
	control := &pipeline.Message{Id: 7, Cmd: pipeline.Method_PipelineStreamFinishAck, Sid: pipeline.Status_MessageEnd}
	require.Same(t, control, forward(control))
	require.Empty(t, control.Err)
	require.Zero(t, client.failures.Load())
	cancel() // Detached cleanup must still receive the real terminal.
	terminal := forward(end())
	failure, ok := terminal.TryToGetMoErr()
	require.True(t, ok)
	require.True(t, moerr.IsMoErrCode(failure, moerr.ErrQueryInterrupted))
	require.Equal(t, int32(1), client.failures.Load())
}

func TestTerminalFailureStreamCloseUnblocksForwarding(t *testing.T) {
	for _, blockedOutput := range []bool{false, true} {
		t.Run(fmt.Sprintf("output=%t", blockedOutput), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			source := make(chan morpc.Message)
			transport := mock_morpc.NewMockStream(ctrl)
			transport.EXPECT().Receive().Return(source, nil)
			transport.EXPECT().Close(true).Return(nil)
			stream := &terminalFailureStream{Stream: transport, client: &terminalFailureClient{}, done: make(chan struct{})}
			t.Cleanup(func() {
				select {
				case <-stream.done:
				default:
					require.NoError(t, stream.Close(true))
				}
			})
			output, err := stream.Receive()
			require.NoError(t, err)
			if blockedOutput {
				source <- &pipeline.Message{}
			} // Receive accepted; no output consumer exists.
			closed := make(chan error, 1)
			go func() { closed <- stream.Close(true) }()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("Close did not release adapter")
			}
			_, ok := <-output
			require.False(t, ok)
		})
	}
}
