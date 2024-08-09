// Copyright 2022 Matrix Origin
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
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type processlistState struct {
	simpleOneBatchState
}

func processlistPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var st processlistState
	return &st, nil
}

func (s *processlistState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	s.startPreamble(tf, proc, nthRow)
	bat := s.batch

	// get all sessions
	sessions, err := fetchSessions(proc.Ctx, proc.GetSessionInfo().Account, proc.Base.QueryClient)
	if err != nil {
		return err
	}

	mp := proc.GetMPool()

	for _, session := range sessions {
		for i, col := range tf.Attrs {
			colName := strings.ToUpper(col)
			switch status.SessionField(status.SessionField_value[colName]) {
			case status.SessionField_NODE_ID:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.NodeID), false, mp); err != nil {
					return err
				}
			case status.SessionField_CONN_ID:
				if err := vector.AppendFixed(bat.Vecs[i], session.ConnID, false, mp); err != nil {
					return err
				}
			case status.SessionField_SESSION_ID:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.SessionID), false, mp); err != nil {
					return err
				}
			case status.SessionField_ACCOUNT:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.Account), false, mp); err != nil {
					return err
				}
			case status.SessionField_USER:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.User), false, mp); err != nil {
					return err
				}
			case status.SessionField_HOST:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.Host), false, mp); err != nil {
					return err
				}
			case status.SessionField_DB:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.DB), false, mp); err != nil {
					return err
				}
			case status.SessionField_SESSION_START:
				if err := vector.AppendBytes(bat.Vecs[i],
					[]byte(session.SessionStart.Format("2006-01-02 15:04:05.000000")),
					false, proc.Mp()); err != nil {
					return err
				}
			case status.SessionField_COMMAND:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.Command), false, mp); err != nil {
					return err
				}
			case status.SessionField_INFO:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.Info), false, mp); err != nil {
					return err
				}
			case status.SessionField_TXN_ID:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.TxnID), false, mp); err != nil {
					return err
				}
			case status.SessionField_STATEMENT_ID:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.StatementID), false, mp); err != nil {
					return err
				}
			case status.SessionField_STATEMENT_TYPE:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.StatementType), false, mp); err != nil {
					return err
				}
			case status.SessionField_QUERY_TYPE:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.QueryType), false, mp); err != nil {
					return err
				}
			case status.SessionField_SQL_SOURCE_TYPE:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.SQLSourceType), false, mp); err != nil {
					return err
				}
			case status.SessionField_QUERY_START:
				var queryStart string
				if !session.QueryStart.Equal(time.Time{}) {
					queryStart = session.QueryStart.Format("2006-01-02 15:04:05.000000")
				}
				if err := vector.AppendBytes(bat.Vecs[i], []byte(queryStart),
					false, proc.Mp()); err != nil {
					return err
				}
			case status.SessionField_CLIENT_HOST:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.GetClientHost()), false, mp); err != nil {
					return err
				}
			case status.SessionField_ROLE:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.GetRole()), false, mp); err != nil {
					return err
				}
			case status.SessionField_PROXY_HOST:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.GetProxyHost()), false, mp); err != nil {
					return err
				}
			}
		}
	}

	bat.SetRowCount(len(sessions))
	return nil
}

// isSysTenant return true if the tenant is sys.
func isSysTenant(tenant string) bool {
	return strings.ToLower(tenant) == "sys"
}

// fetchSessions get sessions all nodes which the tenant has privilege to access.
func fetchSessions(
	ctx context.Context,
	tenant string,
	qc qclient.QueryClient,
) ([]*status.Session, error) {
	var nodes []string
	sysTenant := isSysTenant(tenant)
	clusterservice.GetMOCluster(qc.ServiceID()).GetCNService(clusterservice.NewSelectAll(),
		func(s metadata.CNService) bool {
			nodes = append(nodes, s.QueryAddress)
			return true
		})

	var retErr error
	var sessions []*status.Session

	genRequest := func() *query.Request {
		req := qc.NewRequest(query.CmdMethod_ShowProcessList)
		req.ShowProcessListRequest = &query.ShowProcessListRequest{
			Tenant:    tenant,
			SysTenant: sysTenant,
		}
		return req
	}

	handleValidResponse := func(nodeAddr string, rsp *query.Response) {
		if rsp != nil && rsp.ShowProcessListResponse != nil {
			for _, ss := range rsp.ShowProcessListResponse.Sessions {
				if sysTenant || strings.EqualFold(ss.Account, tenant) {
					sessions = append(sessions, ss)
				}
			}
		}
	}

	retErr = queryservice.RequestMultipleCn(ctx, nodes, qc, genRequest, handleValidResponse, nil)

	// Sort by session start time.
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].SessionStart.Before(sessions[j].SessionStart)
	})

	return sessions, retErr
}
