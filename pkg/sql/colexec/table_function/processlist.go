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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/pkg/errors"
)

func processlistPrepare(proc *process.Process, arg *Argument) error {
	if len(arg.Args) > 0 {
		return moerr.NewInvalidInput(proc.Ctx, "processlist: no argument is required")
	}
	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return nil
}

func processlist(_ int, proc *process.Process, arg *Argument) (bool, error) {
	sessions, err := fetchSessions(proc.Ctx, proc.SessionInfo.Account,
		proc.SessionInfo.GetUser(), proc.QueryService)
	if err != nil {
		return false, err
	}
	bat := batch.NewWithSize(len(arg.Attrs))
	for i, a := range arg.Attrs {
		idx, ok := status.SessionField_value[a]
		if !ok {
			return false, moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", a)
		}

		tp := plan2.SessionsColTypes[idx]
		bat.Vecs[i] = vector.NewVec(tp)
	}
	bat.Attrs = arg.Attrs

	mp := proc.GetMPool()
	for _, session := range sessions {
		for i, col := range arg.Attrs {
			switch status.SessionField(status.SessionField_value[col]) {
			case status.SessionField_NODE_ID:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.NodeID), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_CONN_ID:
				if err := vector.AppendFixed(bat.Vecs[i], session.ConnID, false, mp); err != nil {
					return false, err
				}
			case status.SessionField_SESSION_ID:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.SessionID), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_ACCOUNT:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.Account), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_USER:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.User), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_HOST:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.Host), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_DB:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.DB), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_SESSION_START:
				if err := vector.AppendBytes(bat.Vecs[i],
					[]byte(session.SessionStart.Format("2006-01-02 15:04:05.000000")),
					false, mp); err != nil {
					return false, err
				}
			case status.SessionField_COMMAND:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.Command), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_INFO:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.Info), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_TXN_ID:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.TxnID), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_STATEMENT_ID:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.StatementID), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_STATEMENT_TYPE:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.StatementType), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_QUERY_TYPE:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.QueryType), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_SQL_SOURCE_TYPE:
				if err := vector.AppendBytes(bat.Vecs[i], []byte(session.SQLSourceType), false, mp); err != nil {
					return false, err
				}
			case status.SessionField_QUERY_START:
				var queryStart string
				if !session.QueryStart.Equal(time.Time{}) {
					queryStart = session.QueryStart.Format("2006-01-02 15:04:05.000000")
				}
				if err := vector.AppendBytes(bat.Vecs[i], []byte(queryStart),
					false, mp); err != nil {
					return false, err
				}
			}
		}
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	proc.SetInputBatch(bat)
	return true, nil
}

// isSysTenant return true if the tenant is sys.
func isSysTenant(tenant string) bool {
	return strings.ToLower(tenant) == "sys"
}

// fetchSessions get sessions all nodes which the tenant has privilege to access.
func fetchSessions(ctx context.Context, tenant string, user string, qs queryservice.QueryService) ([]*status.Session, error) {
	var nodes []string
	labels := clusterservice.NewSelector().SelectByLabel(
		map[string]string{"account": tenant}, clusterservice.EQ)
	sysTenant := isSysTenant(tenant)
	if sysTenant {
		disttae.SelectForSuperTenant(clusterservice.NewSelector(), user, nil,
			func(s *metadata.CNService) {
				nodes = append(nodes, s.QueryAddress)
			})
	} else {
		disttae.SelectForCommonTenant(labels, nil, func(s *metadata.CNService) {
			nodes = append(nodes, s.QueryAddress)
		})
	}
	nodesLeft := len(nodes)

	type nodeResponse struct {
		nodeAddr string
		response interface{}
		err      error
	}
	responseChan := make(chan nodeResponse, nodesLeft)

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	var retErr error
	var sessions []*status.Session
	for _, node := range nodes {
		// Invalid node address, ignore it.
		if len(node) == 0 {
			nodesLeft--
			continue
		}

		go func(addr string) {
			req := qs.NewRequest(query.CmdMethod_ShowProcessList)
			req.ShowProcessListRequest = &query.ShowProcessListRequest{
				Tenant:    tenant,
				SysTenant: sysTenant,
			}
			resp, err := qs.SendMessage(ctx, addr, req)
			responseChan <- nodeResponse{nodeAddr: addr, response: resp, err: err}
		}(node)
	}

	// Wait for all responses.
	for nodesLeft > 0 {
		select {
		case res := <-responseChan:
			if res.err != nil && retErr != nil {
				retErr = errors.Wrapf(res.err, "failed to get result from %s", res.nodeAddr)
			} else {
				queryResp, ok := res.response.(*query.Response)
				if ok && queryResp.ShowProcessListResponse != nil {
					sessions = append(sessions, queryResp.ShowProcessListResponse.Sessions...)
				}
			}
		case <-ctx.Done():
			retErr = moerr.NewInternalError(ctx, "context deadline exceeded")
		}
		nodesLeft--
	}

	// Sort by session start time.
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].SessionStart.Before(sessions[j].SessionStart)
	})

	return sessions, retErr
}
