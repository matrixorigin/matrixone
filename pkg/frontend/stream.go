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

package frontend

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	moconnector "github.com/matrixorigin/matrixone/pkg/stream/connector"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

func (mce *MysqlCmdExecutor) handleCreateStream(ctx context.Context, st *tree.CreateConnector) error {
	ts := mce.routineMgr.getParameterUnit().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx, "no task service is found")
	}
	dbName := string(st.TableName.Schema())
	tableName := string(st.TableName.Name())
	_, tableDef := mce.ses.GetTxnCompileCtx().Resolve(dbName, tableName)
	if tableDef == nil {
		return moerr.NewNoSuchTable(ctx, dbName, tableName)
	}
	options := make(map[string]string)
	for _, opt := range st.Options {
		options[string(opt.Key)] = opt.Val.String()
	}
	if err := createConnector(
		ctx,
		mce.ses.GetTenantInfo().TenantID,
		mce.ses.GetTenantName(),
		mce.ses.GetUserName(),
		ts,
		dbName+"."+tableName,
		options,
	); err != nil {
		return err
	}
	return nil
}

func createStreamPipeline(
	ctx context.Context,
	accountID uint32,
	account string,
	username string,
	ts taskservice.TaskService,
	tableName string,
	rawOpts map[string]string,
) error {
	options, err := moconnector.MakeStmtOpts(ctx, rawOpts)
	if err != nil {
		return err
	}
	tasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskType(taskservice.EQ,
			pb.TaskType_TypeKafkaSinkConnector.String()),
		taskservice.WithAccountID(taskservice.EQ,
			accountID),
	)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		dc, ok := t.Details.Details.(*pb.Details_Connector)
		if !ok {
			return moerr.NewInternalError(ctx, fmt.Sprintf("invalid task type %s",
				t.TaskType.String()))
		}
		if dc.Connector.TableName == tableName && duplicate(t, options) {
			return moerr.NewErrDuplicateConnector(ctx, tableName)
		}
	}
	details := &pb.Details{
		AccountID: accountID,
		Account:   account,
		Username:  username,
		Details: &pb.Details_Connector{
			Connector: &pb.ConnectorDetails{
				TableName: tableName,
				Options:   options,
			},
		},
	}
	if err := ts.CreateDaemonTask(ctx, connectorTaskMetadata(), details); err != nil {
		return err
	}
	return nil
}

func (mce *MysqlCmdExecutor) handleDropStreamPipeline(ctx context.Context, st *tree.DropConnector) error {
	//todo: handle drop connector
	return nil
}

func (mce *MysqlCmdExecutor) handleShowStreamPipeline(ctx context.Context, cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	if err := showConnectors(ses); err != nil {
		return err
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)
	if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
		return moerr.NewInternalError(ses.requestCtx, "routine send response failed, error: %v ", err)
	}
	return err
}
