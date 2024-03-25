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
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	moconnector "github.com/matrixorigin/matrixone/pkg/stream/connector"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

const (
	defaultConnectorTaskMaxRetryTimes = 10
	defaultConnectorTaskRetryInterval = int64(time.Second * 10)
)

func handleCreateConnector(ctx context.Context, ses *Session, st *tree.CreateConnector) error {
	ts := gPu.TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx, "no task service is found")
	}
	dbName := string(st.TableName.Schema())
	tableName := string(st.TableName.Name())
	_, tableDef := ses.GetTxnCompileCtx().Resolve(dbName, tableName)
	if tableDef == nil {
		return moerr.NewNoSuchTable(ctx, dbName, tableName)
	}
	options := make(map[string]string)
	for _, opt := range st.Options {
		options[string(opt.Key)] = opt.Val.String()
	}
	if err := createConnector(
		ctx,
		ses.GetTenantInfo().TenantID,
		ses.GetTenantName(),
		ses.GetUserName(),
		ts,
		dbName+"."+tableName,
		options,
		false,
	); err != nil {
		return err
	}
	return nil
}

func connectorTaskMetadata() pb.TaskMetadata {
	return pb.TaskMetadata{
		ID:       "-",
		Executor: pb.TaskCode_ConnectorKafkaSink,
		Options: pb.TaskOptions{
			MaxRetryTimes: defaultConnectorTaskMaxRetryTimes,
			RetryInterval: defaultConnectorTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

func isSameValue(a, b map[string]string, field string) bool {
	v1, ok := a[field]
	if !ok {
		return false
	}
	v2, ok := b[field]
	if !ok {
		return false
	}
	return strings.EqualFold(v1, v2)
}

func duplicate(t pb.DaemonTask, options map[string]string) bool {
	if t.TaskStatus == pb.TaskStatus_Canceled {
		return false
	}
	dup := true
	switch d := t.Details.Details.(type) {
	case *pb.Details_Connector:
		checkFields := []string{
			moconnector.OptConnectorType,
			moconnector.OptConnectorTopic,
			moconnector.OptConnectorServers,
		}
		for _, field := range checkFields {
			dup = dup && isSameValue(d.Connector.Options, options, field)
		}
	}
	return dup
}

func createConnector(
	ctx context.Context,
	accountID uint32,
	account string,
	username string,
	ts taskservice.TaskService,
	tableName string,
	rawOpts map[string]string,
	ifNotExists bool,
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
			// do not return error if ifNotExists is true since the table is not actually created
			if ifNotExists {
				return nil
			}
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

func handleDropConnector(ctx context.Context, ses *Session, st *tree.DropConnector) error {
	//todo: handle Create connector
	return nil
}

func handleShowConnectors(ctx context.Context, ses *Session, isLastStmt bool) error {
	var err error
	if err := showConnectors(ses); err != nil {
		return err
	}
	return err
}

var connectorCols = []Column{
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "task_id",
			columnType: defines.MYSQL_TYPE_LONG,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "task_type",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "task_runner",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "task_status",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "table_name",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "options",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "last_heartbeat",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "created_at",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "updated_at",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "end_at",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "last_run",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "error",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
}

func showConnectors(ses FeSession) error {
	ts := gPu.TaskService
	if ts == nil {
		return moerr.NewInternalError(ses.GetRequestContext(),
			"task service not ready yet, please try again later.")
	}
	tasks, err := ts.QueryDaemonTask(ses.GetRequestContext(),
		taskservice.WithTaskType(taskservice.EQ,
			pb.TaskType_TypeKafkaSinkConnector.String()),
		taskservice.WithAccountID(taskservice.EQ,
			ses.GetAccountId()),
	)
	if err != nil {
		return err
	}
	mrs := ses.GetMysqlResultSet()
	for _, col := range connectorCols {
		mrs.AddColumn(col)
	}
	for _, t := range tasks {
		row := make([]interface{}, 12)
		row[0] = t.ID
		row[1] = t.TaskType.String()
		row[2] = t.TaskRunner
		row[3] = t.TaskStatus.String()
		details := t.Details.Details.(*pb.Details_Connector)
		row[4] = details.Connector.TableName
		row[5] = optionString(details.Connector.Options)
		if t.LastHeartbeat.IsZero() {
			row[6] = ""
		} else {
			row[6] = t.LastHeartbeat.String()
		}
		row[7] = t.CreateAt.String()
		row[8] = t.UpdateAt.String()
		if t.EndAt.IsZero() {
			row[9] = ""
		} else {
			row[9] = t.EndAt.String()
		}
		if t.LastRun.IsZero() {
			row[10] = ""
		} else {
			row[10] = t.LastRun.String()
		}
		row[11] = t.Details.Error
		mrs.AddRow(row)
	}
	return nil
}

func optionString(options map[string]string) string {
	items := make([]string, 0, len(options))
	for key, value := range options {
		items = append(items, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}
