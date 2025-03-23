// Copyright 2023 Matrix Origin
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
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

type CreateTaskRequestOptions struct {
	TaskName     string
	TaskId       string
	AccountInfo  *TenantInfo
	Exclude      string
	StartTs      string
	EndTs        string
	MaxSqlLength int64
	PitrTables   string // json encoded pitr tables: cdc2.PatternTuples
	SrcUri       string // json encoded source uri: cdc2.UriInfo
	SrcUriInfo   cdc.UriInfo
	ExtraOpts    string // json encoded extra opts: map[string]any
	SinkType     string
	NoFull       bool
	ConfigFile   string

	// control options
	UseConsole bool
}

func (opts *CreateTaskRequestOptions) Reset() {
	opts.Exclude = ""
	opts.StartTs = ""
	opts.EndTs = ""
	opts.MaxSqlLength = 0
	opts.SrcUri = ""
	opts.ExtraOpts = ""
	opts.PitrTables = ""
	opts.SinkType = ""
	opts.NoFull = false
	opts.UseConsole = true
	opts.ConfigFile = ""
	opts.SrcUriInfo = cdc.UriInfo{}
	opts.AccountInfo = nil
}

func (opts *CreateTaskRequestOptions) ValidateAndFill(
	ctx context.Context,
	ses *Session,
	req *CreateTaskRequest,
) (err error) {
	opts.Reset()
	taskId := NewTaskId()
	opts.TaskName = req.TaskName.String()
	opts.TaskId = taskId.String()
	opts.AccountInfo = ses.GetTenantInfo()

	tmpOpts := make(map[string]string, len(req.Option)/2)
	for i := 0; i < len(req.Option)-1; i += 2 {
		key := req.Option[i]
		value := req.Option[i+1]
		tmpOpts[key] = value
	}

	// extract source uri and check connection
	// target field: SrcUri
	{
		if opts.SrcUri, opts.SrcUriInfo, err = extractUriInfo(
			ctx, req.SourceUri, cdc.SourceUriPrefix,
		); err != nil {
			return
		}
		if _, err = cdc.OpenDbConn(
			opts.SrcUriInfo.User, opts.SrcUriInfo.Password, opts.SrcUriInfo.Ip, opts.SrcUriInfo.Port, cdc.CDCDefaultSendSqlTimeout,
		); err != nil {
			err = moerr.NewInternalErrorf(ctx, "failed to connect to source, please check the connection, err: %v", err)
			return
		}
	}

	// 1. Converts sink type to lowercase
	// 2. Enables console sink if configured and requested
	// 3. For non-console sinks, validates that only mysql or matrixone sinks are used
	// 4. Returns error for unsupported sink types
	// target field: SinkType, UseConsole
	{
		opts.SinkType = strings.ToLower(req.SinkType)
		if cdc.EnableConsoleSink && opts.SinkType == cdc.CDCSinkType_Console {
			opts.UseConsole = true
		}
		if !opts.UseConsole && opts.SinkType != cdc.CDCSinkType_MySQL && opts.SinkType != cdc.CDCSinkType_MO {
			err = moerr.NewInternalErrorf(ctx, "unsupported sink type: %s", req.SinkType)
			return
		}
	}

	var (
		startTs, endTs time.Time
		extraOpts      = make(map[string]any)
	)

	for _, key := range cdc.CDCRequestOptions {
		value := tmpOpts[key]
		switch key {
		case cdc.CDCRequestOptions_NoFull:
			opts.NoFull, _ = strconv.ParseBool(value)
		case cdc.CDCRequestOptions_Level:
			if err = opts.handleLevel(ctx, ses, req, value); err != nil {
				return
			}
		case cdc.CDCRequestOptions_Exclude:
			if _, err = regexp.Compile(value); err != nil {
				err = moerr.NewInternalErrorf(ctx, "invalid exclude: %s, err: %v", value, err)
				return
			}
			opts.Exclude = strings.ReplaceAll(value, "\\", "\\\\")
		case cdc.CDCRequestOptions_StartTs:
			if value != "" {
				if startTs, err = parseTimestamp(value, ses.timeZone); err != nil {
					err = moerr.NewInternalErrorf(ctx, "invalid startTs: %s, supported timestamp format: `%s`, or `%s`", value, time.DateTime, time.RFC3339)
					return
				}
				opts.StartTs = startTs.Format(time.RFC3339)
			}
		case cdc.CDCRequestOptions_EndTs:
			if value != "" {
				if endTs, err = parseTimestamp(value, ses.timeZone); err != nil {
					err = moerr.NewInternalErrorf(ctx, "invalid endTs: %s, supported timestamp format: `%s`, or `%s`", value, time.DateTime, time.RFC3339)
					return
				}
				opts.EndTs = endTs.Format(time.RFC3339)
			}
		case cdc.CDCRequestOptions_MaxSqlLength:
			if value != "" {
				var maxSqlLength int64
				if maxSqlLength, err = strconv.ParseInt(value, 10, 64); err != nil {
					err = moerr.NewInternalErrorf(ctx, "invalid maxSqlLength: %s", value)
					return
				}
				extraOpts[cdc.CDCTaskExtraOptions_MaxSqlLength] = maxSqlLength
			}
		case cdc.CDCRequestOptions_InitSnapshotSplitTxn:
			if value == "false" {
				extraOpts[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn] = false
			}
		case cdc.CDCRequestOptions_SendSqlTimeout:
			if value != "" {
				if _, err = time.ParseDuration(value); err != nil {
					err = moerr.NewInternalErrorf(ctx, "invalid sendSqlTimeout: %s", value)
					return
				}
				extraOpts[cdc.CDCTaskExtraOptions_SendSqlTimeout] = value
			}
		case cdc.CDCRequestOptions_ConfigFile:
			if value != "" {
				opts.ConfigFile = value
			}
		}
	}

	if !startTs.IsZero() && !endTs.IsZero() && !endTs.After(startTs) {
		err = moerr.NewInternalErrorf(ctx, "startTs: %s should be less than endTs: %s", startTs.Format(time.RFC3339), endTs.Format(time.RFC3339))
		return
	}

	// fill default value for additional opts
	if _, ok := extraOpts[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn]; !ok {
		extraOpts[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn] = cdc.CDCDefaultTaskExtra_InitSnapshotSplitTxn
	}
	if _, ok := extraOpts[cdc.CDCTaskExtraOptions_SendSqlTimeout]; !ok {
		extraOpts[cdc.CDCTaskExtraOptions_SendSqlTimeout] = cdc.CDCDefaultSendSqlTimeout
	}
	if _, ok := extraOpts[cdc.CDCTaskExtraOptions_MaxSqlLength]; !ok {
		extraOpts[cdc.CDCTaskExtraOptions_MaxSqlLength] = cdc.CDCDefaultTaskExtra_MaxSQLLen
	}

	var extraOptsBytes []byte
	if extraOptsBytes, err = json.Marshal(extraOpts); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to marshal extra opts: %v", err)
		return
	}
	opts.ExtraOpts = string(extraOptsBytes)

	return
}

func (opts *CreateTaskRequestOptions) BuildTaskMetadata() task.TaskMetadata {
	return task.TaskMetadata{
		ID:       opts.TaskId,
		Executor: task.TaskCode_InitCdc,
		Options: task.TaskOptions{
			MaxRetryTimes: defaultConnectorTaskMaxRetryTimes,
			RetryInterval: defaultConnectorTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

func (opts *CreateTaskRequestOptions) BuildTaskDetails() (details *task.Details, err error) {
	accountInfo := opts.AccountInfo
	accountId := accountInfo.GetTenantID()
	accountName := accountInfo.GetTenant()
	details = &task.Details{
		AccountID: accountId,
		Account:   accountName,
		Username:  accountInfo.GetUser(),
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{
				TaskName: opts.TaskName,
				TaskId:   opts.TaskId,
				Accounts: []*task.Account{
					{
						Id:   uint64(accountId),
						Name: accountName,
					},
				},
			},
		},
	}
	return
}

func (opts *CreateTaskRequestOptions) ToInsertTaskSQL(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	service string,
) (sql string, err error) {
	var encodedSinkPwd string
	if !opts.UseConsole {
		if err = initAesKeyBySqlExecutor(
			ctx, tx, catalog.System_Account, service,
		); err != nil {
			return
		}
		if encodedSinkPwd, err = opts.SrcUriInfo.GetEncodedPassword(); err != nil {
			return
		}
	}

	sql = CDCSQLBuilder.InsertTaskSQL(
		uint64(opts.AccountInfo.GetTenantID()),
		opts.TaskId,
		opts.TaskName,
		opts.SrcUri,
		"",
		encodedSinkPwd,
		opts.SinkType,
		encodedSinkPwd,
		"",
		"",
		"",
		opts.PitrTables,
		opts.Exclude,
		"",
		cdc.SASCommon,
		cdc.SASCommon,
		opts.StartTs,
		opts.EndTs,
		opts.ConfigFile,
		time.Now().UTC(),
		CdcRunning,
		0,
		opts.NoFull,
		"",
		opts.ExtraOpts,
	)
	return
}

// handleLevel validates the CDC task level and processes the pattern tuples for PITR.
// It checks if the level is valid (account/db/table level), gets pattern tuples based on the level,
// verifies PITR configuration, and encodes the pattern tuples as JSON.
func (opts *CreateTaskRequestOptions) handleLevel(
	ctx context.Context,
	ses *Session,
	req *CreateTaskRequest,
	level string,
) (err error) {
	if level != cdc.CDCPitrGranularity_Account && level != cdc.CDCPitrGranularity_DB && level != cdc.CDCPitrGranularity_Table {
		err = moerr.NewInternalErrorf(ctx, "invalid level: %s", level)
		return
	}
	var patterTupples *cdc.PatternTuples
	if patterTupples, err = getPatternTuples(
		ctx, level, req.Tables,
	); err != nil {
		err = moerr.NewInternalErrorf(ctx, "invalid level: %s", level)
		return
	}
	if err = WithBackgroundExec(
		ctx,
		ses,
		func(ctx context.Context, ses *Session, bh BackgroundExec) error {
			return checkPitr(ctx, bh, ses.GetTenantName(), patterTupples)
		},
	); err != nil {
		return
	}
	opts.PitrTables, err = cdc.JsonEncode(patterTupples)
	return
}
