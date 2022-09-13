// Copyright 2022 Matrix Origin
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

package trace

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

var _ IBuffer2SqlItem = (*MOStatsInfo)(nil)
var _ CsvFields = (*MOStatsInfo)(nil)

type MOStatsInfo struct {
	StatementID [16]byte      `json:"statement_id"`
	Account     string        `json:"account"`
	Timestamp   util.TimeNano `json:"timestamp"`

	// Operator, desc op name
	Operator string `json:"operator"`
	// OperateObjectDesc，desc Operator obj_info/status/result
	OperateObjectDesc string `json:"operate_ojb"`
	// NodeId, index of query's node list
	NodeId int32
	// InputRows, number of rows accepted by node
	InputRows int64 `json:"input_rows"`
	// OutputRows, number of rows output by node
	OutputRows int64 `json:"output_rows"`
	// TimeConsumed, time taken by the node in milliseconds
	TimeConsumed int64 `json:"time_consumed"`
	// InputSize, data size accepted by node
	InputSize int64 `json:"input_size"`
	// OutputSize, data size output by node
	OutputSize int64 `json:"output_size"`
	// MemorySize, memory alloc by node
	MemorySize int64 `json:"memory_size"`
}

func (s MOStatsInfo) GetName() string {
	return MOStatsType
}

func (s MOStatsInfo) Size() int64 {
	return 64
}

func (s MOStatsInfo) Free() {}

func (s MOStatsInfo) CsvOptions() *CsvOptions {
	return CommonCsvOptions
}

func (s MOStatsInfo) CsvFields() []string {
	var result []string
	result = append(result, uuid.UUID(s.StatementID).String())
	result = append(result, s.Account)
	result = append(result, nanoSec2DatetimeString(s.Timestamp))
	result = append(result, GetNodeResource().NodeUuid)
	result = append(result, GetNodeResource().NodeType)
	result = append(result, s.Operator)
	result = append(result, s.OperateObjectDesc)
	result = append(result, fmt.Sprintf("%d", s.NodeId))
	result = append(result, fmt.Sprintf("%d", s.InputRows))
	result = append(result, fmt.Sprintf("%d", s.OutputRows))
	result = append(result, fmt.Sprintf("%d", s.TimeConsumed))
	result = append(result, fmt.Sprintf("%d", s.InputSize))
	result = append(result, fmt.Sprintf("%d", s.OutputSize))
	result = append(result, fmt.Sprintf("%d", s.MemorySize))
	return result
}

func ReportStats(ctx context.Context, s *MOStatsInfo) error {
	if !GetTracerProvider().IsEnable() {
		return nil
	}
	return export.GetGlobalBatchProcessor().Collect(ctx, s)
}

func genStatsBatchSql(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		logutil.Debugf("genStatsBatchSql empty")
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", StatsDatabase, statsTbl))
	buf.WriteString("(")
	buf.WriteString("`statement_id`")
	buf.WriteString(", `tenant_id`")
	buf.WriteString(", `timestamp`")
	buf.WriteString(", `node_uuid`")
	buf.WriteString(", `node_type`")
	buf.WriteString(", `operator`")
	buf.WriteString(", `operate_object`")
	buf.WriteString(", `node_id`")
	buf.WriteString(", `input_rows`")
	buf.WriteString(", `output_rows`")
	buf.WriteString(", `time_consumed`")
	buf.WriteString(", `input_size`")
	buf.WriteString(", `output_size`")
	buf.WriteString(", `memory_size`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	for _, item := range in {
		s, ok := item.(*MOStatsInfo)
		if !ok {
			panic("Not MOStatsInfo")
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf(`%q`, uuid.UUID(s.StatementID).String()))
		buf.WriteString(fmt.Sprintf(`, %q`, s.Account))
		buf.WriteString(fmt.Sprintf(`, %q`, nanoSec2DatetimeString(s.Timestamp)))
		buf.WriteString(fmt.Sprintf(`, %q`, moNode.NodeUuid))
		buf.WriteString(fmt.Sprintf(`, %q`, moNode.NodeType))
		buf.WriteString(fmt.Sprintf(`, %q`, s.Operator))
		buf.WriteString(fmt.Sprintf(`, %q`, s.OperateObjectDesc))
		buf.WriteString(fmt.Sprintf(", %d", s.NodeId))
		buf.WriteString(fmt.Sprintf(", %d", s.InputRows))
		buf.WriteString(fmt.Sprintf(", %d", s.OutputRows))
		buf.WriteString(fmt.Sprintf(", %d", s.TimeConsumed))
		buf.WriteString(fmt.Sprintf(", %d", s.InputSize))
		buf.WriteString(fmt.Sprintf(", %d", s.OutputSize))
		buf.WriteString(fmt.Sprintf(", %d", s.MemorySize))
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}
