// Copyright 2021 Matrix Origin
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

package tree

import (
	"strconv"
	"strings"
)

type SQLTaskSchedule struct {
	CronExpr string
	Timezone string
}

type CreateSQLTask struct {
	statementImpl
	IfNotExists   bool
	Name          Identifier
	CronExpr      string
	Timezone      string
	GateCondition string
	RetryLimit    int64
	Timeout       string
	SQLBody       string
}

func (node *CreateSQLTask) Format(ctx *FmtCtx) {
	ctx.WriteString("create task ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	ctx.WriteString(string(node.Name))
	if node.CronExpr != "" {
		ctx.WriteString(" schedule ")
		ctx.WriteString("'" + node.CronExpr + "'")
		if node.Timezone != "" {
			ctx.WriteString(" timezone ")
			ctx.WriteString("'" + node.Timezone + "'")
		}
	}
	if node.GateCondition != "" {
		ctx.WriteString(" when (")
		ctx.WriteString(node.GateCondition)
		ctx.WriteByte(')')
	}
	if node.RetryLimit > 0 {
		ctx.WriteString(" retry ")
		ctx.WriteString(strconv.FormatInt(node.RetryLimit, 10))
	}
	if node.Timeout != "" {
		ctx.WriteString(" timeout ")
		ctx.WriteString("'" + node.Timeout + "'")
	}
	ctx.WriteString(" as begin")
	if body := strings.TrimSpace(node.SQLBody); body != "" {
		ctx.WriteByte(' ')
		ctx.WriteString(body)
		if !strings.HasSuffix(body, ";") {
			ctx.WriteByte(';')
		}
		ctx.WriteByte(' ')
	} else {
		ctx.WriteByte(' ')
	}
	ctx.WriteString("end")
}

func (node *CreateSQLTask) GetStatementType() string { return "Create Task" }
func (node *CreateSQLTask) GetQueryType() string     { return QueryTypeDDL }

type AlterTaskAction int

const (
	AlterTaskSuspend AlterTaskAction = iota
	AlterTaskResume
	AlterTaskSetSchedule
	AlterTaskSetWhen
	AlterTaskSetRetry
	AlterTaskSetTimeout
)

type AlterSQLTask struct {
	statementImpl
	Name          Identifier
	Action        AlterTaskAction
	CronExpr      string
	Timezone      string
	GateCondition string
	RetryLimit    int64
	Timeout       string
}

func (node *AlterSQLTask) Format(ctx *FmtCtx) {
	ctx.WriteString("alter task ")
	ctx.WriteString(string(node.Name))
	switch node.Action {
	case AlterTaskSuspend:
		ctx.WriteString(" suspend")
	case AlterTaskResume:
		ctx.WriteString(" resume")
	case AlterTaskSetSchedule:
		ctx.WriteString(" set schedule ")
		ctx.WriteString("'" + node.CronExpr + "'")
		if node.Timezone != "" {
			ctx.WriteString(" timezone ")
			ctx.WriteString("'" + node.Timezone + "'")
		}
	case AlterTaskSetWhen:
		ctx.WriteString(" set when (")
		ctx.WriteString(node.GateCondition)
		ctx.WriteByte(')')
	case AlterTaskSetRetry:
		ctx.WriteString(" set retry ")
		ctx.WriteString(strconv.FormatInt(node.RetryLimit, 10))
	case AlterTaskSetTimeout:
		ctx.WriteString(" set timeout ")
		ctx.WriteString("'" + node.Timeout + "'")
	}
}

func (node *AlterSQLTask) GetStatementType() string { return "Alter Task" }
func (node *AlterSQLTask) GetQueryType() string     { return QueryTypeDDL }

type DropSQLTask struct {
	statementImpl
	IfExists bool
	Name     Identifier
}

func (node *DropSQLTask) Format(ctx *FmtCtx) {
	ctx.WriteString("drop task ")
	if node.IfExists {
		ctx.WriteString("if exists ")
	}
	ctx.WriteString(string(node.Name))
}

func (node *DropSQLTask) GetStatementType() string { return "Drop Task" }
func (node *DropSQLTask) GetQueryType() string     { return QueryTypeDDL }

type ExecuteSQLTask struct {
	statementImpl
	Name Identifier
}

func (node *ExecuteSQLTask) Format(ctx *FmtCtx) {
	ctx.WriteString("execute task ")
	ctx.WriteString(string(node.Name))
}

func (node *ExecuteSQLTask) GetStatementType() string { return "Execute Task" }
func (node *ExecuteSQLTask) GetQueryType() string     { return QueryTypeOth }

type ShowSQLTasks struct {
	showImpl
}

func (node *ShowSQLTasks) Format(ctx *FmtCtx) {
	ctx.WriteString("show tasks")
}

func (node *ShowSQLTasks) GetStatementType() string { return "Show Tasks" }
func (node *ShowSQLTasks) GetQueryType() string     { return QueryTypeOth }

type ShowSQLTaskRuns struct {
	showImpl
	TaskName Identifier
	HasTask  bool
	Limit    int64
	HasLimit bool
}

func (node *ShowSQLTaskRuns) Format(ctx *FmtCtx) {
	ctx.WriteString("show task runs")
	if node.HasTask {
		ctx.WriteString(" for ")
		ctx.WriteString(string(node.TaskName))
	}
	if node.HasLimit {
		ctx.WriteString(" limit ")
		ctx.WriteString(strconv.FormatInt(node.Limit, 10))
	}
}

func (node *ShowSQLTaskRuns) GetStatementType() string { return "Show Task Runs" }
func (node *ShowSQLTaskRuns) GetQueryType() string     { return QueryTypeOth }
