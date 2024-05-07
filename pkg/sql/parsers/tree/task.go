// Copyright 2021 -2023 Matrix Origin
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

type PauseDaemonTask struct {
	statementImpl
	TaskID uint64
}

func (node *PauseDaemonTask) Format(ctx *FmtCtx) {
	ctx.WriteString("pause daemon task")
	ctx.WriteByte(' ')
	ctx.WriteRune(rune(node.TaskID))
}

func (node *PauseDaemonTask) GetStatementType() string { return "Pause Daemon Task" }
func (node *PauseDaemonTask) GetQueryType() string     { return QueryTypeDML }

type CancelDaemonTask struct {
	statementImpl
	TaskID uint64
}

func (node *CancelDaemonTask) Format(ctx *FmtCtx) {
	ctx.WriteString("cancel daemon task")
	ctx.WriteByte(' ')
	ctx.WriteRune(rune(node.TaskID))
}

func (node *CancelDaemonTask) GetStatementType() string { return "Cancel Daemon Task" }
func (node *CancelDaemonTask) GetQueryType() string     { return QueryTypeDML }

type ResumeDaemonTask struct {
	statementImpl
	TaskID uint64
}

func (node *ResumeDaemonTask) Format(ctx *FmtCtx) {
	ctx.WriteString("resume daemon task")
	ctx.WriteByte(' ')
	ctx.WriteRune(rune(node.TaskID))
}

func (node *ResumeDaemonTask) GetStatementType() string { return "Resume Daemon Task" }
func (node *ResumeDaemonTask) GetQueryType() string     { return QueryTypeDML }
