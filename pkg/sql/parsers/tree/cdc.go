// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[DropCDC](
		func() *DropCDC { return &DropCDC{} },
		func(d *DropCDC) { d.reset() },
		reuse.DefaultOptions[DropCDC](), //.
	) //WithEnableChecker()
}

type CreateCDCOption struct {
	StartTs                string
	EndTs                  string
	NoFull                 bool
	FullConcurrency        int
	IncrementalConcurrency int
	FullTaskRetry          string
	IncrementalTaskRetry   string
	FullDDLRetry           int
	FullDMLRetry           int
	IncrementalDDLRetry    int
	IncrementalDMLRetry    int
	ConfigFile             string
}

type CreateCDC struct {
	statementImpl
	IfNotExists bool
	TaskName    string
	SourceUri   string
	SinkType    string
	SinkUri     string
	Tables      string
	Option      []string
}

func (node *CreateCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("create cdc ")
	ctx.WriteString(fmt.Sprintf("'%s' ", node.TaskName))
	ctx.WriteString(fmt.Sprintf("'%s' ", node.SourceUri))
	ctx.WriteString(fmt.Sprintf("'%s' ", node.SinkType))
	ctx.WriteString(fmt.Sprintf("'%s' ", node.SinkUri))
	ctx.WriteString(fmt.Sprintf("'%s' ", node.Tables))
	ctx.WriteString("{ ")
	for i := 0; i < len(node.Option)-1; i += 2 {
		ctx.WriteString(fmt.Sprintf("\"%s\"='%s',", node.Option[i], node.Option[i+1]))
	}
	ctx.WriteString("}")
	ctx.WriteByte(';')
}

func (node *CreateCDC) GetStatementType() string { return "Create CDC" }

func (node *CreateCDC) GetQueryType() string { return QueryTypeOth }

func (node CreateCDC) TypeName() string { return "tree.CreateCDC" }

func (node *DropCDC) Free() {
	reuse.Free[DropCDC](node, nil)
}

type AllOrNotCDC struct {
	TaskName string
	All      bool
}
type ShowCDC struct {
	statementImpl
	SourceUri string
	Option    *AllOrNotCDC
}

func (node *ShowCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("show cdc ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.SourceUri))
	if node.Option.All {
		ctx.WriteString(" all")
	} else {
		ctx.WriteString(" task ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Option.TaskName))
	}
	ctx.WriteByte(';')
}

func (node *ShowCDC) GetStatementType() string { return "Show CDC" }

func (node *ShowCDC) GetQueryType() string { return QueryTypeOth }

func (node ShowCDC) TypeName() string { return "tree.ShowCDC" }

type PauseCDC struct {
	statementImpl
	SourceUri string
	Option    *AllOrNotCDC
}

func (node *PauseCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("pause cdc ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.SourceUri))
	if node.Option.All {
		ctx.WriteString(" all")
	} else {
		ctx.WriteString(" task ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Option.TaskName))
	}
	ctx.WriteByte(';')
}

func (node *PauseCDC) GetStatementType() string { return "Pause CDC" }

func (node *PauseCDC) GetQueryType() string { return QueryTypeOth }

func (node PauseCDC) TypeName() string { return "tree.PauseCDC" }

type DropCDC struct {
	statementImpl
	SourceUri string
	Option    *AllOrNotCDC
}

func NewDropCDC(sourceUri string, option *AllOrNotCDC) *DropCDC {
	drop := reuse.Alloc[DropCDC](nil)
	drop.SourceUri = sourceUri
	drop.Option = option
	return drop
}

func (node *DropCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("drop cdc ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.SourceUri))
	if node.Option.All {
		ctx.WriteString(" all")
	} else {
		ctx.WriteString(" task ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Option.TaskName))
	}
	ctx.WriteByte(';')
}

func (node *DropCDC) GetStatementType() string { return "Drop CDC" }

func (node *DropCDC) GetQueryType() string { return QueryTypeOth }

func (node DropCDC) TypeName() string { return "tree.DropCDC" }

func (node *DropCDC) reset() {
	if node.Option != nil {
		node.Option = nil
	}
	*node = DropCDC{}
}

type ResumeCDC struct {
	statementImpl
	SourceUri string
	TaskName  string
}

func (node *ResumeCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("resume cdc ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.SourceUri))
	ctx.WriteString(" task ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.TaskName))
	ctx.WriteByte(';')
}

func (node *ResumeCDC) GetStatementType() string { return "Resume CDC" }

func (node *ResumeCDC) GetQueryType() string { return QueryTypeOth }

func (node ResumeCDC) TypeName() string { return "tree.ResumeCDC" }

type RestartCDC struct {
	statementImpl
	SourceUri string
	TaskName  string
}

func (node *RestartCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("resume cdc ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.SourceUri))
	ctx.WriteString(" task ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.TaskName))
	ctx.WriteString(" 'restart'")
	ctx.WriteByte(';')
}

func (node *RestartCDC) GetStatementType() string { return "Restart CDC" }

func (node *RestartCDC) GetQueryType() string { return QueryTypeOth }

func (node RestartCDC) TypeName() string { return "tree.RestartCDC" }
