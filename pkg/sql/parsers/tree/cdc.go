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
	"encoding/json"
	"fmt"
	"strconv"
)

//	func init() {
//		reuse.CreatePool[BackupStart](
//			func() *BackupStart { return &BackupStart{} },
//			func(b *BackupStart) { b.reset() },
//			reuse.DefaultOptions[BackupStart](), //.
//		) //WithEnableChecker()
//	}

type FilterConfig struct {
	Rules            []string `mapstructure:"rules"`
	OperationFilters []struct {
		Matcher         []string `mapstructure:"matcher"`
		IgnoreOperation []string `mapstructure:"ignore-operation"`
	} `mapstructure:"operation-filters"`
}

type CreateCDCOption struct {
	StartTs                string
	EndTs                  string
	Full                   bool
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
	Option      *CreateCDCOption
	Filter      *FilterConfig
}

func (node *CreateCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("create cdc ")
	ctx.WriteString(fmt.Sprintf("'%s' ", node.TaskName))
	ctx.WriteString(fmt.Sprintf("'%s' ", node.SourceUri))
	ctx.WriteString(fmt.Sprintf("'%s' ", node.SinkType))
	ctx.WriteString(fmt.Sprintf("'%s' ", node.SinkUri))
	ctx.WriteString(fmt.Sprintf("'%s' ", node.Tables))
	ctx.WriteString(fmt.Sprintf("\"StartTS\"='%s',", node.Option.StartTs))
	ctx.WriteString(fmt.Sprintf("\"EndTS\"='%s',", node.Option.EndTs))
	ctx.WriteString(fmt.Sprintf("\"Full\"='%t',", node.Option.Full))
	ctx.WriteString(fmt.Sprintf("\"FullConcurrency\"='%d',", node.Option.FullConcurrency))
	ctx.WriteString(fmt.Sprintf("\"IncrementalConcurrency\"='%d',", node.Option.IncrementalConcurrency))
	ctx.WriteString(fmt.Sprintf("\"ConfigFile\"='%s',", node.Option.ConfigFile))
	ctx.WriteString(fmt.Sprintf("\"FullTaskRetry\"='%s',", node.Option.FullTaskRetry))
	ctx.WriteString(fmt.Sprintf("\"IncrementalTaskRetry\"='%s',", node.Option.IncrementalTaskRetry))
	ctx.WriteString(fmt.Sprintf("\"FullDDLRetry\"='%d',", node.Option.FullDDLRetry))
	ctx.WriteString(fmt.Sprintf("\"FullDMLRetry\"='%d',", node.Option.FullDMLRetry))
	ctx.WriteString(fmt.Sprintf("\"IncrementalDDLRetry\"='%d',", node.Option.IncrementalDDLRetry))
	ctx.WriteString(fmt.Sprintf("\"IncrementalDMLRetry\"='%d',", node.Option.IncrementalDMLRetry))
	ctx.WriteString(fmt.Sprintf("\"Filter\"='%s',", node.Filter))
	ctx.WriteByte(';')
}

func NewCreateCDC(IfNotExists bool, TaskName string, SourceUri string, SinkType string, SinkUri string, Tables string, Options []string) *CreateCDC {
	node := &CreateCDC{
		IfNotExists: IfNotExists,
		TaskName:    TaskName,
		SourceUri:   SourceUri,
		SinkType:    SinkType,
		SinkUri:     SinkUri,
		Tables:      Tables,
	}
	createCDCOpt := CreateCDCOption{}
	for op := 0; op < len(Options)-1; {
		switch Options[op] {
		case "StartTS":
			createCDCOpt.StartTs = Options[op+1]
		case "EndTs":
			createCDCOpt.EndTs = Options[op+1]
		case "Full":
			createCDCOpt.Full = Options[op+1] == "true"
		case "FullConcurrency":
			createCDCOpt.FullConcurrency, _ = strconv.Atoi(Options[op+1])
		case "IncrementalConcurrency":
			createCDCOpt.IncrementalConcurrency, _ = strconv.Atoi(Options[op+1])
		case "ConfigFile":
			createCDCOpt.ConfigFile = Options[op+1]
		case "FullTaskRetry":
			createCDCOpt.FullTaskRetry = Options[op+1]
		case "IncrementalTaskRetry":
			createCDCOpt.IncrementalTaskRetry = Options[op+1]
		case "FullDDLRetry":
			createCDCOpt.FullDDLRetry, _ = strconv.Atoi(Options[op+1])
		case "FullDMLRetry":
			createCDCOpt.FullDMLRetry, _ = strconv.Atoi(Options[op+1])
		case "IncrementalDDLRetry":
			createCDCOpt.IncrementalDDLRetry, _ = strconv.Atoi(Options[op+1])
		case "IncrementalDMLRetry":
			createCDCOpt.IncrementalDMLRetry, _ = strconv.Atoi(Options[op+1])
		case "Filter":
			filterConfig := FilterConfig{}
			_ = json.Unmarshal([]byte(Options[op+1]), &filterConfig)
			node.Filter = &filterConfig
		}
		op += 2
	}
	node.Option = &createCDCOpt
	return node
}

func (node *CreateCDC) GetStatementType() string { return "Create CDC" }

func (node *CreateCDC) GetQueryType() string { return QueryTypeOth }

func (node CreateCDC) TypeName() string { return "tree.CreateCDC" }

func (node *CreateCDC) reset() {
	if node.Option != nil {
		node.Option = nil
	}
	if node.Filter != nil {
		node.Filter = nil
	}
	*node = CreateCDC{}
}

//	func (node *CreateCDC) Free() {
//		reuse.Free[BackupStart](node, nil)
//	}
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
		ctx.WriteString("all")
	} else {
		ctx.WriteString(" taskname ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Option.TaskName))
	}
	ctx.WriteByte(';')
}

func (node *ShowCDC) GetStatementType() string { return "Show CDC" }

func (node *ShowCDC) GetQueryType() string { return QueryTypeOth }

func (node ShowCDC) TypeName() string { return "tree.ShowCDC" }

func (node *ShowCDC) reset() {
	if node.Option != nil {
		node.Option = nil
	}
	*node = ShowCDC{}
}

type PauseCDC struct {
	statementImpl
	SourceUri string
	Option    *AllOrNotCDC
}

func (node *PauseCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("pause cdc ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.SourceUri))
	if node.Option.All {
		ctx.WriteString("all")
	} else {
		ctx.WriteString(" taskname ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Option.TaskName))
	}
	ctx.WriteByte(';')
}

func (node *PauseCDC) GetStatementType() string { return "Pause CDC" }

func (node *PauseCDC) GetQueryType() string { return QueryTypeOth }

func (node PauseCDC) TypeName() string { return "tree.PauseCDC" }

func (node *PauseCDC) reset() {
	if node.Option != nil {
		node.Option = nil
	}
	*node = PauseCDC{}
}

type DropCDC struct {
	statementImpl
	SourceUri string
	Option    *AllOrNotCDC
}

func (node *DropCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("drop cdc ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.SourceUri))
	if node.Option.All {
		ctx.WriteString("all")
	} else {
		ctx.WriteString(" taskname ")
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
	ctx.WriteString(" taskname ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.TaskName))
	ctx.WriteByte(';')
}

func (node *ResumeCDC) GetStatementType() string { return "Resume CDC" }

func (node *ResumeCDC) GetQueryType() string { return QueryTypeOth }

func (node ResumeCDC) TypeName() string { return "tree.ResumeCDC" }

func (node *ResumeCDC) reset() {
	*node = ResumeCDC{}
}

type RestartCDC struct {
	statementImpl
	SourceUri string
	TaskName  string
}

func (node *RestartCDC) Format(ctx *FmtCtx) {
	ctx.WriteString("resume cdc ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.SourceUri))
	ctx.WriteString(" taskname ")
	ctx.WriteString(fmt.Sprintf("'%s'", node.TaskName))
	ctx.WriteString(" 'restart'")
	ctx.WriteByte(';')
}

func (node *RestartCDC) GetStatementType() string { return "Restart CDC" }

func (node *RestartCDC) GetQueryType() string { return QueryTypeOth }

func (node RestartCDC) TypeName() string { return "tree.RestartCDC" }

func (node *RestartCDC) reset() {
	*node = RestartCDC{}
}
