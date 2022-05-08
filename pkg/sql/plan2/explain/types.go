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

package explain

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"strings"
)

//type TableDef plan.TableDef
//type ObjectRef plan.ObjectRef
type Cost plan.Cost
type Const plan.Const

//type Expr plan.Expr

//type Node plan.Node
type RowsetData plan.RowsetData

//type Query plan.Query

type ExplainQuery interface {
	ExplainPlan(buffer *ExplainDataBuffer, options *ExplainOptions)
	ExplainAnalyze(buffer *ExplainDataBuffer, options *ExplainOptions)
}

type NodeDescribe interface {
	GetNodeBasicInfo(options *ExplainOptions) string
	GetExtraInfo(options *ExplainOptions) []string
	GetProjectListInfo(options *ExplainOptions) string
	GetJoinConditionInfo(options *ExplainOptions) string
	GetWhereConditionInfo(options *ExplainOptions) string
	GetOrderByInfo(options *ExplainOptions) string
	GetGroupByInfo(options *ExplainOptions) string
}

type NodeElemDescribe interface {
	GetDescription(options *ExplainOptions) string
}

//-------------------------------------------------------------------------------------------------------

type FormatSettings struct {
	buffer *ExplainDataBuffer
	offset int
	indent int
	level  int
}

type ExplainDataBuffer struct {
	Start          int
	End            int
	CurrentLine    int
	NodeSize       int
	LineWidthLimit int
	Lines          []string
}

func NewExplainDataBuffer() *ExplainDataBuffer {
	return &ExplainDataBuffer{
		Start:          -1,
		End:            -1,
		CurrentLine:    -1,
		NodeSize:       0,
		LineWidthLimit: 65535,
		Lines:          make([]string, 0),
	}
}

// Generates a string describing a ExplainDataBuffer.
func (buf *ExplainDataBuffer) ToString() string {
	return fmt.Sprintf("ExplainDataBuffer{start: %d, end: %d, lines: %s, NodeSize: %d}", buf.Start, buf.End, buf.Lines, buf.NodeSize)
}

func (buf *ExplainDataBuffer) AppendCurrentLine(temp string) {
	if buf.CurrentLine != -1 && buf.CurrentLine < len(buf.Lines) {
		buf.Lines[buf.CurrentLine] += temp
	} else {
		panic("implement me")
	}
}

func calcSpaceNum(level int) int {
	if level <= 0 {
		return 2
	} else {
		return calcSpaceNum(level-1) + 6
	}
}

func (buf *ExplainDataBuffer) PushNewLine(line string, isNewNode bool, level int) {
	var prefix string = ""
	if level <= 0 {
		if isNewNode {
			prefix += ""
		} else {
			prefix += "  "
		}
	} else {
		var offset int = calcSpaceNum(level)
		if isNewNode {
			prefix += strings.Repeat(" ", offset-6) + "->  "
		} else {
			prefix += strings.Repeat(" ", offset)
		}
	}
	if buf.Start == -1 {
		buf.Start++
	}
	buf.CurrentLine++
	buf.Lines = append(buf.Lines, prefix+line)
	//buf.Lines[buf.CurrentLine] = (prefix + line)
	fmt.Println(buf.Lines[buf.CurrentLine])
	buf.End++
}

func (buf *ExplainDataBuffer) PushLine(offset int, line string, planRoot bool, nodeHeader bool) {
	var prefix string = strings.Repeat("#", offset)
	if planRoot {
		prefix += ""
	} else if nodeHeader {
		prefix += "-> "
	} else if buf.NodeSize > 1 {
		prefix += "   "
	} else {
		prefix += "  "
	}
	if buf.Start == -1 {
		buf.Start++
	}
	buf.CurrentLine++
	buf.Lines = append(buf.Lines, prefix+line)
	fmt.Println(buf.Lines[buf.CurrentLine])
	buf.End++
}

func (buf *ExplainDataBuffer) IsFull() bool {
	return false
	//TODO
}

func (buf *ExplainDataBuffer) Empty() bool {
	return false
	//TODO
}

type ExplainFormat int32

const (
	EXPLAIN_FORMAT_TEXT ExplainFormat = 0
	EXPLAIN_FORMAT_XML  ExplainFormat = 1
	EXPLAIN_FORMAT_JSON ExplainFormat = 2
	EXPLAIN_FORMAT_DOT  ExplainFormat = 3
)

type ExplainOptions struct {
	Verbose bool
	Anzlyze bool
	Format  ExplainFormat
}

func NewExplainPlanOptions() *ExplainOptions {
	return nil
}

type QueryPlanSetting struct {
	Name             string
	Optimize         bool
	JSON             bool
	DOT              bool
	QueryPlanOptions ExplainOptions
}
