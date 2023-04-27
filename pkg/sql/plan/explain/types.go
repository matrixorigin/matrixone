// Copyright 2021 - 2022 Matrix Origin
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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type ExplainQuery interface {
	ExplainPlan(ctx context.Context, buffer *ExplainDataBuffer, options *ExplainOptions) error
}

type NodeDescribe interface {
	GetNodeBasicInfo(ctx context.Context, options *ExplainOptions) (string, error)
	GetExtraInfo(ctx context.Context, options *ExplainOptions) ([]string, error)
	GetProjectListInfo(ctx context.Context, options *ExplainOptions) (string, error)
	GetJoinTypeInfo(ctx context.Context, options *ExplainOptions) (string, error)
	GetJoinConditionInfo(ctx context.Context, options *ExplainOptions) (string, error)
	GetFilterConditionInfo(ctx context.Context, options *ExplainOptions) (string, error)
	GetOrderByInfo(ctx context.Context, options *ExplainOptions) (string, error)
	GetGroupByInfo(ctx context.Context, options *ExplainOptions) (string, error)
	GetTableDef(ctx context.Context, options *ExplainOptions) (string, error)
	GetActualAnalyzeInfo(ctx context.Context, options *ExplainOptions) (string, error)
}

type NodeElemDescribe interface {
	GetDescription(ctx context.Context, options *ExplainOptions) (string, error)
}

type FormatSettings struct {
	buffer *ExplainDataBuffer
	offset int
	indent int
	level  int
}

type ExplainDataBuffer struct {
	Start       int
	End         int
	CurrentLine int
	NodeSize    int
	Lines       []string
}

func NewExplainDataBuffer() *ExplainDataBuffer {
	return &ExplainDataBuffer{
		Start:       -1,
		End:         -1,
		CurrentLine: -1,
		NodeSize:    0,
		Lines:       make([]string, 0),
	}
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
	prefix := ""
	if level <= 0 {
		if isNewNode {
			prefix += ""
		} else {
			prefix += "  "
		}
	} else {
		offset := calcSpaceNum(level)
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
	logutil.Infof(buf.Lines[buf.CurrentLine])
	buf.End++
}

func (buf *ExplainDataBuffer) PushPlanTitle(title string) {
	if buf.Start == -1 {
		buf.Start++
	}
	buf.CurrentLine++
	buf.Lines = append(buf.Lines, title)
	logutil.Infof(buf.Lines[buf.CurrentLine])
	buf.End++
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
	Analyze bool
	Format  ExplainFormat
}

func NewExplainDefaultOptions() *ExplainOptions {
	return &ExplainOptions{
		Verbose: false,
		Analyze: false,
		Format:  EXPLAIN_FORMAT_TEXT,
	}
}
