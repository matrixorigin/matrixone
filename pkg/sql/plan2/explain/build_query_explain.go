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
	// "container/list"
	// "fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ ExplainQuery = &ExplainQueryImpl{}

type ExplainQueryImpl struct {
	QueryPlan *plan.Query
}

func NewExplainQueryImpl(query *plan.Query) *ExplainQueryImpl {
	return &ExplainQueryImpl{
		QueryPlan: query,
	}
}

func (e *ExplainQueryImpl) ExplainPlan(buffer *ExplainDataBuffer, options *ExplainOptions) {
	var Nodes []*plan.Node = e.QueryPlan.Nodes
	for index, rootNodeId := range e.QueryPlan.Steps {
		//logutil.Infof("--------------------------------------------Query Plan %v ------------------------------------------", index)
		fmt.Printf("--------------------------------------------Query Plan %v ------------------------------------------\n", index)
		settings := FormatSettings{
			buffer:     buffer,
			offset:     0,
			indent:     4,
			indentChar: ' ',
			level:      0,
		}

		stack := NewStack()
		stack.Push(&Frame{
			node:               Nodes[rootNodeId],
			isDescriptionPrint: false,
			nextChild:          0,
		})

		for !stack.Empty() {
			frame := stack.Top()
			if !frame.isDescriptionPrint {
				settings.offset = (stack.Size()) * settings.indent
				explainStep(frame.node, &settings, options)
				frame.isDescriptionPrint = true
			}

			if frame.nextChild < len(frame.node.Children) {
				childId := frame.node.Children[frame.nextChild]
				stack.Push(&Frame{
					node:               Nodes[childId],
					isDescriptionPrint: false,
					nextChild:          0,
				})
				frame.nextChild++
			} else {
				stack.Pop()
			}
		}
		//Separate the different plan trees with blank lines
		settings.buffer.PushLine(0, " ", true, false)
	}
}

func (e *ExplainQueryImpl) ExplainAnalyze(buffer *ExplainDataBuffer, options *ExplainOptions) {
	//TODO implement me
	panic("implement me")
}

func explainStep(step *plan.Node, settings *FormatSettings, options *ExplainOptions) {
	nodedescImpl := NewNodeDescriptionImpl(step)

	if options.Format == EXPLAIN_FORMAT_TEXT {
		//ExplainIndentText(settings, options)
		basicNodeInfo := nodedescImpl.GetNodeBasicInfo(options)
		if settings.level == 0 {
			settings.buffer.PushLine(settings.offset, basicNodeInfo, true, true)
			settings.level++
		} else {
			settings.buffer.PushLine(settings.offset, basicNodeInfo, false, true)
			settings.level++
		}

		if options.Verbose {
			projecrtInfo := nodedescImpl.GetProjectListInfo(options)
			settings.buffer.PushLine(settings.offset, projecrtInfo, false, false)
		}

		extraInfo := nodedescImpl.GetExtraInfo(options)
		for _, line := range extraInfo {
			settings.buffer.PushLine(settings.offset, line, false, false)
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {

	} else if options.Format == EXPLAIN_FORMAT_DOT {

	}
}
