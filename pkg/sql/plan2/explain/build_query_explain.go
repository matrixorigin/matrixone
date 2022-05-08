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
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

func traversalPlan(node *plan.Node, Nodes []*plan.Node, settings *FormatSettings, options *ExplainOptions) {
	if node == nil {
		return
	}
	explainStep(node, settings, options)
	settings.level++
	if len(node.Children) > 0 {
		for _, childIndex := range node.Children {
			traversalPlan(Nodes[childIndex], Nodes, settings, options)
		}
	}
	settings.level--
}

func (e *ExplainQueryImpl) ExplainPlan(buffer *ExplainDataBuffer, options *ExplainOptions) {
	var Nodes []*plan.Node = e.QueryPlan.Nodes
	for index, rootNodeId := range e.QueryPlan.Steps {
		logutil.Infof("------------------------------------Query Plan-%v ---------------------------------------------", index)
		settings := FormatSettings{
			buffer: buffer,
			offset: 0,
			indent: 2,
			level:  0,
		}
		traversalPlan(Nodes[rootNodeId], Nodes, &settings, options)
	}
}

func (e *ExplainQueryImpl) ExplainAnalyze(buffer *ExplainDataBuffer, options *ExplainOptions) {
	//TODO implement me
	panic("implement me")
}

func explainStep(step *plan.Node, settings *FormatSettings, options *ExplainOptions) {
	nodedescImpl := NewNodeDescriptionImpl(step)

	if options.Format == EXPLAIN_FORMAT_TEXT {
		basicNodeInfo := nodedescImpl.GetNodeBasicInfo(options)

		settings.buffer.PushNewLine(basicNodeInfo, true, settings.level)
		if options.Verbose {
			projecrtInfo := nodedescImpl.GetProjectListInfo(options)
			settings.buffer.PushNewLine(projecrtInfo, false, settings.level)
		}

		extraInfo := nodedescImpl.GetExtraInfo(options)
		for _, line := range extraInfo {
			settings.buffer.PushNewLine(line, false, settings.level)
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		panic("implement me")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		panic("implement me")
	}
}
