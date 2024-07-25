// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// InitPipelineContextToExecuteQuery initializes the context for each pipeline tree.
// The entire process must follow these rules:
// 1. the context of all pipelines can be controlled by the query context.
// 2. if there's a data transfer between two pipelines, the lifecycle of the sender's context ends with the receiver's termination.
func (c *Compile) InitPipelineContextToExecuteQuery() {
	contextBase := c.proc.Base.GetContextBase()
	queryContext := contextBase.BuildQueryCtx()
	queryContext = contextBase.SaveToQueryContext(defines.EngineKey{}, c.e)
	queryContext = contextBase.WithCounterSetToQueryContext(c.counterSet)

	// build pipeline context.
	currentContext := c.proc.BuildPipelineContext(queryContext)
	for _, pipeline := range c.scope {
		if pipeline.Proc == nil {
			continue
		}
		pipeline.buildContextFromParentCtx(currentContext)
	}
}

// buildContextFromParentCtx build the context for the pipeline tree.
// the input parameter is the whole tree's parent context.
func (s *Scope) buildContextFromParentCtx(parentCtx context.Context) {
	receiverCtx := s.Proc.BuildPipelineContext(parentCtx)

	// build context for receiver.
	for _, prePipeline := range s.PreScopes {
		prePipeline.buildContextFromParentCtx(receiverCtx)
	}
}
