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

package aggexec

import "github.com/matrixorigin/matrixone/pkg/container/types"

type AggContext struct {
	hasCommonContext bool
	hasGroupContext  bool
	commonContext    AggCommonExecContext
	groupContext     []AggGroupExecContext
	initGroup        AggGroupContextInit
}

func newAggContextFromImpl(
	ctxImpl aggContextImplementation,
	result types.Type,
	args ...types.Type) *AggContext {

	ctx := &AggContext{
		hasCommonContext: ctxImpl.hasCommonContext,
		hasGroupContext:  ctxImpl.hasGroupContext,
	}
	if ctx.hasGroupContext {
		ctx.initGroup = ctxImpl.generateGroupContext
	}
	if ctxImpl.hasCommonContext {
		ctx.setCommonContext(ctxImpl.generateCommonContext(result, args...))
	}
	return ctx
}

type AggGroupContextInit func(resultType types.Type, parameters ...types.Type) AggGroupExecContext
type AggCommonContextInit func(resultType types.Type, parameters ...types.Type) AggCommonExecContext

func (a *AggContext) setCommonContext(c AggCommonExecContext) {
	a.commonContext = c
}

func (a *AggContext) preAllocate(more int) {
	if !a.hasGroupContext {
		return
	}

	n := len(a.groupContext) + more
	if n <= cap(a.groupContext) {
		return
	}

	oldLen := len(a.groupContext)
	a.groupContext = append(a.groupContext, make([]AggGroupExecContext, n-cap(a.groupContext))...)
	a.groupContext = a.groupContext[:oldLen]
}

func (a *AggContext) growsGroupContext(
	more int,
	resultType types.Type, parameters ...types.Type) {
	if !a.hasGroupContext {
		return
	}
	oldLen, newLen := len(a.groupContext), len(a.groupContext)+more
	if newLen > cap(a.groupContext) {
		a.groupContext = append(a.groupContext, make([]AggGroupExecContext, more)...)
		for i := oldLen; i < newLen; i++ {
			a.groupContext[i] = a.initGroup(resultType, parameters...)
		}

	} else {
		a.groupContext = a.groupContext[:newLen]
		for i := oldLen; i < newLen; i++ {
			a.groupContext[i] = a.initGroup(resultType, parameters...)
		}
	}
}

func (a *AggContext) getCommonContext() AggCommonExecContext {
	return a.commonContext
}

func (a *AggContext) getGroupContext(i int) AggGroupExecContext {
	if a.hasGroupContext {
		return a.groupContext[i]
	}
	return nil
}

func (a *AggContext) getGroupContextEncodings() [][]byte {
	if !a.hasGroupContext {
		return nil
	}
	encodings := make([][]byte, len(a.groupContext))
	for i := range a.groupContext {
		encodings[i] = a.groupContext[i].Marshal()
	}
	return encodings
}

func (a *AggContext) decodeGroupContexts(encodings [][]byte, resultType types.Type, parameters ...types.Type) {
	if !a.hasGroupContext {
		return
	}
	a.groupContext = make([]AggGroupExecContext, len(encodings))
	for i := range encodings {
		a.groupContext[i] = a.initGroup(resultType, parameters...)
		a.groupContext[i].Unmarshal(encodings[i])
	}
}

// AggCommonExecContext stores the common context for all the groups.
// like the type scale, timezone and so on.
type AggCommonExecContext interface {
	AggCanMarshal
}

// AggGroupExecContext store the content of each group individually.
// like the row-count has been filled, the sum of the values and so on.
type AggGroupExecContext interface {
	AggCanMarshal
}
