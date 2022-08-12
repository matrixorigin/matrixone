// Copyright 2022 Matrix Origin
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

package export

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"sync/atomic"
)

func init() {
	ResetGlobalBatchProcessor()
	SetDefaultContextFunc(func() context.Context { return context.Background() })
}

type BatchProcessor interface {
	Collect(context.Context, batchpipe.HasName) error
	Start() bool
	Stop(graceful bool) error
}

func Register(name batchpipe.HasName, impl batchpipe.PipeImpl[batchpipe.HasName, any]) {
	_ = gPipeImplHolder.Put(name.GetName(), impl)
}

var gBatchProcessor atomic.Value

type processorHolder struct {
	p BatchProcessor
}

func ResetGlobalBatchProcessor() {
	var p BatchProcessor = &noopBatchProcessor{}
	SetGlobalBatchProcessor(p)
}

func SetGlobalBatchProcessor(p BatchProcessor) {
	gBatchProcessor.Store(&processorHolder{p: p})
}

func GetGlobalBatchProcessor() BatchProcessor {
	return gBatchProcessor.Load().(*processorHolder).p
}

type getContextFunc func() context.Context

var defaultContext atomic.Value

func SetDefaultContextFunc(f getContextFunc) {
	defaultContext.Store(f)
}
func DefaultContext() context.Context {
	return defaultContext.Load().(getContextFunc)()
}
