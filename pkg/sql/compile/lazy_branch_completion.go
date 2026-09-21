// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"sync"
)

// 一个 MergeRun 执行代独占一组 completion；每个分支只由其启动路径完成一次。
// close(done) 发布 result，也证明 Run/RemoteRun 的清理已结束。
type lazyBranchCompletion struct {
	mu        sync.Mutex
	done      chan struct{}
	result    scopeRunResult
	finished  bool
	callbacks []func(error)
}

func newLazyBranchCompletion() *lazyBranchCompletion {
	return &lazyBranchCompletion{done: make(chan struct{})}
}

func (b *lazyBranchCompletion) finish(result scopeRunResult) {
	b.mu.Lock()
	if b.finished {
		b.mu.Unlock()
		return
	}
	b.result = result
	b.finished = true
	callbacks := b.callbacks
	b.callbacks = nil
	close(b.done)
	b.mu.Unlock()
	resolved, _ := result.resolveCancelCause()
	for _, callback := range callbacks {
		callback(resolved.err)
	}
}

// register attaches a non-blocking continuation callback to the branch
// completion event. It is the only production wait path; callers never park a
// scheduler worker on the completion channel.
func (b *lazyBranchCompletion) register(callback func(error)) error {
	if callback == nil {
		return context.Canceled
	}
	b.mu.Lock()
	if !b.finished {
		b.callbacks = append(b.callbacks, callback)
		b.mu.Unlock()
		return nil
	}
	result := b.result
	b.mu.Unlock()
	resolved, _ := result.resolveCancelCause()
	callback(resolved.err)
	return nil
}

func (b *lazyBranchCompletion) wait(ctx context.Context) error {
	select {
	case <-b.done:
		b.mu.Lock()
		result := b.result
		b.mu.Unlock()
		result, _ = result.resolveCancelCause()
		return result.err
	case <-ctx.Done():
		// 此处只终止等待，不代表分支已清理；MergeRun 仍会 wg.Wait 收尾。
		return context.Cause(ctx)
	}
}
