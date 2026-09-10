// Copyright 2026 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/vm/process"

// warningAttempt binds a generation-specific sink without replacing Session,
// whose optional interfaces are still needed by expression execution.
type warningAttempt struct {
	collector *remoteWarningCollector
	previous  map[*process.Process]any
}

func newWarningAttempt(proc *process.Process) *warningAttempt {
	if proc == nil {
		return nil
	}
	_, single := proc.GetWarningSink().(warningDiagnosticSink)
	_, batch := proc.GetWarningSink().(warningDiagnosticBatchSink)
	if !single && !batch {
		return nil
	}
	a := &warningAttempt{collector: &remoteWarningCollector{}, previous: make(map[*process.Process]any)}
	a.bindProcess(proc)
	return a
}

func (a *warningAttempt) bindProcess(proc *process.Process) {
	if a == nil || proc == nil {
		return
	}
	if _, ok := a.previous[proc]; !ok {
		a.previous[proc] = proc.WarningSink
	}
	proc.WarningSink = a.collector
}

func (a *warningAttempt) bindScopes(scopes []*Scope) {
	if a == nil {
		return
	}
	seen := make(map[*Scope]bool)
	var bind func(*Scope)
	bind = func(s *Scope) {
		if s == nil || seen[s] {
			return
		}
		seen[s] = true
		a.bindProcess(s.Proc)
		for _, child := range s.PreScopes {
			bind(child)
		}
	}
	for _, s := range scopes {
		bind(s)
	}
}

func (a *warningAttempt) finish(success bool, destination any) {
	if a == nil {
		return
	}
	total, warnings := a.collector.closeWarnings(success)
	a.restore()
	if total == 0 {
		return
	}
	codes := make([]uint16, len(warnings))
	messages := make([]string, len(warnings))
	for i, w := range warnings {
		codes[i], messages[i] = w.Code, w.Message
	}
	if sink, ok := destination.(warningDiagnosticBatchSink); ok {
		sink.AppendWarningBatch(total, codes, messages)
	} else if sink, ok := destination.(warningDiagnosticSink); ok {
		for i := range codes {
			sink.AppendWarningDiagnostic(codes[i], messages[i])
		}
	}
}

func (a *warningAttempt) discard() {
	if a != nil {
		a.collector.closeWarnings(false)
	}
}

// restore runs before a retry compile returns its scopes to the pool.
func (a *warningAttempt) restore() {
	if a == nil {
		return
	}
	for proc, previous := range a.previous {
		proc.WarningSink = previous
	}
	clear(a.previous)
}
