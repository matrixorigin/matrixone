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

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// warningAttempt binds a generation-specific sink without replacing Session,
// whose optional interfaces are still needed by expression execution.
type warningAttempt struct {
	collector *remoteWarningCollector
	previous  map[*process.Process]any
}

func newWarningAttempt(proc *process.Process, force ...bool) *warningAttempt {
	if proc == nil {
		return nil
	}
	destination := proc.GetWarningSink()
	_, single := destination.(warningDiagnosticSink)
	_, batch := destination.(warningDiagnosticBatchSink)
	_, count := destination.(warningDiagnosticCountSink)
	forced := len(force) > 0 && force[0]
	if !single && !batch && !count && !forced {
		return nil
	}
	a := &warningAttempt{collector: &remoteWarningCollector{}, previous: make(map[*process.Process]any)}
	a.bindProcess(proc)
	return a
}

func (a *warningAttempt) groupConcatCutDiagnostic() (bool, string) {
	if a == nil {
		return false, ""
	}
	return a.collector.groupConcatCutDiagnostic()
}

func (c *Compile) strictWriteGroupConcatPromotionEnabled() (bool, error) {
	if c == nil {
		return false, nil
	}
	switch stmt := c.stmt.(type) {
	case *tree.Insert:
		if len(stmt.OnDuplicateUpdate) == 1 && stmt.OnDuplicateUpdate[0] == nil {
			return false, nil
		}
	case *tree.Update, *tree.Replace:
	case *tree.CreateTable:
		if !stmt.IsAsSelect {
			return false, nil
		}
	default:
		return false, nil
	}
	err, strict := StrictSqlMode(c.proc)
	return strict, err
}

func (c *Compile) strictWriteGroupConcatCutError(
	warnings *warningAttempt,
	promotionEnabled bool,
) error {
	if !promotionEnabled {
		return nil
	}
	cut, message := warnings.groupConcatCutDiagnostic()
	if !cut {
		return nil
	}
	return moerr.NewGroupConcatCut(c.proc.Ctx, message)
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
	appendWarningBatchToSink(destination, total, codes, messages)
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
