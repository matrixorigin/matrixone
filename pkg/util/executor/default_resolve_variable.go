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

package executor

// DefaultResolveVariable is a process-wide fallback system-variable
// resolver, installed once at startup by pkg/frontend's init() from
// gSysVarsDefs. It is consulted whenever code needs to resolve a
// system variable from a *process.Process that has no session-bound
// resolver attached — typically background flows (idxcron ALTER
// REINDEX, ProcessInitSQL, bootstrap, cron tasks).
//
// Lives here in pkg/util/executor because:
//   - pkg/iscp, pkg/sql/compile, and pkg/frontend already import this
//     package, so it's the lowest common ancestor.
//   - Conceptually paired with Options.WithResolveVariableFunc and
//     proc.Base.IsFrontend — all three turn on the same axis ("does
//     this proc have a session?").
//
// Nil-safe by design: tests that don't blank-import pkg/frontend see
// this as nil; callers must nil-check before invocation. See
// pkg/iscp/iteration.go::ProcessInitSQL and
// pkg/sql/compile/util.go::resolveVariableOrDefault for the canonical
// nil-checked consumers.
//
// Defaults-only — SET GLOBAL overrides are NOT honoured. Per-index
// admin-tuned values are expected to ride along in the captured-vars
// sqlexec.Metadata that the idxcron task carries.
var DefaultResolveVariable func(
	varName string, isSystemVar, isGlobalVar bool,
) (any, error)
