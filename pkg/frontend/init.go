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

package frontend

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// init wires executor.DefaultResolveVariable so background internal-
// SQL execution (which has no frontend session attached to its
// *process.Process — ProcessInitSQL, idxcron, etc.) can still resolve
// system variables to their defaults. Mirrors the function-variable
// wiring used by pkg/sql/plan/plugin_builder.go.
//
// gSysVarsDefs (the in-memory defaults map) is the single source of
// truth here — no per-tenant catalog read, no SET GLOBAL fidelity.
// Per-index admin overrides are expected to ride along in the
// captured-vars sqlexec.Metadata that the idxcron task carries.
func init() {
	executor.DefaultResolveVariable = func(
		varName string, isSystemVar, _ bool,
	) (any, error) {
		if !isSystemVar {
			return nil, moerr.NewInternalErrorNoCtx(
				"user variables unavailable in background ProcessInitSQL")
		}
		name := strings.ToLower(varName)
		def, ok := gSysVarsDefs[name]
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx(
				errorSystemVariableDoesNotExist())
		}
		return def.Default, nil
	}
}
