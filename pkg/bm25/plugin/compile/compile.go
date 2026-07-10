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

// Package compile implements the bm25 plugin's compile-layer (DDL) hooks:
// building the binary index from source on CREATE, reindex/merge, restore,
// and CDC-task cleanup on DROP.
//
// STUBS (Phase 1b): the bodies land in Phase 2 (create/build) and Phase 4
// (reindex/merge/restore). The plugin is not registered until they are real,
// so these stubs are never dispatched.
package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Compile-time interface check.
var _ compileplugin.Hooks = Hooks{}

// Hooks implements plugin/compile.Hooks for bm25.
type Hooks struct{}

func (Hooks) HandleCreateIndex(compileplugin.CompileContext, map[string]*plan.IndexDef) error {
	return moerr.NewNYINoCtx("bm25 HandleCreateIndex (Phase 2)")
}

func (Hooks) HandleReindex(compileplugin.CompileContext, map[string]*plan.IndexDef, bool) error {
	return moerr.NewNYINoCtx("bm25 HandleReindex (Phase 4)")
}

func (Hooks) RestoreInitSQL(compileplugin.CompileContext, map[string]*plan.IndexDef) (bool, string, error) {
	return false, "", moerr.NewNYINoCtx("bm25 RestoreInitSQL (Phase 4)")
}

func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	// bm25 has no reindex-time param overrides yet; keep the existing params.
	return old, nil
}

func (Hooks) HandleDropIndex(compileplugin.CompileContext, map[string]*plan.IndexDef) error {
	// CDC-task teardown lands in Phase 4; no-op is safe until then.
	return nil
}

func (Hooks) IdxcronMetadata(compileplugin.CompileContext) ([]byte, error) {
	return nil, nil
}
