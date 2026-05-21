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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// IdxcronVarSpec declares the system / session variables an algorithm
// wants pinned into its idxcron task's metadata blob. Consumed by
// BuildIdxcronMetadata.
//
// Why declarative: every algorithm's IdxcronMetadata otherwise reduces
// to the same "if background, defer; else resolve N session variables,
// write them via sqlexec.MetadataWriter, marshal" sequence. Centralising
// that loop in BuildIdxcronMetadata lets each algo's hook shrink to a
// 2-line spec declaration.
type IdxcronVarSpec struct {
	// FrontendProbeVar is a known vector-index session sysvar (e.g.
	// "ivf_threads_search" for IVF-FLAT, "cagra_threads_search" for
	// CAGRA) used as a second-level gate AFTER ctx.IsFrontend().
	//
	// Rationale: a sub-Compile spawned via runSqlWithOptions (for
	// example, the CREATE TABLE CLONE flow) inherits IsFrontend=true
	// from the outer frontend Compile AND inherits the frontend
	// session's resolver — but the session-sysvar lookup in that
	// sub-Compile may legitimately return (nil, nil) for specific
	// vars. Capturing partial metadata in that state would write a
	// blob missing fields the idxcron executor expects; instead we
	// short-circuit to background semantics (the consumer's read
	// path falls back to compile-time defaults).
	//
	// The probe var must:
	//   - Be a known sysvar this algorithm cares about.
	//   - Resolve to a non-nil value in a true frontend session.
	//   - Be allowed to resolve to nil in a partial-context
	//     sub-Compile (no specific contract on the value itself).
	//
	// Empty FrontendProbeVar means "no probe" — Capture is always
	// resolved. Used by plugins whose Capture list is empty.
	FrontendProbeVar string

	// Capture is the list of session/system variable names to resolve
	// and write into the metadata blob, in declaration order.
	Capture []string
}

// BuildIdxcronMetadata is the shared implementation each algorithm's
// compile.Hooks.IdxcronMetadata delegates to. It checks the explicit
// IsFrontend signal, resolves each captured variable through the
// CompileContext, and serialises the result via
// sqlexec.MetadataWriter — producing the typed JSON shape that the
// idxcron executor's task.Metadata.ResolveVariableFunc reads back at
// firing time (so the eventual ALTER REINDEX runs with the values the
// user picked at CREATE INDEX, not current system-var state).
//
// Returns (nil, nil) when:
//   - ctx.IsFrontend() is false (caller is the cron executor's running
//     ALTER REINDEX path or another internal-SQL flow — the captured
//     metadata from the original frontend CREATE INDEX is authoritative,
//     don't overwrite with defaults), or
//   - Capture is empty (algorithm wants no pinned config).
//
// The implementation type-switches on ResolveVariable's runtime value
// to call the right MetadataWriter.AddInt / AddFloat / AddString /
// AddInt8 method.
func BuildIdxcronMetadata(ctx CompileContext, spec IdxcronVarSpec) ([]byte, error) {
	tblName := ""
	if def := ctx.OriginalTableDef(); def != nil {
		tblName = def.Name
	}
	if !ctx.IsFrontend() {
		logutil.Infof("[isfrontend] BuildIdxcronMetadata skip: table=%s isFrontend=false (background re-entry)", tblName)
		return nil, nil
	}
	if len(spec.Capture) == 0 {
		return nil, nil
	}
	logutil.Infof("[isfrontend] BuildIdxcronMetadata capture: table=%s isFrontend=true capture=%v", tblName, spec.Capture)

	// FrontendProbeVar gates the whole Capture pass: if a known
	// vector-index sysvar can't be resolved in this context (typical
	// of sub-Compiles inheriting a partial frontend resolver — e.g.
	// CREATE TABLE CLONE), defer to background semantics and let the
	// consumer-side fallback handle it.
	if spec.FrontendProbeVar != "" {
		probe, err := ctx.ResolveVariable(spec.FrontendProbeVar, true, false)
		if err != nil || probe == nil {
			return nil, nil
		}
	}

	w := sqlexec.NewMetadataWriter()
	for _, name := range spec.Capture {
		v, err := ctx.ResolveVariable(name, true, false)
		if err != nil {
			return nil, err
		}
		switch tv := v.(type) {
		case int8:
			w.AddInt8(name, tv)
		case int:
			w.AddInt(name, int64(tv))
		case int32:
			w.AddInt(name, int64(tv))
		case int64:
			w.AddInt(name, tv)
		case float32:
			w.AddFloat(name, float64(tv))
		case float64:
			w.AddFloat(name, tv)
		case string:
			w.AddString(name, tv)
		default:
			return nil, moerr.NewInternalErrorNoCtxf(
				"BuildIdxcronMetadata: variable %q has unsupported type %T", name, v)
		}
	}
	return w.Marshal()
}
