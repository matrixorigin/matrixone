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
	if !ctx.IsFrontend() {
		return nil, nil
	}
	if len(spec.Capture) == 0 {
		return nil, nil
	}

	w := sqlexec.NewMetadataWriter()
	for _, name := range spec.Capture {
		v, err := ctx.ResolveVariable(name, true, false)
		if err != nil {
			return nil, err
		}
		// nil happens when a sysvar is registered but has no
		// session-level value set (e.g. inside a sub-Compile spawned
		// by CREATE TABLE CLONE or other internal-SQL paths whose
		// session lookup may return (nil, nil) instead of the
		// compile-time default). Skip — the idxcron consumer's
		// task.Metadata.ResolveVariableFunc falls back to its own
		// default when the var isn't in the captured blob.
		if v == nil {
			continue
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
