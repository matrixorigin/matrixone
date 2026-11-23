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

package plan

// resolveTemporaryTable restricts destination checks to a session-owned alias
// when the compiler has that capability. Other contexts retain their existing
// resolution semantics, including any authorization error.
func resolveTemporaryTable(ctx CompilerContext, db, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	if resolver, ok := ctx.(interface {
		ResolveTemporaryTable(string, string, *Snapshot) (*ObjectRef, *TableDef, error)
	}); ok {
		return resolver.ResolveTemporaryTable(db, name, snapshot)
	}
	return ctx.Resolve(db, name, snapshot, nil)
}
