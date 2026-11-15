// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestFloatCatalogParentProtocolScope(t *testing.T) {
	// Establish the same runtime owner as a real planner process.
	proc := newPlanTestProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	keys := []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolAuthoringFloor}
	for _, key := range keys {
		previous, present := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if present {
				rt.SetGlobalVariables(key, previous)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
	}
	for _, existing := range []bool{false, true} {
		for i, key := range keys {
			if existing {
				rt.SetGlobalVariables(key, int64(i+1))
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		}
		name := "absent"
		if existing {
			name = "preexisting"
		}
		t.Run(name, func(parent *testing.T) {
			floatCatalogCompiler(parent)
			// Finish one real fixture before the delayed parallel child. Its
			// cleanup must restore the parent scope, never the older values.
			parent.Run("completed_sibling", func(child *testing.T) {
				floatCatalogCompiler(child)
			})
			parent.Run("parallel_child", func(child *testing.T) {
				child.Parallel()
				// Check before acquiring another fixture, which could otherwise
				// hide a lost parent scope by setting the globals again.
				for _, key := range keys {
					value, ok := rt.GetGlobalVariables(key)
					require.True(child, ok)
					require.Equal(child, defines.MORPCLatestVersion, value)
				}
				compiler := floatCatalogCompiler(child)
				require.Same(child, rt, moruntime.ServiceRuntime(compiler.GetProcess().GetService()))
			})
		})
		// t.Run waits for children and their cleanup before the parent restores its scope.
		for i, key := range keys {
			value, present := rt.GetGlobalVariables(key)
			require.Equal(t, existing, present, key)
			if existing {
				require.Equal(t, int64(i+1), value, key)
			}
		}
	}
}
