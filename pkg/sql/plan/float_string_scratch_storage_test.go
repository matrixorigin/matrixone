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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Count Go object storage reachable from the scratch owner. String bytes are
// charged even when shared with source, so this conservatively overcounts them.
// Allocator rounding and unrelated compiler/process/memo storage are excluded.
func legacyFloatScratchStorageBytes(root *legacyFloatProbeScratch) uintptr {
	type address struct {
		ptr uintptr
		typ reflect.Type
	}
	seen := make(map[address]bool)
	var bytes uintptr
	var walk func(reflect.Value)
	walk = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.Interface:
			if !v.IsNil() {
				walk(v.Elem())
			}
		case reflect.Pointer:
			if v.IsNil() {
				return
			}
			key := address{v.Pointer(), v.Type()}
			if seen[key] {
				return
			}
			seen[key] = true
			bytes += v.Type().Elem().Size()
			walk(v.Elem())
		case reflect.Struct:
			for i := range v.NumField() {
				walk(v.Field(i))
			}
		case reflect.Slice:
			if v.IsNil() {
				return
			}
			key := address{v.Pointer(), v.Type()}
			if seen[key] {
				return
			}
			seen[key] = true
			bytes += uintptr(v.Cap()) * v.Type().Elem().Size()
			for i := range v.Len() {
				walk(v.Index(i))
			}
		case reflect.Array:
			for i := range v.Len() {
				walk(v.Index(i))
			}
		case reflect.Map, reflect.Func, reflect.Chan, reflect.UnsafePointer:
			panic("unsupported scratch storage field: " + v.Type().String())
		case reflect.String:
			bytes += uintptr(v.Len())
		}
	}
	walk(reflect.ValueOf(root))
	return bytes
}

// Four owners stay alive together at a barrier after 32 real session folds.
// This measures the scratch retention bound, independently of total allocation.
// TerminalCleanup/InterruptedIslandsReuse already cover the unhappy terminals.
func TestLegacyFloatScratchStorageBound(t *testing.T) {
	for _, shared := range []bool{true, false} {
		t.Run(fmt.Sprintf("same_source=%t", shared), func(t *testing.T) {
			var compilers [4]*MockCompilerContext
			for i := range compilers {
				compilers[i] = floatCatalogCompiler(t)
			}
			temporal, err := BindFuncExprImplByPlanExpr(compilers[0].GetContext(), "from_unixtime", []*planpb.Expr{makePlan2Int64ConstExprWithType(0)})
			require.NoError(t, err)
			source, err := BindFuncExprImplByPlanExpr(compilers[0].GetContext(), "date_format", []*planpb.Expr{temporal, makePlan2StringConstExprWithType("%M %H")})
			require.NoError(t, err)
			before := proto.Clone(source)
			var sources [4]*planpb.Expr
			for i := range sources {
				sources[i] = source
				if !shared {
					sources[i] = DeepCopyExpr(source)
					sources[i].GetF().Args[0].GetF().Args[0] = makePlan2Int64ConstExprWithType(int64(i+1) * 60)
				}
			}
			type held struct {
				probe        *legacyFloatSessionProbe
				initialBytes uintptr
				err          error
			}
			ready, done := make(chan held, 4), make(chan error, 4)
			release := make(chan struct{})
			defer func() {
				if release != nil {
					close(release)
					for range compilers {
						<-done
					}
				}
			}()
			for worker, compiler := range compilers {
				go func() {
					probe := &legacyFloatSessionProbe{islands: []legacyFloatSessionIsland{{source: sources[worker]}}}
					var initial *legacyFloatProbeScratch
					var initialBytes uintptr
					run := func() error {
						for candidate := range 32 {
							compiler.GetProcess().GetSessionInfo().TimeZone = time.FixedZone("candidate", candidate*900)
							want, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(sources[worker]), legacyFloatSessionProbeProcess(compiler, 0), false, true)
							if err != nil {
								return err
							}
							probe.islands[0].want = want
							matched, err := probe.matches(compiler, &tableDumpBindingBudget{})
							if err != nil {
								return err
							}
							if !matched {
								return fmt.Errorf("fold did not match candidate %d", candidate)
							}
							scratch := probe.islands[0].scratch
							if scratch == nil {
								return fmt.Errorf("no scratch owner")
							}
							if candidate == 0 {
								initial = scratch
								initialBytes = legacyFloatScratchStorageBytes(scratch)
							}
							if scratch != initial || legacyFloatScratchStorageBytes(scratch) != initialBytes {
								return fmt.Errorf("scratch grew across candidates")
							}
						}
						return nil
					}
					err := run()
					ready <- held{probe, initialBytes, err}
					<-release
					// Even an exhausted search must clear its production scratch owner.
					_, cleanupErr := matchLegacyFloatSessionProbes(compiler, time.UTC, "en_US", func(func(*time.Location) bool) {}, []*legacyFloatSessionProbe{probe}, &tableDumpBindingBudget{}, tableDumpMaxCandidateVisits, nil, nil)
					if cleanupErr == nil && probe.islands[0].scratch != nil {
						cleanupErr = fmt.Errorf("retained terminal scratch")
					}
					done <- cleanupErr
				}()
			}
			var bytes uintptr
			roots := make(map[*planpb.Expr]bool)
			for range compilers {
				state := <-ready
				require.NoError(t, state.err)
				scratch := state.probe.islands[0].scratch
				require.NotNil(t, scratch)
				require.False(t, roots[scratch.root], "workers must own distinct clones")
				roots[scratch.root] = true
				require.Equal(t, state.initialBytes, legacyFloatScratchStorageBytes(scratch))
				nodes, edges, ok := legacyFloatScratchShape(state.probe.islands[0].source)
				require.True(t, ok)
				require.Equal(t, nodes, cap(scratch.slots))
				require.Equal(t, edges, cap(scratch.edges))
				bytes += state.initialBytes
			}
			close(release)
			release = nil
			var cleanupErrors [4]error
			for i := range cleanupErrors {
				cleanupErrors[i] = <-done
			}
			for _, err := range cleanupErrors {
				require.NoError(t, err)
			}
			require.True(t, proto.Equal(before, source))
			for _, compiler := range compilers {
				require.Zero(t, compiler.GetProcess().Mp().CurrNB())
			}
			t.Logf("four concurrent owners, 32 sessions/owner: reachable scratch storage <=%d bytes (shared strings charged conservatively; excludes allocator rounding/process/memo)", bytes)
		})
	}
}
