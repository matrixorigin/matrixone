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

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type temporalScopeReleaseProbe struct {
	scope           *TemporalPlanningScope
	calls           int
	releasedOutside bool
	cleared         bool
}

func (p *temporalScopeReleaseProbe) ReleaseTemporalScope() {
	p.calls++
	p.releasedOutside = p.scope.nativeMu.TryLock()
	if p.releasedOutside {
		p.cleared = !p.scope.Usable() && p.scope.root.Load() == nil && p.scope.query.Load() == nil &&
			p.scope.sources == nil && p.scope.nativeOwner == nil
		p.scope.nativeMu.Unlock()
	}
}

func TestTemporalPlanningScopeIsSingleUseAndRootOwned(t *testing.T) {
	root := &tree.Select{}
	other := &tree.Select{}
	intent := TemporalPlanningIntent{ParameterFree: true}
	scope := NewTemporalPlanningScope(root, intent)
	_, ok := scope.claim(other)
	require.False(t, ok)
	got, ok := scope.claim(root)
	require.True(t, ok)
	require.True(t, got.ParameterFree)
	_, ok = scope.claim(root)
	require.False(t, ok)
	var absent *TemporalPlanningScope
	_, ok = absent.claim(root)
	require.False(t, ok)

	// Two optimization attempts can share an immutable borrowed AST without
	// sharing a claim; concurrent claims within one attempt still have one owner.
	fresh := NewTemporalPlanningScope(root, intent)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, claimed := fresh.claim(root); claimed {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, successes.Load())
	_, ok = NewTemporalPlanningScope(root, intent).claim(root)
	require.True(t, ok)
}

func TestTemporalIntentRejectsSessionLimitDrift(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	intent := TemporalPlanningIntent{TimeZone: proc.GetSessionInfo().TimeZone, UnixTime: proc.Base.UnixTime}
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.SetResolveVariableFunc(nil)
	require.False(t, intent.MatchesProcess(proc), "unknown session limits cannot prove a full scan")
	limit := ^uint64(0)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		require.Equal(t, SQLSelectLimitVariable, name)
		return limit, nil
	})
	require.True(t, intent.MatchesProcess(proc))
	limit = 1
	require.False(t, intent.MatchesProcess(proc), "a limit installed after planning invalidates the execution proof")
}

func TestTemporalPlanningScopeFollowsRootStatementWrappers(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctx := NewMockCompilerContext(true, proc)
	ctx.SetContext(proc.Ctx)
	for _, sql := range []string{"select 1", "(select 1)", "explain select 1", "explain analyze select 1"} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			var root *tree.Select
			switch s := stmt.(type) {
			case *tree.Select:
				root = s
			case *tree.ParenSelect:
				root = s.Select
			case *tree.ExplainStmt:
				root, _ = s.Statement.(*tree.Select)
			case *tree.ExplainAnalyze:
				root, _ = s.Statement.(*tree.Select)
			}
			require.NotNil(t, root)
			scope := NewTemporalPlanningScope(root, TemporalPlanningIntent{ParameterFree: true})
			_, err = NewBaseOptimizer(ctx).OptimizeWithTemporalScope(stmt, false, scope)
			require.NoError(t, err)
			_, claimed := scope.claim(root)
			require.False(t, claimed, "the actual root builder must consume the one-shot scope")
			other := &tree.Select{}
			mismatch := NewTemporalPlanningScope(other, TemporalPlanningIntent{ParameterFree: true})
			_, err = BuildPlanWithTemporalScope(ctx, stmt, false, mismatch)
			require.NoError(t, err)
			_, claimed = mismatch.claim(other)
			require.True(t, claimed, "a different root must not consume the scope")
			if sql == "select 1" {
				prepared := &tree.PrepareStmt{Name: "temporal_scope", Stmt: root}
				preparedScope := NewTemporalPlanningScope(root, TemporalPlanningIntent{ParameterFree: true})
				_, err = BuildPlanWithTemporalScope(ctx, prepared, false, preparedScope)
				require.NoError(t, err)
				_, claimed = preparedScope.claim(root)
				require.False(t, claimed, "the explicit prepared AST must reach its own optimizer attempt")
				stringScope := NewTemporalPlanningScope(root, TemporalPlanningIntent{ParameterFree: true})
				_, err = BuildPlanWithTemporalScope(ctx, tree.NewPrepareString("temporal_text", "select 1"), false, stringScope)
				require.NoError(t, err)
				_, claimed = stringScope.claim(root)
				require.True(t, claimed, "a separately parsed PREPARE string must not consume another AST's scope")
			}
		})
	}
}

// A relation is borrowed from ordinary resolution; closing the scope releases
// its references without invoking any method on the relation itself.
func TestTemporalPlanningScopeBorrowsCanonicalRelation(t *testing.T) {
	builder, relation, _ := temporalCBOTraversalFixture(t)
	scope := builder.temporalPlanningScope
	scan := builder.qry.Nodes[0]
	got, full, read, ok := scope.NativeRelation(scan.ObjRef, scan.TableDef)
	require.True(t, ok)
	require.Same(t, relation, got)
	require.Same(t, scan.TableDef, full)
	require.Same(t, builder.compCtx.GetProcess().GetTxnOperator(), read.Op)
	wrong := DeepCopyObjectRef(scan.ObjRef)
	wrong.SchemaName = "another_database"
	_, _, _, ok = scope.NativeRelation(wrong, scan.TableDef)
	require.False(t, ok)
	wrongVersion := *scan.TableDef
	wrongVersion.Version++
	_, _, _, ok = scope.NativeRelation(scan.ObjRef, &wrongVersion)
	require.False(t, ok)
	unknownRef, unknownDef := *scan.ObjRef, *scan.TableDef
	unknownRef.Obj, unknownRef.ObjName = 9999, "unregistered"
	unknownDef.TblId, unknownDef.Name = uint64(unknownRef.Obj), unknownRef.ObjName
	_, _, _, ok = scope.NativeRelation(&unknownRef, &unknownDef)
	require.False(t, ok)
	scope.SelectQuery(builder.qry)
	require.True(t, scope.MatchesQuery(builder.qry))
	owner := &temporalScopeReleaseProbe{scope: scope}
	require.True(t, scope.SetNativeOwner(owner))
	borrowed := scope.sources
	scope.Close()
	require.Nil(t, scope.sources)
	for _, record := range borrowed {
		require.Zero(t, record)
	}
	require.Equal(t, 1, owner.calls)
	require.True(t, owner.releasedOutside)
	require.True(t, owner.cleared)
	_, _, _, ok = scope.NativeRelation(scan.ObjRef, scan.TableDef)
	require.False(t, ok)
	require.False(t, scope.AddNativeRelation(scan.ObjRef, scan.TableDef, relation, read))
	scope.Close()
	require.Equal(t, 1, owner.calls)
}

func TestTemporalPlanningScopeNativeRelationLimit(t *testing.T) {
	builder, relation, _ := temporalCBOTraversalFixture(t)
	scope := builder.temporalPlanningScope
	scan := builder.qry.Nodes[0]
	_, _, read, ok := scope.NativeRelation(scan.ObjRef, scan.TableDef)
	require.True(t, ok)

	const limit = 64
	refs := make([]*ObjectRef, 0, limit)
	defs := make([]*TableDef, 0, limit)
	refs, defs = append(refs, scan.ObjRef), append(defs, scan.TableDef)
	for i := 1; i < limit; i++ {
		ref, def := *scan.ObjRef, *scan.TableDef
		ref.Obj = int64(100 + i)
		ref.ObjName = "table_" + strconv.Itoa(i)
		def.TblId, def.Name = uint64(ref.Obj), ref.ObjName
		require.True(t, scope.AddNativeRelation(&ref, &def, relation, read))
		refs, defs = append(refs, &ref), append(defs, &def)
	}
	for i := range refs {
		got, full, _, ok := scope.NativeRelation(refs[i], defs[i])
		require.True(t, ok)
		require.Same(t, relation, got)
		require.Same(t, defs[i], full)
	}
	ref, def := *scan.ObjRef, *scan.TableDef
	ref.Obj, ref.ObjName = 1000, "overflow"
	def.TblId, def.Name = uint64(ref.Obj), ref.ObjName
	require.False(t, scope.AddNativeRelation(&ref, &def, relation, read))
	require.False(t, scope.Usable())
	_, _, _, ok = scope.NativeRelation(refs[0], defs[0])
	require.False(t, ok)
}

func TestTemporalPlanningScopeConcurrentNativeClose(t *testing.T) {
	builder, relation, _ := temporalCBOTraversalFixture(t)
	scope := builder.temporalPlanningScope
	scan := builder.qry.Nodes[0]
	_, _, read, ok := scope.NativeRelation(scan.ObjRef, scan.TableDef)
	require.True(t, ok)

	const workers = 16
	start := make(chan struct{})
	ready := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ref, def := *scan.ObjRef, *scan.TableDef
			ref.Obj, ref.ObjName = int64(1000+i), "table_"+strconv.Itoa(i)
			def.TblId, def.Name = uint64(ref.Obj), ref.ObjName
			<-start
			ready <- struct{}{}
			scope.AddNativeRelation(&ref, &def, relation, read)
			scope.NativeRelation(scan.ObjRef, scan.TableDef)
		}(i)
	}
	close(start)
	for range workers {
		<-ready
	}
	scope.Close()
	wg.Wait()
	require.False(t, scope.Usable())
	require.Nil(t, scope.sources)
	_, _, _, ok = scope.NativeRelation(scan.ObjRef, scan.TableDef)
	require.False(t, ok)
}

func TestTemporalPlanningScopeDuplicateRelationDeclines(t *testing.T) {
	builder, relation, _ := temporalCBOTraversalFixture(t)
	scope := builder.temporalPlanningScope
	scan := builder.qry.Nodes[0]
	read := engine.TemporalRead{Op: builder.compCtx.GetProcess().GetTxnOperator(), Stamp: client.CatalogReadStamp{Snapshot: timestamp.Timestamp{PhysicalTime: 1}, Workspace: &temporalCBOTestWorkspace{}}}
	otherName := *scan.ObjRef
	otherName.ObjName = "another_name"
	require.False(t, scope.AddNativeRelation(&otherName, scan.TableDef, relation, read))
	require.False(t, scope.Usable())
}
