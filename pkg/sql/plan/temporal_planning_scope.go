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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TemporalPlanningIntent contains only one execution's placement and original
// parameter provenance. Neither the AST nor this value belongs in a plan cache.
type TemporalPlanningIntent struct {
	Placement     schedule.TemporalAccessIntent
	ParameterFree bool
	TimeZone      *time.Location
	UnixTime      int64
}

// MatchesProcess rejects any change to the execution state used by candidate
// estimation and diagnostics. Locations are immutable, and identity comparison
// conservatively declines an equivalent but independently loaded location.
func (intent TemporalPlanningIntent) MatchesProcess(proc *process.Process) bool {
	if proc == nil || proc.Base == nil || intent.TimeZone != proc.GetSessionInfo().TimeZone || intent.UnixTime != proc.Base.UnixTime {
		return false
	}
	if proc.Base.SessionInfo.ApplySQLSelectLimit {
		resolve := proc.GetResolveVariableFunc()
		if resolve == nil {
			return false
		}
		value, err := resolve(SQLSelectLimitVariable, true, false)
		limit, ok := value.(uint64)
		if err != nil || !ok || limit != ^uint64(0) {
			return false
		}
	}
	return true
}

// TemporalPlanningScope is private to one top-level Optimize attempt. A root
// identity match is necessary but not sufficient for candidate admission.
type TemporalPlanningScope struct {
	root          atomic.Pointer[tree.Select]
	intent        TemporalPlanningIntent
	admission     atomic.Uint32
	query         atomic.Pointer[Query]
	nativeMu      sync.Mutex
	sources       [64]temporalNativeBinding
	sourceCount   int
	nativeOwner   any
	ordinaryCache atomic.Bool
	cacheProof    TemporalCacheProof
}

func NewTemporalPlanningScope(root *tree.Select, intent TemporalPlanningIntent) *TemporalPlanningScope {
	if root == nil {
		return nil
	}
	scope := &TemporalPlanningScope{intent: intent}
	scope.root.Store(root)
	return scope
}

func (scope *TemporalPlanningScope) claim(root *tree.Select) (TemporalPlanningIntent, bool) {
	if scope == nil || root == nil || scope.admission.Load()&1 != 0 || !scope.root.CompareAndSwap(root, nil) {
		return TemporalPlanningIntent{}, false
	}
	return scope.intent, true
}

// All native metadata remains statement-local. The complete resolved definition
// survives the optimizer's replacement with a pruned scan definition.
type temporalNativeBinding struct {
	relation        engine.Relation
	full            *TableDef
	read            engine.TemporalRead
	database, table string
}

func (scope *TemporalPlanningScope) Invalidate() {
	if scope != nil {
		scope.admission.Or(1)
	}
}
func (scope *TemporalPlanningScope) Usable() bool {
	return scope != nil && scope.admission.Load()&1 == 0
}
func (scope *TemporalPlanningScope) SelectQuery(query *Query) {
	if scope.Usable() {
		scope.query.Store(query)
		if !scope.Usable() {
			scope.query.CompareAndSwap(query, nil)
		}
	}
}
func (scope *TemporalPlanningScope) MatchesQuery(query *Query) bool {
	return scope.Usable() && query != nil && scope.query.Load() == query
}

// AddNativeRelation borrows a canonical relation for this lexical root scope.
// Duplicates or overflow disable the optional attempt; no resource is acquired.
func (scope *TemporalPlanningScope) AddNativeRelation(ref *ObjectRef, full *TableDef, relation engine.Relation, read engine.TemporalRead) bool {
	if !scope.Usable() || ref == nil || full == nil || relation == nil || read.Op == nil || read.Stamp.Workspace == nil || read.Stamp.Snapshot.IsEmpty() || ref.PubInfo != nil || ref.SubscriptionName != "" || IsSnapshotValid(ref.Snapshot) || full.IsTemporary {
		return false
	}
	if len(ref.SchemaName) == 0 || len(ref.SchemaName) > engine.MaxTemporalNameBytes || len(ref.ObjName) == 0 || len(ref.ObjName) > engine.MaxTemporalNameBytes || ref.Obj <= 0 || uint64(ref.Obj) != full.TblId || !scope.nativeMu.TryLock() {
		return false
	}
	defer scope.nativeMu.Unlock()
	if !scope.Usable() {
		return false
	}
	for i := range scope.sourceCount {
		previous := scope.sources[i]
		if previous.full.TblId == full.TblId {
			scope.Invalidate()
			return false
		}
	}
	if scope.sourceCount == len(scope.sources) {
		scope.Invalidate()
		return false
	}
	scope.sources[scope.sourceCount] = temporalNativeBinding{relation: relation, full: full, read: read, database: ref.SchemaName, table: ref.ObjName}
	scope.sourceCount++
	return true
}

func (scope *TemporalPlanningScope) NativeRelation(ref *ObjectRef, def *TableDef) (engine.Relation, *TableDef, engine.TemporalRead, bool) {
	if !scope.Usable() || ref == nil || def == nil || ref.PubInfo != nil || ref.SubscriptionName != "" || IsSnapshotValid(ref.Snapshot) || ref.Obj <= 0 || uint64(ref.Obj) != def.TblId || !scope.nativeMu.TryLock() {
		return nil, nil, engine.TemporalRead{}, false
	}
	defer scope.nativeMu.Unlock()
	if !scope.Usable() {
		return nil, nil, engine.TemporalRead{}, false
	}
	for i := range scope.sourceCount {
		record := scope.sources[i]
		if record.full.TblId == def.TblId && record.full.Version == def.Version && record.database == ref.SchemaName && record.table == ref.ObjName {
			return record.relation, record.full, record.read, true
		}
	}
	return nil, nil, engine.TemporalRead{}, false
}

func (scope *TemporalPlanningScope) Close() {
	if scope == nil {
		return
	}
	scope.Invalidate()
	scope.root.Store(nil)
	scope.query.Store(nil)
	scope.nativeMu.Lock()
	owner := scope.nativeOwner
	clear(scope.sources[:])
	scope.sourceCount = 0
	scope.nativeOwner = nil
	scope.cacheProof = TemporalCacheProof{}
	scope.ordinaryCache.Store(false)
	scope.nativeMu.Unlock()
	if release, ok := owner.(interface{ ReleaseTemporalScope() }); ok {
		release.ReleaseTemporalScope()
	}

}

// NativeOwner is an opaque, statement-local token supplied by the real
// compiler context. It never enters the plan or a reusable compile cache.
func (scope *TemporalPlanningScope) SetNativeOwner(owner any) bool {
	if !scope.Usable() || !scope.nativeMu.TryLock() {
		return false
	}
	defer scope.nativeMu.Unlock()
	if scope.nativeOwner != nil || !scope.Usable() {
		return false
	}
	scope.nativeOwner = owner
	return true
}
func (scope *TemporalPlanningScope) NativeOwner() any {
	if !scope.Usable() || !scope.nativeMu.TryLock() {
		return nil
	}
	defer scope.nativeMu.Unlock()
	return scope.nativeOwner
}
