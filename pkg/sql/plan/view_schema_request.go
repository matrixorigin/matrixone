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
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	viewSchemaInputLimit    = 16 << 20
	viewSchemaResultLimit   = 16 << 20
	viewSchemaMemoLimit     = 16 << 20
	viewSchemaMemoEntries   = 4096
	viewSchemaDepthLimit    = 64
	viewSchemaRootWorkLimit = 4096
	viewSchemaWorkLimit     = 65536
)

var (
	ErrViewSchemaChanged = errors.New("view schema visibility changed; retry the statement")
	ErrViewSchemaClosed  = errors.New("view schema request is closed")
	ErrViewSchemaBusy    = errors.New("view schema request already has an active binder")
	ErrViewSchemaLimit   = errors.New("view schema request resource limit exceeded")
)

// ViewSchemaBinding belongs to one statement. Check must reject a change of
// transaction, workspace visibility, authorization identity or execution
// generation. Compiler must own its mutable binding state and borrow only the
// statement's read domain. Close never commits or closes the borrowed transaction.
// Authorize runs for EVERY root, including memo hits, before catalog resolution.
type ViewSchemaBinding struct {
	Compiler   CompilerContext
	Generation *process.ExecutionResourceGeneration
	Check      func() error
	Authorize  func(context.Context, string, string, *Snapshot) error
	Close      func()
}

type ViewSchemaProvider interface {
	OpenViewSchemaBinding(context.Context) (*ViewSchemaBinding, error)
}

// ViewSchemaRequest is an opt-in, read-only derivation boundary. Existing SQL
// entry points do not construct it. Its provider is acquired lazily at the first
// Describe; no worker, catalog write or cross-statement cache is created.
type ViewSchemaRequest struct {
	ctx          context.Context
	workCtx      context.Context
	cancel       context.CancelCauseFunc
	provider     ViewSchemaProvider
	gate         chan struct{}
	closeOnce    sync.Once
	readers      sync.WaitGroup
	binding      *ViewSchemaBinding
	stopDeadline context.CancelFunc
	memo         map[[32]byte]*viewSchemaMemoEntry
	nestedMemo   map[[32]byte]*viewSchemaMemoEntry
	memoBytes    int
	work, roots  int
	binds, hits  uint64
	memoDisabled bool
}

type viewSchemaMemoEntry struct {
	columns            []byte
	provenance         []byte
	requiredProtocol   int64
	dependencies       []byte
	boundary           []byte
	work, depth, slots int
	lease              *process.ExecutionTransientMemoryReservation
}

// ViewSchemaResult keeps its encoded value private. Callers receive independent
// column/dependency graphs, and must Release the handle before closing the
// request. Release waits for current readers; results remain valid after cancel.
type ViewSchemaResult struct {
	mu                                sync.RWMutex
	columns, dependencies, provenance []byte
	requiredProtocol                  int64
	lease                             *process.ExecutionTransientMemoryReservation
	request                           *ViewSchemaRequest
}

func NewViewSchemaRequest(ctx context.Context, provider ViewSchemaProvider) *ViewSchemaRequest {
	ctx, cancel := context.WithCancelCause(ctx)
	r := &ViewSchemaRequest{ctx: ctx, cancel: cancel, provider: provider, gate: make(chan struct{}, 1)}
	r.gate <- struct{}{}
	return r
}

func (r *ViewSchemaRequest) Describe(database, name string, snapshot *Snapshot) (result *ViewSchemaResult, err error) {
	select {
	case <-r.ctx.Done():
		return nil, context.Cause(r.ctx)
	case <-r.gate:
	default:
		return nil, ErrViewSchemaBusy
	}
	completed := false
	defer func() {
		if !completed {
			r.clearMemo()
		}
		r.gate <- struct{}{}
	}()
	if len(database) > viewSchemaInputLimit || len(name) > viewSchemaInputLimit-len(database) {
		return nil, ErrViewSchemaLimit
	}
	if err = r.open(); err != nil {
		return nil, err
	}
	if r.roots == viewSchemaWorkLimit {
		return nil, ErrViewSchemaLimit
	}
	r.roots++
	if snapshot == nil {
		snapshot = r.binding.Compiler.GetSnapshot()
	}
	if snapshot != nil && snapshot.ProtoSize() > viewSchemaInputLimit {
		return nil, ErrViewSchemaLimit
	}
	snapshot = DeepCopySnapshot(snapshot)
	if err = r.binding.Authorize(r.workCtx, database, name, snapshot); err != nil {
		return nil, r.cause(err)
	}
	if err = r.check(); err != nil {
		return nil, err
	}
	state := newViewSchemaDerivation(r)
	defer state.close()
	value, err := state.describe(database, name, snapshot)
	if err != nil {
		return nil, r.cause(err)
	}
	if err = r.check(); err != nil {
		return nil, err
	}
	state.close()
	lease, err := r.reserve(len(value.columns) + len(value.dependencies) + len(value.provenance))
	if err != nil {
		return nil, err
	}
	defer func() {
		if !completed {
			lease.Release()
		}
	}()
	if err = r.check(); err != nil {
		return nil, err
	}
	result = &ViewSchemaResult{columns: append([]byte(nil), value.columns...), dependencies: append([]byte(nil), value.dependencies...), provenance: append([]byte(nil), value.provenance...), requiredProtocol: value.requiredProtocol, lease: lease, request: r}
	r.readers.Add(1)
	completed = true
	return result, nil
}

func (r *ViewSchemaRequest) open() error {
	if err := context.Cause(r.ctx); err != nil {
		return err
	}
	if r.binding != nil {
		return r.check()
	}
	if r.provider == nil {
		return moerr.NewInternalError(r.ctx, "view schema provider is unavailable")
	}
	if r.workCtx == nil {
		r.workCtx, r.stopDeadline = context.WithTimeout(r.ctx, 30*time.Second)
	}
	ctx := r.workCtx
	binding, err := r.provider.OpenViewSchemaBinding(ctx)
	if err != nil {
		if binding != nil && binding.Close != nil {
			binding.Close()
		}
		return r.cause(err)
	}
	if binding == nil || binding.Compiler == nil || binding.Generation == nil || binding.Check == nil || binding.Authorize == nil || binding.Close == nil {
		if binding != nil && binding.Close != nil {
			binding.Close()
		}
		return moerr.NewInternalError(ctx, "incomplete view schema binding")
	}
	r.binding = binding
	r.memo = make(map[[32]byte]*viewSchemaMemoEntry)
	r.nestedMemo = make(map[[32]byte]*viewSchemaMemoEntry)
	return r.check()
}

func (r *ViewSchemaRequest) cause(err error) error {
	if cause := context.Cause(r.ctx); cause != nil {
		return cause
	}
	if r.workCtx != nil {
		if cause := context.Cause(r.workCtx); cause != nil {
			return cause
		}
	}
	return err
}

func (r *ViewSchemaRequest) check() error {
	if err := r.cause(nil); err != nil {
		return err
	}
	if r.binding.Generation.Closed() {
		return ErrViewSchemaClosed
	}
	return r.cause(r.binding.Check())
}

func (r *ViewSchemaRequest) clearMemo() {
	for key, entry := range r.nestedMemo {
		if entry.lease != nil {
			entry.lease.Release()
		}
		delete(r.nestedMemo, key)
	}
	for key, entry := range r.memo {
		if entry.lease != nil {
			entry.lease.Release()
		}
		delete(r.memo, key)
	}
	r.memoBytes = 0
}

func (r *ViewSchemaRequest) Close() {
	r.cancel(ErrViewSchemaClosed)
	r.closeOnce.Do(func() {
		<-r.gate
		defer func() { r.gate <- struct{}{} }()
		r.readers.Wait()
		r.clearMemo()
		if r.stopDeadline != nil {
			r.stopDeadline()
		}
		if r.binding != nil {
			r.binding.Close()
			r.binding = nil
		}
		r.provider = nil
	})
}

func (r *ViewSchemaResult) Columns() ([]*ColDef, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.request == nil {
		return nil, ErrViewSchemaClosed
	}
	var table planpb.TableDef
	if err := table.Unmarshal(r.columns); err != nil {
		return nil, err
	}
	return table.Cols, nil
}

func (r *ViewSchemaResult) Dependencies() ([]ViewDependency, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.request == nil {
		return nil, ErrViewSchemaClosed
	}
	var deps []ViewDependency
	if err := json.Unmarshal(r.dependencies, &deps); err != nil {
		return nil, err
	}
	return deps, nil
}

func (r *ViewSchemaResult) Release() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.request == nil {
		return
	}
	r.columns, r.dependencies, r.provenance = nil, nil, nil
	r.lease.Release()
	r.request.readers.Done()
	r.request, r.lease = nil, nil
}

func viewSchemaKey(obj *ObjectRef, def *TableDef, snapshot *Snapshot) [32]byte {
	// Hashing bounded SQL avoids retaining one SQL copy for every dependency.
	identity, _ := json.Marshal(struct {
		Object      *ObjectRef
		ID, Logical uint64
		Version     uint32
		Snapshot    *Snapshot
	}{obj, def.TblId, def.LogicalId, def.Version, snapshot})
	h := sha256.New()
	h.Write(identity)
	h.Write([]byte(def.ViewSql.View))
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

func (r *ViewSchemaRequest) reserve(size int) (*process.ExecutionTransientMemoryReservation, error) {
	lease, err := r.binding.Generation.ReserveTransientMemory(uint64(size))
	if err != nil && (len(r.memo) > 0 || len(r.nestedMemo) > 0) {
		r.clearMemo()
		return r.binding.Generation.ReserveTransientMemory(uint64(size))
	}
	return lease, err
}
func (r *ViewSchemaRequest) remember(key [32]byte, value *viewSchemaMemoEntry) {
	r.rememberIn(r.memo, key, value)
}
func (r *ViewSchemaRequest) rememberIn(destination map[[32]byte]*viewSchemaMemoEntry, key [32]byte, value *viewSchemaMemoEntry) {
	if r.memoDisabled || len(r.memo)+len(r.nestedMemo) >= viewSchemaMemoEntries {
		return
	}
	size := len(value.columns) + len(value.dependencies) + len(value.provenance) + len(value.boundary) + 128
	if size > viewSchemaMemoLimit-r.memoBytes {
		return
	}
	lease, err := r.binding.Generation.ReserveTransientMemory(uint64(size))
	if err != nil {
		return
	} // Optional retention never makes a valid result fail.
	copy := *value
	copy.columns = append([]byte(nil), value.columns...)
	copy.provenance = append([]byte(nil), value.provenance...)
	copy.dependencies = append([]byte(nil), value.dependencies...)
	copy.boundary = append([]byte(nil), value.boundary...)
	copy.lease = lease
	if old := destination[key]; old != nil {
		lease.Release()
		return
	}
	destination[key] = &copy
	r.memoBytes += size
}
