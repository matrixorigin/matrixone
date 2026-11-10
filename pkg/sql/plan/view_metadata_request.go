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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type viewMetadataRequestKey struct{}

// ViewMetadataRequest is the opt-in owner shared by a statement's SHOW and
// relational metadata consumers. The caller must keep it alive through operator
// cleanup, release every result, and then Close it. It is neither a session cache
// nor an activation flag: normal SQL does not construct this owner.
//
// Parallel APPLY/UNION consumers serialize only their binding work. They retain
// independent results, and share the schema request's work/deadline/memo bounds.
// The provider remains responsible for authorizing EVERY root before resolution.
// Authorization SQL and the isolated schema binder do not inherit this marker.
type ViewMetadataRequest struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	schema *ViewSchemaRequest
	gate   chan struct{}
}

func NewViewMetadataRequest(ctx context.Context, provider ViewSchemaProvider) *ViewMetadataRequest {
	ctx, cancel := context.WithCancelCause(ctx)
	r := &ViewMetadataRequest{cancel: cancel, schema: NewViewSchemaRequest(ctx, provider), gate: make(chan struct{}, 1)}
	r.ctx = context.WithValue(ctx, viewMetadataRequestKey{}, r)
	r.gate <- struct{}{}
	return r
}

// Context must reach both the planner and the origin-CN execution processes.
// It is not serialized to remote workers or stored in reusable plans.
func (r *ViewMetadataRequest) Context() context.Context { return r.ctx }

func (r *ViewMetadataRequest) Close() {
	r.cancel(ErrViewSchemaClosed)
	r.schema.Close()
}

func viewMetadataRequest(ctx context.Context) *ViewMetadataRequest {
	r, _ := ctx.Value(viewMetadataRequestKey{}).(*ViewMetadataRequest)
	return r
}

func HasViewMetadataRequest(ctx context.Context) bool { return viewMetadataRequest(ctx) != nil }

// DescribeViewMetadata returns a complete result, never a stale fallback. The
// consumer owns Release, including on formatting errors and early termination.
func DescribeViewMetadata(ctx context.Context, database, name string, snapshot *Snapshot) (*ViewSchemaResult, error) {
	result, _, err := DescribeViewMetadataCandidate(ctx, database, name, snapshot)
	return result, err
}

// DescribeViewMetadataCandidate additionally reports proven invalid-definition
// errors that a bulk scan may skip. Authorization, root lookup and provider
// failures remain fatal, even if they use a missing-object error code.
func DescribeViewMetadataCandidate(ctx context.Context, database, name string, snapshot *Snapshot) (*ViewSchemaResult, bool, error) {
	r := viewMetadataRequest(ctx)
	if r == nil {
		return nil, false, moerr.NewNotSupported(ctx, "derived View metadata requires its statement request")
	}
	// The marker can be inherited by child SQL, but must never authorize a
	// consumer that changed the caller's tenant, user, or active role.
	for _, key := range []any{defines.TenantIDKey{}, defines.UserIDKey{}, defines.RoleIDKey{}} {
		expected, present := r.ctx.Value(key).(uint32)
		actual, actualPresent := ctx.Value(key).(uint32)
		if present != actualPresent || expected != actual {
			return nil, false, ErrViewSchemaChanged
		}
	}
	select {
	case <-ctx.Done():
		return nil, false, context.Cause(ctx)
	case <-r.ctx.Done():
		return nil, false, context.Cause(r.ctx)
	case <-r.gate:
	}
	defer func() { r.gate <- struct{}{} }()
	if err := context.Cause(ctx); err != nil {
		return nil, false, err
	}
	// Cancellation of the active consumer interrupts catalog work as well as
	// waiters. Stop the callback before returning; a normally completed branch
	// must not cancel its sibling's later binding when its process is freed.
	stop := context.AfterFunc(ctx, func() { r.cancel(context.Cause(ctx)) })
	defer stop()
	result, err := r.schema.describe(database, name, snapshot)
	if cause := context.Cause(ctx); cause != nil {
		result.Release()
		return nil, false, cause
	}
	_, definitionFailure := err.(*viewSchemaDefinitionError)
	err = unwrapViewSchemaDefinitionError(err)
	invalid := definitionFailure && (moerr.IsMoErrCode(err, moerr.ErrNoDB) ||
		moerr.IsMoErrCode(err, moerr.ErrBadDB) || moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
		moerr.IsMoErrCode(err, moerr.ErrBadFieldError))
	return result, invalid, err
}

func viewMetadataDependencyRefs(dependencies []ViewDependency, accountID uint32) []*ObjectRef {
	refs := make([]*ObjectRef, 0, len(dependencies))
	for _, dep := range dependencies {
		ref := &ObjectRef{
			Server: int64(dep.Version), Db: int64(dep.DatabaseID), Schema: int64(dep.DatabaseID),
			Obj: int64(dep.RelationID), SchemaName: dep.DatabaseName, ObjName: dep.RelationName,
			SubscriptionName: dep.SubscriptionName, Snapshot: DeepCopySnapshot(dep.Snapshot),
		}
		if dep.AccountID != accountID || dep.SubscriptionName != "" {
			ref.PubInfo = &planpb.PubInfo{TenantId: int32(dep.AccountID)}
		}
		refs = append(refs, ref)
	}
	return refs
}
