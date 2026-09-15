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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// RequirePersistedIPFunctionProtocol validates catalog-bound expressions
// against the durable protocol floor while they are read or rebound. DDL
// construction uses RequirePersistedIPFunctionProtocolForAuthoring below so
// the phase-one admission barrier cannot publish new metadata.
//
// The owner may be an Expr or a TableDef (or another protobuf owner containing
// expressions). RequiredRemoteExpressionFeatures walks only expression roots
// and returns immediately for owners that do not use the changed IP functions.
// The protocol lookup is consequently on DDL/metadata paths, never on row
// execution hot paths.
func RequirePersistedIPFunctionProtocol(ctx context.Context, proc *process.Process, owner any) error {
	return requirePersistedIPFunctionProtocol(ctx, proc, owner, false)
}

// RequirePersistedIPFunctionProtocolForAuthoring validates a newly authored
// catalog definition. The authoring gate is raised only after the HAKeeper
// admission epoch is enabled and catalog-fenced on this CN.
func RequirePersistedIPFunctionProtocolForAuthoring(
	ctx context.Context,
	proc *process.Process,
	owner any,
) error {
	return requirePersistedIPFunctionProtocol(ctx, proc, owner, true)
}

func requirePersistedIPFunctionProtocol(
	ctx context.Context,
	proc *process.Process,
	owner any,
	authoring bool,
) error {
	requiredVersion, err := RequiredPersistedIPFunctionProtocolVersion(owner)
	if err != nil {
		return err
	}
	if requiredVersion == 0 {
		return nil
	}
	if authoring {
		return RequirePersistedProtocolVersionForAuthoring(ctx, proc, requiredVersion)
	}
	return RequirePersistedProtocolVersion(ctx, proc, requiredVersion)
}

// RequirePersistedProtocolVersion checks an explicit catalog-expression floor
// against the local deployment protocol. It is used for persisted metadata
// that was written before the version marker was introduced and must be
// rejected before local binding can expose it to execution.
func RequirePersistedProtocolVersion(
	ctx context.Context,
	proc *process.Process,
	requiredVersion int64,
) error {
	if requiredVersion <= 0 {
		return nil
	}
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			if persistedProtocolRuntimeAllows(rt, requiredVersion) {
				return nil
			}
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return moerr.NewNotSupportedf(
		ctx,
		"persisted expression semantics require all CNs to support protocol version %d",
		requiredVersion)
}

// RequirePersistedProtocolVersionForAuthoring checks the local write gate for
// a newly persisted catalog expression. During phase one the durable read floor
// is already installed, but authoring must wait until this CN receives an
// enabled, admitted, catalog-fenced snapshot.
func RequirePersistedProtocolVersionForAuthoring(
	ctx context.Context,
	proc *process.Process,
	requiredVersion int64,
) error {
	if requiredVersion <= 0 {
		return nil
	}
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			if persistedProtocolAuthoringRuntimeAllows(rt, requiredVersion) {
				return nil
			}
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return moerr.NewNotSupportedf(
		ctx,
		"persisted expression semantics require the local catalog admission protocol version %d",
		requiredVersion)
}

// RequirePersistedProtocolVersionForService is the process-independent form
// used by restore/background executors. Restore code has a service identity
// but not a planner process; it must still reject a persisted marker before
// executing its SQL on a downgraded CN.
func RequirePersistedProtocolVersionForService(
	ctx context.Context,
	service string,
	requiredVersion int64,
) error {
	if requiredVersion <= 0 {
		return nil
	}
	if service != "" {
		if rt := moruntime.ServiceRuntime(service); rt != nil {
			if persistedProtocolRuntimeAllows(rt, requiredVersion) {
				return nil
			}
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return moerr.NewNotSupportedf(
		ctx,
		"persisted expression semantics require all CNs to support protocol version %d",
		requiredVersion)
}

// persistedProtocolRuntimeAllows keeps the fast DDL/metadata admission check
// independent from row execution.  The deployment protocol must be new
// enough, and when a CN has installed the durable HAKeeper-floor key that
// floor must have reached the requested version too.  A missing floor key is
// retained as a compatibility fallback for standalone/unit-test runtimes.
func persistedProtocolRuntimeAllows(
	rt moruntime.Runtime,
	requiredVersion int64,
) bool {
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < requiredVersion {
		return false
	}
	floorValue, floorPresent := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolFloor)
	if !floorPresent {
		return true
	}
	floor, valid := floorValue.(int64)
	return valid && floor >= requiredVersion
}

func persistedProtocolAuthoringRuntimeAllows(
	rt moruntime.Runtime,
	requiredVersion int64,
) bool {
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < requiredVersion {
		return false
	}
	floorValue, floorPresent := rt.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	if !floorPresent {
		// Standalone/unit-test runtimes created before the admission protocol
		// was introduced have no write-gate key. Production CN initialization
		// always installs it, so this fallback preserves historical test setup.
		return true
	}
	floor, valid := floorValue.(int64)
	return valid && floor >= requiredVersion
}

// RequiredPersistedIPFunctionProtocolVersion reports the durable protocol
// floor needed by a catalog-bound owner. It is intentionally separate from
// the runtime check so view metadata can persist the requirement and readers
// can reapply it without rescanning SQL text.
func RequiredPersistedIPFunctionProtocolVersion(owner any) (int64, error) {
	features, err := planpb.RequiredRemoteExpressionFeatures(owner)
	if err != nil {
		return 0, err
	}
	if features.IPFunctionSemantics {
		return defines.MORPCVersion72, nil
	}
	return 0, nil
}
