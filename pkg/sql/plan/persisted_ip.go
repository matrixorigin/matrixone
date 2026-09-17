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

// RequirePersistedExpressionProtocol validates catalog-bound expressions
// against the durable protocol floor while they are read or rebound.
// Authoring uses the explicit ForAuthoring variant below so the phase-one
// admission barrier cannot publish metadata before the catalog fence.
//
// The owner may be an Expr or a TableDef (or another protobuf owner containing
// expressions). RequiredRemoteExpressionFeatures walks only expression roots,
// so this check remains on DDL/metadata paths rather than row execution.
func RequirePersistedExpressionProtocol(ctx context.Context, proc *process.Process, owner any) error {
	return requirePersistedExpressionProtocol(ctx, proc, owner, false)
}

// RequirePersistedExpressionProtocolForAuthoring validates a newly authored
// catalog definition after the HAKeeper admission epoch is enabled and
// catalog-fenced on this CN.
func RequirePersistedExpressionProtocolForAuthoring(
	ctx context.Context,
	proc *process.Process,
	owner any,
) error {
	return requirePersistedExpressionProtocol(ctx, proc, owner, true)
}

// RequirePersistedIPFunctionProtocol is retained for source compatibility
// with existing catalog builders. It now shares the complete feature walk so
// IP and string numeric requirements cannot mask one another.
func RequirePersistedIPFunctionProtocol(ctx context.Context, proc *process.Process, owner any) error {
	return RequirePersistedExpressionProtocol(ctx, proc, owner)
}

// RequirePersistedIPFunctionProtocolForAuthoring is the compatibility wrapper
// for existing DDL callers that need the write-side admission gate.
func RequirePersistedIPFunctionProtocolForAuthoring(
	ctx context.Context,
	proc *process.Process,
	owner any,
) error {
	return RequirePersistedExpressionProtocolForAuthoring(ctx, proc, owner)
}

func requirePersistedExpressionProtocol(
	ctx context.Context,
	proc *process.Process,
	owner any,
	authoring bool,
) error {
	requiredVersion, err := RequiredPersistedExpressionProtocolVersion(owner)
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

// RequiredPersistedExpressionProtocolVersion reports the durable floor needed
// by a catalog-bound owner. Keep this separate from runtime checks so VIEW
// metadata can persist the requirement and readers can reapply it without
// relying on a transient placement decision.
func RequiredPersistedExpressionProtocolVersion(owner any) (int64, error) {
	features, err := planpb.RequiredRemoteExpressionFeatures(owner)
	if err != nil {
		return 0, err
	}
	requiredVersion := int64(0)
	if features.IPFunctionSemantics {
		requiredVersion = defines.MORPCVersion72
	}
	if features.StringNumericResultContracts && requiredVersion < defines.MORPCVersion80 {
		requiredVersion = defines.MORPCVersion80
	}
	if features.BoundedConditionalStringDomains && requiredVersion < defines.MORPCVersion83 {
		requiredVersion = defines.MORPCVersion83
	}
	if features.ExportSetNumericContracts && requiredVersion < defines.MORPCVersion84 {
		requiredVersion = defines.MORPCVersion84
	}
	return requiredVersion, nil
}

// RequiredPersistedIPFunctionProtocolVersion is retained for callers that
// used the old name; the returned floor includes all protected expression
// contracts so a mixed owner is never admitted at an incomplete version.
func RequiredPersistedIPFunctionProtocolVersion(owner any) (int64, error) {
	return RequiredPersistedExpressionProtocolVersion(owner)
}

// RequirePersistedProtocolVersion checks an explicit catalog-expression floor
// against the local deployment protocol. It rejects old or not-yet-fenced CNs
// before local binding can expose persisted metadata to execution.
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
// a newly persisted catalog expression. The read floor is insufficient during
// phase one: authoring waits for an enabled, admitted, catalog-fenced snapshot.
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
// used by restore/background executors that only have a service identity.
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
// independent from row execution. A missing floor key is retained only for
// standalone/unit-test runtimes created before the admission protocol.
func persistedProtocolRuntimeAllows(rt moruntime.Runtime, requiredVersion int64) bool {
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < requiredVersion {
		return false
	}
	floorValue, floorPresent := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	if !floorPresent {
		return true
	}
	floor, valid := floorValue.(int64)
	return valid && floor >= requiredVersion
}

func persistedProtocolAuthoringRuntimeAllows(rt moruntime.Runtime, requiredVersion int64) bool {
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < requiredVersion {
		return false
	}
	floorValue, floorPresent := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	if !floorPresent {
		// Production CN initialization always installs this key; the fallback
		// preserves historical standalone/unit-test runtime setup.
		return true
	}
	floor, valid := floorValue.(int64)
	return valid && floor >= requiredVersion
}
