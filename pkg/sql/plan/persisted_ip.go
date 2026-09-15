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

// RequirePersistedIPFunctionProtocol admits catalog-bound expressions only
// after the deployment-managed common protocol reaches v72. Unlike a remote
// pipeline, a catalog default/generated/check/on-update expression can be
// evaluated locally by an older CN and therefore bypasses the per-send
// capability check. Call this before folding a newly bound expression and at
// the final TableDef publication boundary.
//
// The owner may be an Expr or a TableDef (or another protobuf owner containing
// expressions). RequiredRemoteExpressionFeatures walks only expression roots
// and returns immediately for owners that do not use the changed IP functions.
// The protocol lookup is consequently on DDL/metadata paths, never on row
// execution hot paths.
func RequirePersistedIPFunctionProtocol(ctx context.Context, proc *process.Process, owner any) error {
	requiredVersion, err := RequiredPersistedIPFunctionProtocolVersion(owner)
	if err != nil {
		return err
	}
	if requiredVersion == 0 {
		return nil
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
