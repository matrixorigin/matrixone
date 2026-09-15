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
	features, err := planpb.RequiredRemoteExpressionFeatures(owner)
	if err != nil {
		return err
	}
	if !features.IPFunctionSemantics {
		return nil
	}
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			version, valid := value.(int64)
			if ok && valid && version >= defines.MORPCVersion72 {
				return nil
			}
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return moerr.NewNotSupported(
		ctx,
		"persisted IP function expressions require all CNs to support protocol version 72",
	)
}
