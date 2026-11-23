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

package process

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type trustedIndexRelationAccessKey struct{}

// WithTrustedIndexRelationAccess is reserved for server-authored SQL. The
// caller must not apply it to user-supplied routine or expression SQL.
func WithTrustedIndexRelationAccess(ctx context.Context) context.Context {
	return context.WithValue(ctx, trustedIndexRelationAccessKey{}, true)
}

// WithUserIndexRelationAccess removes an inherited trusted capability before
// entering SQL supplied by a client-owned stored routine.
func WithUserIndexRelationAccess(ctx context.Context) context.Context {
	return context.WithValue(ctx, trustedIndexRelationAccessKey{}, false)
}

// CheckIndexRelationAccess checks an explicit relation target after its name
// has been evaluated. Optimizer-generated lookups do not call this function.
// SessionInfo identity is captured by the frontend and carried to remote CNs.
func CheckIndexRelationAccess(proc *Process, name, kind string, flags uint64) error {
	if !catalog.RequiresPrivilegedRelationAccess(name, kind, flags) {
		return nil
	}
	if proc != nil && proc.Base != nil {
		if proc.Ctx != nil {
			if trusted, _ := proc.Ctx.Value(trustedIndexRelationAccessKey{}).(bool); trusted {
				return nil
			}
		}
		info := proc.GetSessionInfo()
		if info.User != "" && strings.EqualFold(info.Account, "sys") && strings.EqualFold(info.Role, "moadmin") {
			return nil
		}
		return moerr.NewInternalError(proc.Ctx, "do not have privilege to execute the statement")
	}
	return moerr.NewInternalErrorNoCtx("do not have privilege to execute the statement")
}

// CheckIndexRelationDefinitionAccess checks an already resolved target. A
// missing definition cannot safely be treated as an ordinary relation.
func CheckIndexRelationDefinitionAccess(proc *Process, name string, def *plan.TableDef) error {
	if def == nil {
		return moerr.NewInternalErrorNoCtx("table definition is unavailable for index relation admission")
	}
	return CheckIndexRelationAccess(proc, name, def.TableType, def.FeatureFlag)
}
