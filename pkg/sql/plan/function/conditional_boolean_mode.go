// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type mysqlConditionalBooleanContextKey struct{}

// WithMySQLConditionalBooleanContracts carries the statement's
// MatrixOneNativeMode decision to the boolean admission sites (IF/CASE
// condition typing, the boolean-context display rewrites). Native mode binds
// with the exact pre-contract behavior; the default MySQL-compatible session
// binds with the ENUM/SET/BIT storage truth and the in-domain JSON conditional
// semantics (docs/design/20261027-implicit-boolean-semantics-enum-set-bit-json.md,
// decision D11: one scalar resolved from the session flag, carried on the
// BindContext and attached here from ddlExpressionContext).
func WithMySQLConditionalBooleanContracts(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, mysqlConditionalBooleanContextKey{}, enabled)
}

// MySQLConditionalBooleanContracts reports whether the current statement binds
// boolean contexts with the MySQL conditional boolean contracts. An unset
// value means the default MySQL-compatible session, so admission stays
// conservative only when a carrier explicitly attaches a native-mode
// decision. This mirrors the legacy WithLegacyX binding family: the setting
// must never affect ordinary binding that has not carried the scalar.
//
// A nil context carries no statement at all - the LOAD assignment rewrites
// (build_load.go) and unit-test plan harnesses bind on a nil context, exactly
// like the legacy binding family. Keep those paths on the default
// MySQL-compatible binding instead of panicking inside the resolver.
func MySQLConditionalBooleanContracts(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	value, ok := ctx.Value(mysqlConditionalBooleanContextKey{}).(bool)
	if !ok {
		return true
	}
	return value
}

// MySQLConditionalBooleanContractsFromProcess resolves the statement setting
// from the session the executors read: a native-mode session disables the
// optional contracts, every other session keeps them. This is the single
// resolver; the BindContext scalar (SELECT planning) and the
// ddlExpressionContext attachment (standalone DDL binders) both derive from
// it, so DDL-authored expressions bind with the same statement setting as
// ordinary SELECT expressions.
func MySQLConditionalBooleanContractsFromProcess(proc *process.Process) bool {
	if proc == nil || proc.GetSessionInfo() == nil {
		return true
	}
	return !proc.GetSessionInfo().MatrixOneNativeMode
}
