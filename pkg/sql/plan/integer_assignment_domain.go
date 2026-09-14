// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// integerAssignmentDomainKey is planning-only. It is not stored in a session,
// cached plan or protobuf: ordinary SELECT and CTAS keep their existing numeric
// result contracts. Integer write sources need exact execution before sharing
// a producer with predicates, grouping, windows or another assignment target.
type integerAssignmentDomainKey struct{}

func inIntegerAssignmentDomain(ctx context.Context) bool {
	enabled, _ := ctx.Value(integerAssignmentDomainKey{}).(bool)
	return enabled
}

func withIntegerAssignmentDomain(ctx context.Context) context.Context {
	if inIntegerAssignmentDomain(ctx) {
		return ctx
	}
	return context.WithValue(ctx, integerAssignmentDomainKey{}, true)
}

// enterIntegerAssignmentDomain changes only this builder. The returned restore
// function must be deferred so nested binding and errors cannot leak the mode.
func (builder *QueryBuilder) enterIntegerAssignmentDomain(enabled bool) func() {
	previous := builder.integerAssignmentDomain
	builder.integerAssignmentDomain = previous || enabled
	return func() { builder.integerAssignmentDomain = previous }
}

func hasIntegerInsertTarget(columns []string, table *TableDef) bool {
	for _, name := range columns {
		if types.T(table.Cols[table.Name2ColIndex[name]].Typ.Id).IsInteger() {
			return true
		}
	}
	return false
}
