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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type procedureInvocationKey struct{}

// Frames are immutable and belong to one CALL's context. Returning to the
// caller also restores its active invocations, including on error or cancel.
type procedureInvocation struct {
	database string
	routine  string
	level    int64
	parent   *procedureInvocation
}

func enterSQLProcedure(ctx context.Context, ses FeSession, database, routine string) (context.Context, error) {
	value, err := ses.GetSessionSysVar("max_sp_recursion_depth")
	if err != nil {
		return nil, err
	}
	limit, ok := value.(int64)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "invalid max_sp_recursion_depth value")
	}
	parent, _ := ctx.Value(procedureInvocationKey{}).(*procedureInvocation)
	var level int64
	for ancestor := parent; ancestor != nil; ancestor = ancestor.parent {
		if ancestor.database == database && ancestor.routine == routine {
			level = ancestor.level + 1
			break
		}
	}
	// MySQL counts the first active instance of each routine as level zero.
	// Calls to other routines (or schemas) do not consume this allowance.
	if level > limit {
		return nil, moerr.NewSPRecursionLimit(ctx, limit, routine)
	}
	return context.WithValue(ctx, procedureInvocationKey{}, &procedureInvocation{
		database: database,
		routine:  routine,
		level:    level,
		parent:   parent,
	}), nil
}
