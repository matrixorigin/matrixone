// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
)

type internalExecutorSessionKey struct{}

type internalExecutorSession interface {
	GetTempTable(dbName, alias string) (string, bool)
}

// internalExecutorPrivilegeCheckKey toggles internal SQL auth behavior:
// absent -> bypass as internal executor; present -> run normal auth checks.
type internalExecutorPrivilegeCheckKey struct{}

func attachInternalExecutorSession(ctx context.Context, ses internalExecutorSession) context.Context {
	if ses == nil {
		return ctx
	}
	return context.WithValue(ctx, internalExecutorSessionKey{}, ses)
}

func getInternalExecutorSession(ctx context.Context) internalExecutorSession {
	if ctx == nil {
		return nil
	}
	if v := ctx.Value(internalExecutorSessionKey{}); v != nil {
		if ses, ok := v.(internalExecutorSession); ok {
			return ses
		}
	}
	return nil
}

func attachInternalExecutorPrivilegeCheck(ctx context.Context) context.Context {
	// Mark this internal SQL to run with normal privilege validation path.
	return context.WithValue(ctx, internalExecutorPrivilegeCheckKey{}, true)
}

func needInternalExecutorPrivilegeCheck(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if v := ctx.Value(internalExecutorPrivilegeCheckKey{}); v != nil {
		if yes, ok := v.(bool); ok {
			return yes
		}
	}
	return false
}
