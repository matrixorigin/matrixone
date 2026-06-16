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

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type internalExecutorSessionKey struct{}

// internalExecutorPrivilegeCheckKey toggles internal SQL auth behavior:
// absent -> bypass as internal executor; present -> run normal auth checks.
type internalExecutorPrivilegeCheckKey struct{}

func attachInternalExecutorSession(ctx context.Context, ses process.Session) context.Context {
	if ses == nil {
		return ctx
	}
	return context.WithValue(ctx, internalExecutorSessionKey{}, ses)
}

// AttachInternalExecutorSession attaches the original frontend session to
// internal SQL so temp-table aliases and other session-scoped metadata resolve
// the same way they do for the user's statement.
func AttachInternalExecutorSession(ctx context.Context, ses process.Session) context.Context {
	return attachInternalExecutorSession(ctx, ses)
}

func getInternalExecutorSession(ctx context.Context) process.Session {
	if ctx == nil {
		return nil
	}
	if v := ctx.Value(internalExecutorSessionKey{}); v != nil {
		if ses, ok := v.(process.Session); ok {
			return ses
		}
	}
	return nil
}

func attachInternalExecutorPrivilegeCheck(ctx context.Context) context.Context {
	// Mark this internal SQL to run with normal privilege validation path.
	return context.WithValue(ctx, internalExecutorPrivilegeCheckKey{}, true)
}

// AttachInternalExecutorPrivilegeCheck forces internal SQL to run through the
// normal privilege validation path instead of bypassing auth as trusted SQL.
func AttachInternalExecutorPrivilegeCheck(ctx context.Context) context.Context {
	return attachInternalExecutorPrivilegeCheck(ctx)
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
