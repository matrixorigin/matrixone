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

package process

import "context"

type internalExecutorSessionKey struct{}
type internalExecutorSessionValue struct{ session Session }

// ContextWithInternalExecutorSession lends the caller's session to a nested SQL
// executor. A nil session masks an inherited session when execution changes
// tenants; the context never grants unrestricted temporary-table access.
func ContextWithInternalExecutorSession(ctx context.Context, session Session) context.Context {
	if ctx == nil {
		if session == nil {
			return nil
		}
		ctx = context.Background()
	}
	if session == nil && ctx.Value(internalExecutorSessionKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, internalExecutorSessionKey{}, internalExecutorSessionValue{session})
}

func InternalExecutorSessionFromContext(ctx context.Context) Session {
	if ctx == nil {
		return nil
	}
	value, _ := ctx.Value(internalExecutorSessionKey{}).(internalExecutorSessionValue)
	return value.session
}
