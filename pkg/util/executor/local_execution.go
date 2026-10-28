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

package executor

import "context"

type localExecutionKey struct{}

// WithLocalExecution confines an internal SQL operation and its nested SQL
// to its current CN. Bootstrap uses it before query-ready CNs are available.
func WithLocalExecution(ctx context.Context) context.Context {
	return context.WithValue(ctx, localExecutionKey{}, true)
}

// RequiresLocalExecution reports whether an operation must remain on its CN.
func RequiresLocalExecution(ctx context.Context) bool {
	local, _ := ctx.Value(localExecutionKey{}).(bool)
	return local
}
