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

package morpc

import "context"

type maxMessageSizeContextKey struct{}

// ContextWithMaxMessageSize carries the body limit configured on the RPC
// codec to an application handler. A zero value means that the codec uses its
// default limit and is therefore omitted from the context lookup result.
func ContextWithMaxMessageSize(ctx context.Context, size uint64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, maxMessageSizeContextKey{}, size)
}

// MaxMessageSizeFromContext returns the configured codec body limit when one
// was supplied by the owning RPC server. Invalid or zero values fall back to
// the codec package default at the call site.
func MaxMessageSizeFromContext(ctx context.Context) (int, bool) {
	if ctx == nil {
		return 0, false
	}
	size, ok := ctx.Value(maxMessageSizeContextKey{}).(uint64)
	if !ok || size == 0 || size > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(size), true
}
