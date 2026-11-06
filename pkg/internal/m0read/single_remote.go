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

// Package m0read carries in-process reader ownership, never RPC state.
package m0read

import "context"

type singleRemoteKey struct{}
type remoteMemberValidationKey struct{}

// WithSingleRemote marks a validated single-remote reader construction.
func WithSingleRemote(ctx context.Context) context.Context {
	return context.WithValue(ctx, singleRemoteKey{}, true)
}

// IsSingleRemote reports only a marker installed in the current process.
func IsSingleRemote(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	marked, _ := ctx.Value(singleRemoteKey{}).(bool)
	return marked
}

// WithRemoteMemberValidation requests member mapping checks before remote
// partition pruning. This marker is local to the receiving process.
func WithRemoteMemberValidation(ctx context.Context) context.Context {
	return context.WithValue(ctx, remoteMemberValidationKey{}, true)
}

func NeedsRemoteMemberValidation(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	marked, _ := ctx.Value(remoteMemberValidationKey{}).(bool)
	return marked
}
