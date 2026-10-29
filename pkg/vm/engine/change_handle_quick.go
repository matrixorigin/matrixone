// Copyright 2026 Matrix Origin
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

package engine

import "context"

type collectChangesDisableQuickKey struct{}

// WithCollectChangesDisableQuick prevents a range probe from materializing
// an entire insert-only or delete-only range in one Next call.
func WithCollectChangesDisableQuick(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, collectChangesDisableQuickKey{}, true)
}

func CollectChangesDisableQuickFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	disable, _ := ctx.Value(collectChangesDisableQuickKey{}).(bool)
	return disable
}
