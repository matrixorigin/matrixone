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

package engine

import "context"

type collectChangesDebugLabelKey struct{}

// WithCollectChangesDebugLabel attaches a narrow debug label to the context.
// CollectChanges paths can use this to emit focused diagnostics without
// turning on high-volume logs for unrelated callers.
func WithCollectChangesDebugLabel(ctx context.Context, label string) context.Context {
	if ctx == nil || label == "" {
		return ctx
	}
	return context.WithValue(ctx, collectChangesDebugLabelKey{}, label)
}

// CollectChangesDebugLabelFromContext returns the debug label attached by
// WithCollectChangesDebugLabel, or the empty string when none was set.
func CollectChangesDebugLabelFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	label, _ := ctx.Value(collectChangesDebugLabelKey{}).(string)
	return label
}
