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

package function

import "context"

type legacyJSONBindingContextKey struct{}

// WithLegacyJSONBinding reconstructs the pre-#30806 JSON semantics only while
// verifying complete persisted expression trees for DUMP/LOAD. It must not be
// used by ordinary SQL or new DDL, nor escape the verifier's private process.
func WithLegacyJSONBinding(ctx context.Context) context.Context {
	return context.WithValue(ctx, legacyJSONBindingContextKey{}, true)
}

func legacyJSONBinding(ctx context.Context) bool {
	legacy, _ := ctx.Value(legacyJSONBindingContextKey{}).(bool)
	return legacy
}
