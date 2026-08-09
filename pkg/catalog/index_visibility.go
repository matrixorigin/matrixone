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

package catalog

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// IsIndexVisible reports whether an index is eligible for optimizer use.
//
// IndexDef.Visible predates optimizer visibility enforcement and is a proto3
// bool, so old metadata cannot distinguish an omitted value from an explicitly
// invisible index. VisibilitySet provides that distinction for newly written
// metadata. Treating legacy metadata as visible preserves upgrade compatibility
// instead of disabling every pre-existing index whose Visible field is absent.
func IsIndexVisible(indexDef *plan.IndexDef) bool {
	return indexDef != nil && (!indexDef.VisibilitySet || indexDef.Visible)
}

// SetIndexVisibility records an explicit visibility value on an IndexDef.
func SetIndexVisibility(indexDef *plan.IndexDef, visible bool) {
	if indexDef == nil {
		return
	}
	indexDef.Visible = visible
	indexDef.VisibilitySet = true
}
