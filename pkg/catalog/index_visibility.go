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

package catalog

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// GetIndexVisibility returns an explicitly persisted index visibility value.
//
// IndexDef.Visible is a legacy proto3 bool. Older constraints omit it for
// ordinary visible indexes, so false alone cannot distinguish legacy default
// visibility from an invisible index. Callers that need to reconstruct legacy
// DDL must reconcile an unset value with mo_indexes.is_visible.
func GetIndexVisibility(indexDef *plan.IndexDef) (visible bool, isSet bool) {
	if indexDef == nil || indexDef.Option == nil {
		return true, false
	}
	switch indexDef.Option.Visibility {
	case plan.IndexOption_VISIBILITY_VISIBLE:
		return true, true
	case plan.IndexOption_VISIBILITY_INVISIBLE:
		return false, true
	default:
		return true, false
	}
}

// SetIndexVisibility persists index visibility without relying on the legacy
// IndexDef.Visible bool. Keep that field synchronized for code paths that
// still carry it, but use IndexOption.Visibility for new durable metadata.
func SetIndexVisibility(indexDef *plan.IndexDef, visible bool) {
	if indexDef == nil {
		return
	}
	if indexDef.Option == nil {
		indexDef.Option = &plan.IndexOption{}
	}
	indexDef.Visible = visible
	if visible {
		indexDef.Option.Visibility = plan.IndexOption_VISIBILITY_VISIBLE
	} else {
		indexDef.Option.Visibility = plan.IndexOption_VISIBILITY_INVISIBLE
	}
}
