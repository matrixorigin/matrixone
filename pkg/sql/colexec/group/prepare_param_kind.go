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

package group

import "github.com/matrixorigin/matrixone/pkg/container/vector"

// Zero is reserved for an unobserved aggregate input. Observed kinds are
// shifted by one so the complete state remains one byte per aggregate.
func encodePrepareParamKindState(kind vector.PrepareParamKind, seen bool) byte {
	if !seen {
		return 0
	}
	return byte(kind) + 1
}

func decodePrepareParamKindState(encoded byte) (
	vector.PrepareParamKind,
	bool,
	bool,
) {
	if encoded == 0 {
		return vector.PrepareParamNone, false, true
	}
	kind := vector.PrepareParamKind(encoded - 1)
	if kind > vector.PrepareParamBoolean {
		return vector.PrepareParamNone, false, false
	}
	return kind, true, true
}
