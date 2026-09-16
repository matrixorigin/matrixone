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

package plan

import "github.com/matrixorigin/matrixone/pkg/container/types"

// isVersionedCollationType is the schema-level predicate shared by the
// admission fence and later index builders. Keeping it in the metadata/tuple
// layer makes the PR2 prefix independently buildable; index DDL must not be
// the only place that knows which text types require a V1 comparison key.
func isVersionedCollationType(typ Type) bool {
	if !types.T(typ.Id).IsMySQLString() {
		return false
	}
	runtimeType := types.NewWithCharsetVersion(
		types.T(typ.Id), typ.Width, typ.Scale,
		uint8(typ.Charset), uint8(typ.CollationVersion),
	)
	return types.NeedsCollationKey(runtimeType, types.PADSpaceKeyV1)
}
