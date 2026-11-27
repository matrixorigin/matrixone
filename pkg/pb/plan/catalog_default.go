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

package plan

// CatalogZeroDefault reports whether d can be represented by a schema-local
// zero Default. A nil Default and an explicitly empty unknown field must retain
// their distinct representations. This predicate does not authorize sharing
// the returned object with callers that may mutate it.
func CatalogZeroDefault(d *Default) bool {
	return d != nil && d.Expr == nil && d.OriginString == "" &&
		d.XXX_unrecognized == nil && d.XXX_sizecache == 0
}
