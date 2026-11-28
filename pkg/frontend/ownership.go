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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// executionOwnerOf extracts the genuinely executable SELECT identity entering
// the frontend executor: the SELECT itself, or the parenthesized wrapper's
// body that owns the Ep. Any other statement stays ownership-neutral.
func executionOwnerOf(stmt tree.Statement) *tree.Select {
	switch s := stmt.(type) {
	case *tree.Select:
		return s
	case *tree.ParenSelect:
		return s.Select
	}
	return nil
}
