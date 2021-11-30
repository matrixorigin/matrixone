// Copyright 2021 Matrix Origin
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

package rewrite

import "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

func Rewrite(stmt tree.Statement) tree.Statement {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return rewriteSelect(stmt)
	case *tree.ParenSelect:
		stmt.Select = rewriteSelect(stmt.Select)
		return stmt
	case *tree.Insert:
		return rewriteInsert(stmt)
	}
	return stmt
}
