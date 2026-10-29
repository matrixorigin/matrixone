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

import "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

// CollectDeleteSourceAliases maps explicit FROM aliases to their source schema
// and table. Omitted schemas remain empty for the consumer's statement default.
// Derived sources have no base table identity; their validation stays in binding.
func CollectDeleteSourceAliases(tableRefs tree.TableExprs) map[string][2]string {
	aliases := make(map[string][2]string)
	for _, ref := range tableRefs {
		collectDeleteSourceAlias(ref, "", aliases)
	}
	return aliases
}

func collectDeleteSourceAlias(expr tree.TableExpr, alias string, aliases map[string][2]string) {
	switch t := expr.(type) {
	case *tree.TableName:
		if alias != "" {
			aliases[alias] = [2]string{string(t.SchemaName), string(t.ObjectName)}
		}
	case *tree.AliasedTableExpr:
		collectDeleteSourceAlias(t.Expr, string(t.As.Alias), aliases)
	case *tree.JoinTableExpr:
		collectDeleteSourceAlias(t.Left, alias, aliases)
		collectDeleteSourceAlias(t.Right, alias, aliases)
	}
}

// ResolveDMLTargetName applies DELETE's source aliases before catalog resolution
// or temporary-table classification. An alias hit replaces even a qualified
// target's schema, while retaining its original object name as the binding alias.
// Other DML callers pass nil aliases and keep their original names unchanged.
func ResolveDMLTargetName(dbName, tableName, alias string, aliases map[string][2]string) (string, string, string) {
	if source, ok := aliases[tableName]; ok {
		return source[0], source[1], tableName
	}
	return dbName, tableName, alias
}
