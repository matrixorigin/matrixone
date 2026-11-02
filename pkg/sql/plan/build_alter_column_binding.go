// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// alterColumnBinding describes index-reference identity, not the data source of
// a column. COPY owns data identity separately in AlterTableContext.alterColMap.
// Ordinals remain stable when generic constraint drops are resolved.
type alterColumnBindings struct {
	clauses       []alterColumnBinding
	sourceOrigins map[string]string
}

type alterColumnBinding struct {
	origin          string
	preserveIndexes bool
}

// analyzeAlterColumnBindings shares ordered index naming between COPY and
// generic constraint resolution. Only a fresh column can replace a dropped
// binding; renaming a surviving source column into its spelling cannot.
func analyzeAlterColumnBindings(source *TableDef, options tree.AlterTableOptions) alterColumnBindings {
	needsBindings := false
	for _, option := range options {
		switch opt := option.(type) {
		case *tree.AlterAddCol, *tree.AlterTableModifyColumnClause,
			*tree.AlterTableRenameColumnClause, *tree.AlterTableChangeColumnClause:
			needsBindings = true
		case *tree.AlterOptionDrop:
			needsBindings = needsBindings || opt.Typ == tree.AlterTableDropColumn
		}
	}
	if !needsBindings {
		return alterColumnBindings{}
	}
	type column struct {
		origin int
		fresh  bool
	}
	names := make(map[string]column, len(source.Cols))
	used := make(map[string]bool, len(source.Cols))
	origins := make([]string, 0, len(source.Cols)+len(options))
	parents := make([]int, 0, len(source.Cols)+len(options))
	allocate := func(name string) int {
		id := len(origins)
		origins = append(origins, name)
		parents = append(parents, id)
		used[name] = true
		return id
	}
	for _, col := range source.Cols {
		names[col.Name] = column{origin: allocate(col.Name)}
	}
	root := func(id int) int {
		for parents[id] != id {
			parents[id] = parents[parents[id]]
			id = parents[id]
		}
		return id
	}
	result := make([]alterColumnBinding, len(options))
	ids := make([]int, len(options))
	for i := range ids {
		ids[i] = -1
	}
	// Each vacant spelling remembers only its latest DROP. A later fresh
	// arrival consumes it; a surviving original arriving there discards it.
	pending := make(map[string]int)
	arrive := func(name string, col column) {
		if drop, ok := pending[name]; ok {
			if col.fresh && ids[drop] >= 0 {
				result[drop].preserveIndexes = true
				parents[root(col.origin)] = root(ids[drop])
			}
			delete(pending, name)
		}
		names[name] = col
	}
	nextOrigin := 0
	for i, option := range options {
		switch opt := option.(type) {
		case *tree.AlterAddCol:
			var origin string
			for {
				origin = fmt.Sprintf("__mo_alter_col_%d", nextOrigin)
				nextOrigin++
				if !used[origin] {
					break
				}
			}
			col := column{origin: allocate(origin), fresh: true}
			ids[i] = col.origin
			arrive(opt.Column.Name.ColName(), col)
		case *tree.AlterTableModifyColumnClause:
			if col, ok := names[opt.NewColumn.Name.ColName()]; ok {
				ids[i] = col.origin
			}
		case *tree.AlterTableRenameColumnClause:
			oldName, newName := opt.OldColumnName.ColName(), opt.NewColumnName.ColName()
			if col, ok := names[oldName]; ok {
				ids[i] = col.origin
				if oldName != newName {
					delete(names, oldName)
					arrive(newName, col)
				}
			}
		case *tree.AlterTableChangeColumnClause:
			oldName, newName := opt.OldColumnName.ColName(), opt.NewColumn.Name.ColName()
			if col, ok := names[oldName]; ok {
				ids[i] = col.origin
				if oldName != newName {
					delete(names, oldName)
					arrive(newName, col)
				}
			}
		case *tree.AlterOptionDrop:
			if opt.Typ == tree.AlterTableDropColumn {
				name := string(opt.Name)
				if col, ok := names[name]; ok {
					ids[i] = col.origin
					delete(names, name)
					pending[name] = i
				}
			}
		}
	}
	for i, id := range ids {
		if id >= 0 {
			result[i].origin = origins[root(id)]
		}
	}
	// A fresh column can successively replace more than one source binding.
	// Canonicalize the resolver's initial index parts too, not only later
	// clause origins; otherwise a final DROP could leave one inherited name.
	var sourceOrigins map[string]string
	for i, col := range source.Cols {
		if origin := origins[root(i)]; origin != col.Name {
			if sourceOrigins == nil {
				sourceOrigins = make(map[string]string)
			}
			sourceOrigins[col.Name] = origin
		}
	}
	return alterColumnBindings{clauses: result, sourceOrigins: sourceOrigins}
}
