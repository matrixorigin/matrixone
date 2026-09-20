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

import (
	"context"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func validateAlterIndexRenameOptions(ctx context.Context, options []tree.AlterTableOption) (bool, error) {
	rename, other := false, false
	for _, option := range options {
		switch option.(type) {
		case *tree.AlterTableRenameIndexClause:
			rename = true
		case *tree.AlterOptionAlgorithm, *tree.AlterOptionLock:
		default:
			other = true
		}
	}
	if rename && other {
		return true, moerr.NewNotSupported(ctx, "RENAME INDEX combined with other ALTER operations")
	}
	return rename, nil
}

func renameCopyIndex(ctx context.Context, table *TableDef, option *tree.AlterTableRenameIndexClause) error {
	for _, name := range []string{option.OldName, option.NewName} {
		if err := validateIdentifier(ctx, name); err != nil {
			return err
		}
		if name == "" || strings.ContainsRune(name, 0) || strings.HasSuffix(name, " ") {
			return moerr.NewInvalidInputf(ctx, "invalid index name %q", name)
		}
		if IndexNamesEqual(name, "PRIMARY") {
			return moerr.NewNotSupported(ctx, "renaming PRIMARY index")
		}
	}
	source, found := resolveIndexName(table.Indexes, option.OldName)
	if !found {
		return moerr.NewInvalidInputf(ctx, "not found index: %s", option.OldName)
	}
	if destination, found := resolveIndexName(table.Indexes, option.NewName); found && !IndexNamesEqual(source, destination) {
		return moerr.NewDuplicateKey(ctx, option.NewName)
	}
	for _, idx := range table.Indexes {
		if IndexNamesEqual(idx.IndexName, source) {
			idx.IndexName = option.NewName
		}
	}
	return nil
}

// AlterCopyIndexRenames returns original catalog spelling -> final spelling for
// changed logical index names, including case-only changes. The COPY planner
// preserves source physical descriptors and list order until internal CREATE.
// Call this before replacing those descriptors with newly allocated identities.
// Ordinary COPY operations may change index membership or definitions: absent a
// positional, same-source descriptor with a changed name, they return nil, nil.
// Once rename is detected, the entire list must preserve descriptors and each
// logical group must map consistently and injectively to a final group.
func AlterCopyIndexRenames(original, copied *TableDef) (map[string]string, error) {
	if original == nil || copied == nil {
		return nil, nil
	}
	descriptorsEqual := func(a, b *IndexDef) bool {
		left, right := *a, *b
		left.IndexName, right.IndexName = "", ""
		return proto.Equal(&left, &right)
	}
	rename := false
	for i, a := range original.Indexes {
		if i >= len(copied.Indexes) {
			break
		}
		b := copied.Indexes[i]
		if a == nil || b == nil || a.IndexName == b.IndexName {
			continue
		}
		if a.IdxId == b.IdxId && a.IndexTableName == b.IndexTableName &&
			(a.IdxId != "" || a.IndexTableName != "") {
			rename = true
			break
		}
	}
	if !rename {
		return nil, nil
	}
	invalid := func() (map[string]string, error) {
		return nil, moerr.NewInternalErrorNoCtx("invalid COPY index rename lineage")
	}
	if len(original.Indexes) != len(copied.Indexes) {
		return invalid()
	}
	sources, destinations := make(map[string]string), make(map[string]string)
	originalNames := make(map[string]string)
	var result map[string]string
	for i, a := range original.Indexes {
		b := copied.Indexes[i]
		if a == nil || b == nil || !descriptorsEqual(a, b) || a.IndexName == "" || b.IndexName == "" ||
			IndexNamesEqual(a.IndexName, "PRIMARY") || IndexNamesEqual(b.IndexName, "PRIMARY") {
			return invalid()
		}
		from, to := indexNameKey(a.IndexName), indexNameKey(b.IndexName)
		if prev, ok := sources[from]; ok && prev != b.IndexName {
			return invalid()
		}
		if prev, ok := originalNames[from]; ok && prev != a.IndexName {
			return invalid()
		}
		if prev, ok := destinations[to]; ok && prev != from {
			return invalid()
		}
		sources[from], destinations[to], originalNames[from] = b.IndexName, from, a.IndexName
		if a.IndexName != b.IndexName {
			if result == nil {
				result = make(map[string]string)
			}
			result[a.IndexName] = b.IndexName
		}
	}
	return result, nil
}
