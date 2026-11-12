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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// IsFileExternalTableDef reports whether tableDef is a plain file-backed
// external mapping (CSV/JSONLINE/Parquet): an external relation whose durable
// rel_createsql is the JSON of its ExternParam carrying a file source. The
// envelope-backed mappings (MongoDB, Iceberg, Kafka, ESQL/SQL foreign,
// datastream) store their own comment envelopes instead of file-JSON, and
// other legacy external mappings keep whatever they store; neither classifies
// as a file mapping, so classification is decided by the effective ExternParam
// file source alone (issue #31920).
func IsFileExternalTableDef(tableDef *TableDef) bool {
	if tableDef == nil || tableDef.TableType != catalog.SystemExternalRel {
		return false
	}
	param := getExternParamFromTableDef(tableDef)
	// The durable rel_createsql is the raw JSON of the parser's ExternParam and
	// the create path never normalizes INFILE{'filepath'=...}, stage or S3
	// mappings into a top-level field: the shape reported in issue #31920 keeps
	// its path only in Option. Read the effective file source exactly like the
	// read side does (GetFilePathFromParam) and keep stage mappings (StageName)
	// classified as file mappings too; unknown envelope shapes keep their own
	// contracts.
	return GetFilePathFromParam(param) != "" || param.StageFilepath != "" || param.StageName != ""
}

// IsWritableFileExternalTableDef reports whether a file-backed external
// mapping was created with WRITE_FILE_PATTERN, which is the only marker that
// grants INSERT/LOAD (docs/design/writable_external_table_impl.md). Without
// the pattern the file mapping is read-only and rejects every mutation.
func IsWritableFileExternalTableDef(tableDef *TableDef) bool {
	if !IsFileExternalTableDef(tableDef) {
		return false
	}
	_, ok := GetWriteFilePattern(getExternParamFromTableDef(tableDef))
	return ok
}

// RejectTruncateWritableFileExternal is the long-standing writable external
// TRUNCATE rejection. It lives here so the planner admission and the compiled
// terminal guard share one message source and cannot drift.
func RejectTruncateWritableFileExternal(ctx context.Context, table string) error {
	return moerr.NewNotSupportedf(ctx,
		"truncate writable external table '%v'; its files in the stage are not managed by the table", table)
}

// RejectFileExternalTruncate returns the typed TRUNCATE rejection for a
// file-backed external mapping: an external table never owns the mapping's
// files, so TRUNCATE cannot be acknowledged as OK, and historically it was a
// silent no-op for read-only mappings (issue #31920). Writable mappings keep
// their dedicated rejection; non-file mappings (envelope-backed or legacy)
// keep their existing contracts and return nil.
func RejectFileExternalTruncate(ctx context.Context, tableDef *TableDef, table string) error {
	if !IsFileExternalTableDef(tableDef) {
		return nil
	}
	if IsWritableFileExternalTableDef(tableDef) {
		return RejectTruncateWritableFileExternal(ctx, table)
	}
	return moerr.NewNotSupportedf(ctx,
		"truncate read-only external table '%v'; its files are not managed by the table", table)
}

// AlterAddsIndexOrConstraint reports whether any option of the ALTER
// statement creates an index or a table constraint (ADD UNIQUE / ADD
// [FULLTEXT] INDEX / ADD PRIMARY KEY, including inline column index
// attributes). The read-only file ALTER guard keeps rejecting such
// statements itself but delegates them to the established index type
// rejection so the dedicated cannot-create-index diagnostic is preserved
// (CI compatibility for the pre-existing external-table suites).
func AlterAddsIndexOrConstraint(stmt *tree.AlterTable) bool {
	for _, opt := range stmt.Options {
		switch option := opt.(type) {
		case *tree.AlterOptionAdd:
			switch option.Def.(type) {
			case *tree.PrimaryKeyIndex, *tree.Index, *tree.UniqueIndex, *tree.FullTextIndex:
				return true
			case *tree.ColumnTableDef:
				if columnDefHasIndexAttribute(option.Def.(*tree.ColumnTableDef)) {
					return true
				}
			}
		case *tree.AlterAddCol:
			if columnDefHasIndexAttribute(option.Column) {
				return true
			}
		}
	}
	return false
}
