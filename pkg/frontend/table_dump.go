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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/sync/errgroup"
)

const (
	tableDumpFormatVersion            = 1
	tableDumpLegacyBoundFormatVersion = 2
	tableDumpBoundFormatVersion       = 3
	tableDumpManifestName             = "manifest.json"
	tableDumpReadyName                = "READY"
	tableDumpMaxManifest              = 64 << 20
	tableDumpMaxRelations             = 4_096
	tableDumpMaxObjects               = 250_000
	tableDumpMaxBlocks                = 1_000_000
	tableDumpMaxAutoIncr              = tableDumpMaxRelations * 16

	// Keep LOAD object installation separate from real catalog table locks.
	// The high synthetic table ID follows the existing user-level-lock
	// namespace and serializes installs until the owning transaction ends.
	tableDumpObjectInstallLockTableID uint64 = (1 << 62) + 1
)

type tableDumpManifest struct {
	Version          int                 `json:"version"`
	SourceDatabase   string              `json:"source_database"`
	SourceTable      string              `json:"source_table"`
	CreateSQL        string              `json:"create_sql,omitempty"`
	SchemaHash       string              `json:"schema_hash"`
	BoundExpressions []byte              `json:"bound_expressions,omitempty"`
	ExpressionHash   string              `json:"expression_hash,omitempty"`
	MetadataOnly     bool                `json:"metadata_only"`
	Relations        []tableDumpRelation `json:"relations"`
}

type tableDumpRelation struct {
	Role               string              `json:"role"`
	IndexName          string              `json:"index_name,omitempty"`
	IndexAlgoTableType string              `json:"index_algo_table_type,omitempty"`
	SourceTable        string              `json:"source_table"`
	SchemaHash         string              `json:"schema_hash"`
	LogicalSchemaHash  string              `json:"logical_schema_hash,omitempty"`
	AutoIncrement      []tableDumpAutoIncr `json:"auto_increment,omitempty"`
	Objects            []tableDumpObject   `json:"objects"`
}

// HighWatermark is the last AUTO_INCREMENT value already used or reserved by
// the source relation. The next generated value starts after it.
type tableDumpAutoIncr struct {
	Column        string `json:"column"`
	HighWatermark uint64 `json:"high_watermark"`
}

type tableDumpObject struct {
	Name        string `json:"name"`
	Stats       []byte `json:"stats"`
	Tombstone   bool   `json:"tombstone"`
	Size        int64  `json:"size,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	FixturePath string `json:"fixture_path,omitempty"`
}

type tableDumpRelationRef struct {
	tableDumpRelation
	relation engine.Relation
}

var newImmutableTableMetaReader = disttae.NewImmutableTableMetaReader

func openLocalTableDump(path string) (fileservice.FileService, error) {
	path = strings.TrimPrefix(path, "file://")
	if strings.Contains(path, "://") || !filepath.IsAbs(path) {
		return nil, moerr.NewInvalidInputNoCtx("table dump path must be an absolute local path or file:// URL")
	}
	return fileservice.NewLocalETLFS("table-dump", filepath.Clean(path))
}

func openTableDumpFS(
	ctx context.Context,
	ses *Session,
	dumpPath string,
) (fileservice.FileService, func(), error) {
	if decoded, ok, err := tryDecodeStagePath(ses, dumpPath); err != nil {
		return nil, nil, err
	} else if ok {
		if err := process.AuthorizeSQLFileAccess(ses.proc, decoded); err != nil {
			return nil, nil, err
		}
		etlFS, root, err := fileservice.GetForETL(process.SQLFileAccessContext(ctx, ses.proc), nil, decoded)
		if err != nil {
			return nil, nil, err
		}
		return fileservice.SubPath(etlFS, root), func() { etlFS.Close(ctx) }, nil
	}
	// This branch always opens a local directory. Filename spelling must not
	// reclassify its provider through the generic ETL service-path parser.
	if err := process.AuthorizeSQLFileAccessPath(ses.proc, fileservice.Path{File: dumpPath}); err != nil {
		return nil, nil, err
	}
	fs, err := openLocalTableDump(dumpPath)
	if err != nil {
		return nil, nil, err
	}
	return fs, func() { fs.Close(ctx) }, nil
}

func resolveTableName(ses *Session, table *tree.TableName) (string, string, error) {
	if table == nil || table.Name() == "" {
		return "", "", moerr.NewInvalidInputNoCtx("table name is required")
	}
	dbName := string(table.Schema())
	if dbName == "" {
		dbName = ses.GetDatabaseName()
	}
	if dbName == "" {
		return "", "", moerr.NewNoDBNoCtx()
	}
	return dbName, string(table.Name()), nil
}

func getTableForDump(ctx context.Context, ses *Session, table *tree.TableName) (string, string, engine.Relation, error) {
	dbName, tableName, err := resolveTableName(ses, table)
	if err != nil {
		return "", "", nil, err
	}
	eng := ses.GetTxnHandler().GetStorage()
	txn := ses.GetTxnHandler().GetTxn()
	db, err := eng.Database(ctx, dbName, txn)
	if err != nil {
		return "", "", nil, err
	}
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return "", "", nil, err
	}
	def := rel.GetTableDef(ctx)
	if def == nil {
		return "", "", nil, moerr.NewInternalError(ctx, "table definition is unavailable for index relation admission")
	}
	if err := checkResolvedIndexRelation(&ExecCtx{reqCtx: ctx, ses: ses}, tableName, def.Name, def.TableType, def.FeatureFlag); err != nil {
		return "", "", nil, err
	}
	return dbName, tableName, rel, nil
}

func tableDumpRelationKey(role, indexName, indexAlgoTableType string) string {
	return role + "\x00" + indexName + "\x00" + indexAlgoTableType
}

func tableDumpRelationSchemaMatches(target, dump tableDumpRelation) bool {
	if dump.LogicalSchemaHash != "" {
		return target.LogicalSchemaHash == dump.LogicalSchemaHash
	}
	return target.SchemaHash == dump.SchemaHash
}

func tableDumpLegacyTinyTextResolver(
	ses *Session,
	defaultDB string,
	defaultDatabase engine.Database,
) sqlplan.LegacyTinyTextTableResolver {
	return func(
		ctx context.Context,
		sourceDB string,
		sourceTable string,
	) (*plan.TableDef, error) {
		if sourceDB == "" {
			sourceDB = defaultDB
		}
		database := defaultDatabase
		if database == nil || !strings.EqualFold(sourceDB, defaultDB) {
			var err error
			database, err = ses.GetTxnHandler().GetStorage().Database(
				ctx, sourceDB, ses.GetTxnHandler().GetTxn(),
			)
			if err != nil {
				return nil, err
			}
		}
		relation, err := database.Relation(ctx, sourceTable, nil)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
				return nil, nil
			}
			return nil, err
		}
		sourceDef := sqlplan.CloneTableDefForPlan(relation.GetTableDef(ctx), true)
		if sourceDef.DbName == "" {
			sourceDef.DbName = sourceDB
		}
		return sourceDef, nil
	}
}

func getTableDumpRelations(
	ctx context.Context,
	ses *Session,
	dbName string,
	master engine.Relation,
) ([]tableDumpRelationRef, error) {
	def := master.GetTableDef(ctx)
	if def == nil {
		return nil, moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	db, err := ses.GetTxnHandler().GetStorage().Database(ctx, dbName, ses.GetTxnHandler().GetTxn())
	if err != nil {
		return nil, err
	}
	resolve := tableDumpLegacyTinyTextResolver(ses, dbName, db)
	masterHash, err := tableSchemaHashWithResolver(ctx, def, resolve)
	if err != nil {
		return nil, err
	}
	refs := []tableDumpRelationRef{{
		tableDumpRelation: tableDumpRelation{
			Role: "main", SourceTable: master.GetTableName(), SchemaHash: masterHash,
			LogicalSchemaHash: masterHash,
		},
		relation: master,
	}}
	seen := map[string]struct{}{tableDumpRelationKey("main", "", ""): {}}
	for _, index := range def.Indexes {
		key := tableDumpRelationKey("index", index.IndexName, index.IndexAlgoTableType)
		if _, ok := seen[key]; ok {
			return nil, moerr.NewInternalErrorNoCtxf(
				"duplicate index relation mapping for %s/%s", index.IndexName, index.IndexAlgoTableType,
			)
		}
		indexRel, err := db.Relation(ctx, index.IndexTableName, nil)
		if err != nil {
			return nil, err
		}
		indexDef := indexRel.GetTableDef(ctx)
		indexHash, err := tableSchemaHashWithResolver(ctx, indexDef, resolve)
		if err != nil {
			return nil, err
		}
		indexLogicalHash, err := tableLogicalSchemaHashWithResolver(ctx, indexDef, resolve)
		if err != nil {
			return nil, err
		}
		seen[key] = struct{}{}
		refs = append(refs, tableDumpRelationRef{
			tableDumpRelation: tableDumpRelation{
				Role:               "index",
				IndexName:          index.IndexName,
				IndexAlgoTableType: index.IndexAlgoTableType,
				SourceTable:        index.IndexTableName,
				SchemaHash:         indexHash,
				LogicalSchemaHash:  indexLogicalHash,
			},
			relation: indexRel,
		})
	}
	return refs, nil
}

func tableSchemaHash(def *plan.TableDef) (string, error) {
	return tableSchemaHashWithResolver(context.Background(), def, nil)
}

func tableSchemaHashWithResolver(
	ctx context.Context,
	def *plan.TableDef,
	resolve sqlplan.LegacyTinyTextTableResolver,
) (string, error) {
	return tableSchemaHashWithOptions(ctx, def, resolve, false)
}

func tableLogicalSchemaHashWithResolver(
	ctx context.Context,
	def *plan.TableDef,
	resolve sqlplan.LegacyTinyTextTableResolver,
) (string, error) {
	return tableSchemaHashWithOptions(ctx, def, resolve, true)
}

func tableSchemaHashWithOptions(
	ctx context.Context,
	def *plan.TableDef,
	resolve sqlplan.LegacyTinyTextTableResolver,
	logicalIndexSchema bool,
) (string, error) {
	if def == nil {
		return "", moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	// Catalogs written before TINYTEXT had a durable width marker retain the
	// subtype only in Createsql. Normalize a planner-owned clone before hashing
	// so DUMP and LOAD compare the same logical schema without mutating the
	// catalog definition shared by the engine cache. Keeping this at the common
	// hash boundary covers both main and index relations.
	def = sqlplan.CloneTableDefForPlan(def, true)
	if err := sqlplan.RecoverLegacyTinyText(ctx, def, resolve); err != nil {
		return "", moerr.NewInternalErrorNoCtxf("cannot normalize table schema: %v", err)
	}
	// LOAD can recreate internal index relations with equivalent columns but
	// different catalog-only defaults and CREATE SQL provenance. Normalize those
	// identities for the new index hash without changing the engine-owned catalog
	// definition. Keep the legacy hash above for old manifest compatibility.
	if logicalIndexSchema {
		normalizeTableDumpIndexSchema(def)
		// Programmatically created index relations can have no Createsql, while
		// the same relations recreated from SHOW CREATE retain one. Use the
		// structural fallback for both forms so that the presence of catalog DDL
		// does not select two different hashing representations.
		def.Createsql = ""
	} else if tableDumpHasLegacyTextMetadata(def) {
		normalizeTableDumpLegacyTextMetadata(def)
	}
	return tableSchemaFingerprint(def)
}

func tableSchemaFingerprint(def *plan.TableDef) (string, error) {
	// For ordinary tables, reconstruct the DDL from the expanded TableDef. This
	// makes CREATE TABLE ... LIKE ... compare equal to an equivalent explicit
	// CREATE TABLE statement stored by another cluster.
	if canReconstructTableSchema(def) {
		canonical, err := tableDumpCanonicalCreate(def)
		if err != nil {
			return "", moerr.NewInternalErrorNoCtxf("cannot reconstruct table schema: %v", err)
		}
		sum := sha256.Sum256([]byte(canonical))
		return hex.EncodeToString(sum[:]), nil
	}
	// CREATE SQL is catalog-normalized and deliberately excludes physical table,
	// database, column and constraint IDs, which differ between clusters.
	if createSQL := strings.TrimSpace(def.Createsql); createSQL != "" {
		stmt, err := mysql.ParseOne(context.Background(), createSQL, 1)
		if err != nil {
			return "", moerr.NewInternalErrorNoCtxf("cannot normalize table schema: %v", err)
		}
		defer stmt.Free()
		create, ok := stmt.(*tree.CreateTable)
		if !ok {
			return "", moerr.NewInternalErrorNoCtx("catalog CREATE SQL is not a CREATE TABLE statement")
		}
		create.Table.ExplicitSchema = false
		create.Table.SchemaName = ""
		create.Table.ObjectName = "__table_dump_target__"
		normalized := tree.String(create, dialect.MYSQL)
		sum := sha256.Sum256([]byte(normalized))
		return hex.EncodeToString(sum[:]), nil
	}
	// Some old catalog entries do not retain CREATE SQL. Keep a fallback for
	// those entries while removing the top-level physical identity fields.
	clone := *def
	clone.TblId = 0
	clone.Name = ""
	clone.Createsql = ""
	clone.DbName = ""
	clone.DbId = 0
	clone.Name2ColIndex = nil
	clone.IsLocked = false
	clone.AutoIncrOffset = 0
	clone.AutoIncrEpoch = 0
	clone.LogicalId = 0
	clone.OriginalName = ""
	data, err := clone.Marshal()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func tableDumpCanonicalCreate(def *plan.TableDef) (string, error) {
	clone := *def
	clone.Name = "__table_dump_target__"
	clone.DbName = ""
	clone.Createsql = ""
	// Keep the historical comment and default bytes in persisted fingerprints.
	return sqlplan.ConstructCreateTableSQLForSchemaHash(&clone)
}

// Internal index relations cannot be altered directly, so their table-level
// default charset does not affect stored objects. It is also not stable: an
// index created with an implicit legacy default and the same index recreated
// from SHOW CREATE can have identical column charsets but a different default.
// Normalize that non-storage metadata while retaining each column's effective
// charset, including binary containers used for packed index keys.
func normalizeTableDumpIndexSchema(def *plan.TableDef) {
	def.DefaultCharset = uint32(types.CharsetLegacy)
	for i, col := range def.Cols {
		if col == nil {
			continue
		}
		switch types.T(col.Typ.Id) {
		case types.T_char, types.T_varchar, types.T_text:
			if col.Typ.Charset == uint32(types.CharsetLegacy) {
				cloned := *col
				cloned.Typ.Charset = uint32(types.CharsetUTF8MB4Bin)
				def.Cols[i] = &cloned
			}
		}
	}
}

func canReconstructTableSchema(def *plan.TableDef) bool {
	if def == nil || def.TableType == catalog.SystemClusterRel ||
		def.TableType == catalog.SystemExternalRel || def.Partition != nil ||
		len(def.Fkeys) != 0 || def.ViewSql != nil || def.TblFunc != nil {
		return false
	}
	for _, col := range def.Cols {
		if col == nil || col.Default == nil {
			return false
		}
	}
	return true
}

func tableDumpManifestCreateSQL(
	ctx context.Context,
	def *plan.TableDef,
	resolve sqlplan.LegacyTinyTextTableResolver,
) (string, error) {
	if def == nil {
		return "", moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	needsLegacyCollationRebuild := tableDumpHasLegacyTextMetadata(def)
	needsLegacyBlobRebuild := tableDumpHasLegacyUnboundedBlob(def)
	if (!sqlplan.LegacyTinyTextCreateSQLNeedsRebuild(def) && !needsLegacyCollationRebuild && !needsLegacyBlobRebuild) ||
		!canReconstructTableSchema(def) {
		return def.Createsql, nil
	}
	clone := sqlplan.CloneTableDefForPlan(def, true)
	if err := sqlplan.RecoverLegacyTinyText(ctx, clone, resolve); err != nil {
		return "", moerr.NewInternalErrorNoCtxf("cannot normalize table schema: %v", err)
	}
	if needsLegacyCollationRebuild {
		// CharsetLegacy was persisted before the charset field had semantics, but
		// its text ordering was bytewise. A bare historical CREATE statement is
		// no longer replay-safe now that newly authored text defaults to
		// general_ci. Use the same public compatibility identity as SHOW CREATE
		// and CREATE LIKE so dump/load preserves both the table default and any
		// partially migrated column overrides.
		normalizeTableDumpLegacyTextMetadata(clone)
	}
	canonical, _, err := sqlplan.ConstructCreateTableSQL(nil, clone, nil, false, nil)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("cannot reconstruct table schema: %v", err)
	}
	return canonical, nil
}

func tableDumpHasLegacyUnboundedBlob(def *plan.TableDef) bool {
	for _, col := range def.Cols {
		if col != nil && types.T(col.Typ.Id) == types.T_blob && col.Typ.Width == 0 {
			return true
		}
	}
	return false
}

func tableDumpHasLegacyTextMetadata(def *plan.TableDef) bool {
	hasText := false
	for _, col := range def.Cols {
		if col == nil {
			continue
		}
		switch types.T(col.Typ.Id) {
		case types.T_char, types.T_varchar, types.T_text:
			hasText = true
			if col.Typ.Charset == uint32(types.CharsetLegacy) {
				return true
			}
		}
	}
	return hasText && def.DefaultCharset == uint32(types.CharsetLegacy)
}

func normalizeTableDumpLegacyTextMetadata(def *plan.TableDef) {
	if def.DefaultCharset == uint32(types.CharsetLegacy) {
		def.DefaultCharset = uint32(types.CharsetUTF8MB4Bin)
	}
	for i, col := range def.Cols {
		if col == nil {
			continue
		}
		switch types.T(col.Typ.Id) {
		case types.T_char, types.T_varchar, types.T_text:
			if col.Typ.Charset == uint32(types.CharsetLegacy) {
				cloned := *col
				cloned.Typ.Charset = uint32(types.CharsetUTF8MB4Bin)
				def.Cols[i] = &cloned
			}
		}
	}
}

func validateTableDumpSchema(def *plan.TableDef) error {
	if def == nil {
		return moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	if def.IsTemporary {
		return moerr.NewNotSupportedNoCtx("DUMP TABLE and LOAD TABLE with temporary tables")
	}
	switch def.TableType {
	case catalog.SystemExternalRel, catalog.SystemSourceRel:
		return moerr.NewNotSupportedNoCtx("DUMP TABLE and LOAD TABLE with external or source tables")
	case catalog.SystemViewRel:
		return moerr.NewNotSupportedNoCtx("DUMP TABLE and LOAD TABLE with views")
	case catalog.SystemSequenceRel:
		return moerr.NewNotSupportedNoCtx("DUMP TABLE and LOAD TABLE with sequences")
	case catalog.SystemTransientRel:
		return moerr.NewNotSupportedNoCtx("DUMP TABLE and LOAD TABLE with transient relations")
	}
	if def.ViewSql != nil || def.TblFunc != nil {
		return moerr.NewNotSupportedNoCtx("DUMP TABLE and LOAD TABLE with non-object-backed relations")
	}
	if def.Partition != nil {
		return moerr.NewNotSupportedNoCtx("DUMP TABLE and LOAD TABLE with partitions")
	}
	if len(def.Fkeys) != 0 || len(def.RefChildTbls) != 0 {
		return moerr.NewNotSupportedNoCtx("DUMP TABLE and LOAD TABLE with foreign key constraints")
	}
	return nil
}

func validateTableDumpRelations(ctx context.Context, refs []tableDumpRelationRef) error {
	for _, ref := range refs {
		if err := validateTableDumpSchema(ref.relation.GetTableDef(ctx)); err != nil {
			return err
		}
	}
	return nil
}

func collectTableDumpAutoIncrementState(
	ctx context.Context,
	ses *Session,
	dbName string,
	ref tableDumpRelationRef,
) ([]tableDumpAutoIncr, error) {
	def := ref.relation.GetTableDef(ctx)
	cols := incrservice.GetAutoColumnFromDef(def)
	if len(cols) == 0 {
		return nil, nil
	}

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()
	return collectTableDumpAutoIncrementStateWithExec(ctx, bh, dbName, ref, cols)
}

func collectTableDumpAutoIncrementStateWithExec(
	ctx context.Context,
	bh BackgroundExec,
	dbName string,
	ref tableDumpRelationRef,
	cols []incrservice.AutoColumn,
) ([]tableDumpAutoIncr, error) {
	offsets := make(map[string]uint64, len(cols))
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, fmt.Sprintf(
		"select col_name, offset from mo_catalog.mo_increment_columns where table_id = %d",
		ref.relation.GetTableID(ctx),
	)); err != nil {
		return nil, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		for row := uint64(0); row < result.GetRowCount(); row++ {
			name, err := result.GetString(ctx, row, 0)
			if err != nil {
				return nil, err
			}
			offset, err := result.GetUint64(ctx, row, 1)
			if err != nil {
				return nil, err
			}
			offsets[strings.ToLower(name)] = offset
		}
	}

	state := make([]tableDumpAutoIncr, 0, len(cols))
	for _, col := range cols {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		column := sqlquote.Ident(col.ColName)
		table := sqlquote.QualifiedIdent(dbName, ref.SourceTable)
		bh.ClearExecResultSet()
		if err := bh.Exec(ctx, fmt.Sprintf(
			"select cast(coalesce(max(case when %s > 0 then %s else 0 end), 0) as unsigned) from %s",
			column, column, table,
		)); err != nil {
			return nil, err
		}
		results, err = getResultSet(ctx, bh)
		if err != nil {
			return nil, err
		}
		var maxValue uint64
		for _, result := range results {
			if result.GetRowCount() == 0 {
				continue
			}
			maxValue, err = result.GetUint64(ctx, 0, 0)
			if err != nil {
				return nil, err
			}
			break
		}
		state = append(state, tableDumpAutoIncr{
			Column:        col.ColName,
			HighWatermark: max(offsets[strings.ToLower(col.ColName)], maxValue),
		})
	}
	return state, nil
}

func hashTableDumpFile(ctx context.Context, fs fileservice.FileService, name string) (int64, string, error) {
	var reader io.ReadCloser
	if err := fs.Read(ctx, &fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1, ReadCloserForRead: &reader}},
		Policy:   fileservice.SkipAllCache,
	}); err != nil {
		return 0, "", err
	}
	defer reader.Close()

	hasher := sha256.New()
	n, err := io.Copy(hasher, reader)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(hasher.Sum(nil)), nil
}

func copyTableDumpFile(
	ctx context.Context,
	srcFS, dstFS fileservice.FileService,
	src, dst string,
) (size int64, hash string, providerCopied bool, err error) {
	srcEntry, err := srcFS.StatFile(ctx, src)
	if err != nil {
		return 0, "", false, err
	}
	if copier, ok := dstFS.(fileservice.ObjectCopier); ok {
		copied, err := copier.CopyObject(ctx, srcFS, src, dst)
		if err != nil {
			return 0, "", false, err
		}
		if copied {
			dstEntry, err := dstFS.StatFile(ctx, dst)
			if err != nil {
				return 0, "", false, err
			}
			if dstEntry.Size != srcEntry.Size {
				return 0, "", false, moerr.NewInternalErrorNoCtxf(
					"server-side object copy size mismatch: source %d, destination %d",
					srcEntry.Size, dstEntry.Size,
				)
			}
			// LOAD TABLE is an administrator-only operation, and CopyObject is a
			// trusted provider-side primitive. Reading the entire destination back
			// through CN solely to recompute SHA-256 defeats server-side copy and
			// makes load time proportional to the fixture size. The provider result
			// plus the destination size is sufficient here.
			return srcEntry.Size, "", true, nil
		}
	}
	size, hash, err = streamTableDumpFile(ctx, srcFS, dstFS, src, dst)
	return size, hash, false, err
}

func streamTableDumpFile(ctx context.Context, srcFS, dstFS fileservice.FileService, src, dst string) (int64, string, error) {
	var reader io.ReadCloser
	if err := srcFS.Read(ctx, &fileservice.IOVector{
		FilePath: src,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1, ReadCloserForRead: &reader}},
		Policy:   fileservice.SkipAllCache,
	}); err != nil {
		return 0, "", err
	}
	defer reader.Close()

	hasher := sha256.New()
	counter := &countingWriter{}
	stream := io.TeeReader(reader, io.MultiWriter(hasher, counter))
	if err := dstFS.Write(ctx, fileservice.IOVector{
		FilePath: dst,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1, ReaderForWrite: stream}},
		Policy:   fileservice.SkipAllCache,
	}); err != nil {
		return 0, "", err
	}
	return counter.n, hex.EncodeToString(hasher.Sum(nil)), nil
}

func copyTableDumpObjects(
	ctx context.Context,
	sourceFS, dumpFS fileservice.FileService,
	objects []tableDumpObject,
) ([]string, error) {
	if len(objects) == 0 {
		return nil, nil
	}

	copies := make([]fileservice.ObjectCopy, len(objects))
	for i := range objects {
		copies[i] = fileservice.ObjectCopy{
			SourcePath:      objects[i].Name,
			DestinationPath: objects[i].FixturePath,
		}
	}
	results, batchErr := fileservice.CopyObjects(
		ctx, sourceFS, dumpFS, copies, fileservice.ObjectCopyOptions{},
	)
	if len(results) != len(objects) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"object batch copier returned %d results for %d objects", len(results), len(objects),
		)
	}
	written := make([]string, 0, len(objects))
	for i := range objects {
		if !results[i].Copied {
			continue
		}
		written = append(written, objects[i].FixturePath)
	}
	var next atomic.Uint64
	group, statCtx := errgroup.WithContext(ctx)
	concurrency := min(fileservice.DefaultObjectCopyConcurrency, len(objects))
	for range concurrency {
		group.Go(func() error {
			for {
				if err := statCtx.Err(); err != nil {
					return err
				}
				i := int(next.Add(1) - 1)
				if i >= len(objects) {
					return nil
				}
				if !results[i].Copied {
					continue
				}
				srcEntry, err := sourceFS.StatFile(statCtx, objects[i].Name)
				if err != nil {
					return err
				}
				dstSize, hash, err := hashTableDumpFile(statCtx, dumpFS, objects[i].FixturePath)
				if err != nil {
					return err
				}
				if dstSize != srcEntry.Size {
					return moerr.NewInternalErrorNoCtxf(
						"server-side object copy size mismatch: source %d, destination %d",
						srcEntry.Size, dstSize,
					)
				}
				objects[i].Size = srcEntry.Size
				objects[i].SHA256 = hash
			}
		})
	}
	if err := group.Wait(); err != nil {
		return written, err
	}
	if batchErr != nil {
		return written, batchErr
	}
	for i := range objects {
		if results[i].Copied {
			continue
		}
		size, hash, err := streamTableDumpFile(
			ctx, sourceFS, dumpFS, objects[i].Name, objects[i].FixturePath,
		)
		if err != nil {
			return written, err
		}
		objects[i].Size = size
		objects[i].SHA256 = hash
		written = append(written, objects[i].FixturePath)
	}
	return written, nil
}

type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

func writeTableDumpJSON(ctx context.Context, fs fileservice.FileService, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > tableDumpMaxManifest {
		return moerr.NewInvalidInputNoCtxf(
			"table dump manifest exceeds %d bytes", tableDumpMaxManifest,
		)
	}
	return fileservice.WriteExclusive(ctx, fs, fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), ReaderForWrite: bytes.NewReader(data)}},
	})
}

func readTableDumpManifest(ctx context.Context, fs fileservice.FileService) (*tableDumpManifest, error) {
	entry, err := fs.StatFile(ctx, tableDumpManifestName)
	if err != nil {
		return nil, err
	}
	if entry.Size < 0 || entry.Size > tableDumpMaxManifest {
		return nil, moerr.NewInvalidInputNoCtxf(
			"table dump manifest size %d exceeds the %d-byte limit", entry.Size, tableDumpMaxManifest,
		)
	}
	vec := &fileservice.IOVector{FilePath: tableDumpManifestName, Entries: []fileservice.IOEntry{{Offset: 0, Size: entry.Size}}}
	if err = fs.Read(ctx, vec); err != nil {
		return nil, err
	}
	manifest, err := decodeTableDumpManifest(vec.Entries[0].Data)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("invalid table dump manifest: %v", err)
	}
	if manifest.Version != tableDumpFormatVersion && manifest.Version != tableDumpLegacyBoundFormatVersion && manifest.Version != tableDumpBoundFormatVersion {
		return nil, moerr.NewInvalidInputNoCtxf("unsupported table dump format version %d", manifest.Version)
	}
	if (manifest.Version == tableDumpLegacyBoundFormatVersion || manifest.Version == tableDumpBoundFormatVersion) &&
		(len(manifest.BoundExpressions) == 0 || manifest.ExpressionHash == "") {
		return nil, moerr.NewInvalidInputNoCtx("table dump is missing bound expression metadata")
	}
	return manifest, nil
}

func decodeTableDumpManifest(data []byte) (*tableDumpManifest, error) {
	if err := rejectDuplicateTableDumpFields(data); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if start != json.Delim('{') {
		return nil, moerr.NewInvalidInputNoCtx("table dump manifest must be a JSON object")
	}

	manifest := new(tableDumpManifest)
	relationCount := 0
	objectCount := 0
	autoIncrCount := 0
	for decoder.More() {
		name, err := decodeTableDumpFieldName(decoder)
		if err != nil {
			return nil, err
		}
		switch name {
		case "version":
			err = decoder.Decode(&manifest.Version)
		case "source_database":
			err = decoder.Decode(&manifest.SourceDatabase)
		case "source_table":
			err = decoder.Decode(&manifest.SourceTable)
		case "create_sql":
			err = decoder.Decode(&manifest.CreateSQL)
		case "schema_hash":
			err = decoder.Decode(&manifest.SchemaHash)
		case "bound_expressions":
			err = decoder.Decode(&manifest.BoundExpressions)
		case "expression_hash":
			err = decoder.Decode(&manifest.ExpressionHash)
		case "metadata_only":
			err = decoder.Decode(&manifest.MetadataOnly)
		case "relations":
			manifest.Relations, err = decodeTableDumpRelations(
				decoder, &relationCount, &objectCount, &autoIncrCount,
			)
		default:
			err = skipTableDumpJSONValue(decoder)
		}
		if err != nil {
			return nil, err
		}
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInvalidInputNoCtx("invalid table dump manifest object")
	}
	if _, err = decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, moerr.NewInvalidInputNoCtx("table dump manifest contains trailing JSON data")
		}
		return nil, err
	}
	return manifest, nil
}

// encoding/json accepts duplicate object keys and silently keeps the last
// value. A dump is an untrusted restore instruction, so reject ambiguous
// metadata at every nesting level before interpreting it.
func rejectDuplicateTableDumpFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	type frame struct {
		object    bool
		fieldName bool
		seen      map[string]struct{}
	}
	var stack []frame
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if delimiter, ok := token.(json.Delim); ok && (delimiter == '}' || delimiter == ']') {
			if len(stack) == 0 {
				return moerr.NewInvalidInputNoCtx("invalid table dump JSON")
			}
			stack = stack[:len(stack)-1]
			continue
		}
		if len(stack) != 0 && stack[len(stack)-1].object && stack[len(stack)-1].fieldName {
			name, ok := token.(string)
			if !ok {
				return moerr.NewInvalidInputNoCtx("invalid table dump field name")
			}
			current := &stack[len(stack)-1]
			if _, exists := current.seen[name]; exists {
				return moerr.NewInvalidInputNoCtxf("duplicate table dump field %s", name)
			}
			current.seen[name] = struct{}{}
			current.fieldName = false
			continue
		}
		if len(stack) != 0 && stack[len(stack)-1].object {
			stack[len(stack)-1].fieldName = true
		}
		if delimiter, ok := token.(json.Delim); ok && (delimiter == '{' || delimiter == '[') {
			if len(stack) >= 64 {
				return moerr.NewInvalidInputNoCtx("table dump JSON nesting exceeds limit")
			}
			stack = append(stack, frame{object: delimiter == '{', fieldName: delimiter == '{', seen: make(map[string]struct{})})
		}
	}
}

func decodeTableDumpRelations(
	decoder *json.Decoder,
	relationCount, objectCount, autoIncrCount *int,
) ([]tableDumpRelation, error) {
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if start == nil {
		return nil, nil
	}
	if start != json.Delim('[') {
		return nil, moerr.NewInvalidInputNoCtx("table dump relations must be a JSON array")
	}
	relations := make([]tableDumpRelation, 0)
	for decoder.More() {
		if *relationCount >= tableDumpMaxRelations {
			return nil, moerr.NewInvalidInputNoCtxf(
				"table dump contains more than %d relations", tableDumpMaxRelations,
			)
		}
		(*relationCount)++
		relation, err := decodeTableDumpRelation(decoder, objectCount, autoIncrCount)
		if err != nil {
			return nil, err
		}
		relations = append(relations, relation)
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInvalidInputNoCtx("invalid table dump relations array")
	}
	return relations, nil
}

func decodeTableDumpRelation(
	decoder *json.Decoder,
	objectCount, autoIncrCount *int,
) (tableDumpRelation, error) {
	var relation tableDumpRelation
	start, err := decoder.Token()
	if err != nil {
		return relation, err
	}
	if start != json.Delim('{') {
		return relation, moerr.NewInvalidInputNoCtx("table dump relation must be a JSON object")
	}
	for decoder.More() {
		name, err := decodeTableDumpFieldName(decoder)
		if err != nil {
			return relation, err
		}
		switch name {
		case "role":
			err = decoder.Decode(&relation.Role)
		case "index_name":
			err = decoder.Decode(&relation.IndexName)
		case "index_algo_table_type":
			err = decoder.Decode(&relation.IndexAlgoTableType)
		case "source_table":
			err = decoder.Decode(&relation.SourceTable)
		case "schema_hash":
			err = decoder.Decode(&relation.SchemaHash)
		case "logical_schema_hash":
			err = decoder.Decode(&relation.LogicalSchemaHash)
		case "auto_increment":
			relation.AutoIncrement, err = decodeTableDumpAutoIncrement(decoder, autoIncrCount)
		case "objects":
			relation.Objects, err = decodeTableDumpObjects(decoder, objectCount)
		default:
			err = skipTableDumpJSONValue(decoder)
		}
		if err != nil {
			return relation, err
		}
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		if err != nil {
			return relation, err
		}
		return relation, moerr.NewInvalidInputNoCtx("invalid table dump relation object")
	}
	return relation, nil
}

func decodeTableDumpAutoIncrement(
	decoder *json.Decoder,
	autoIncrCount *int,
) ([]tableDumpAutoIncr, error) {
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if start == nil {
		return nil, nil
	}
	if start != json.Delim('[') {
		return nil, moerr.NewInvalidInputNoCtx("table dump auto_increment must be a JSON array")
	}
	state := make([]tableDumpAutoIncr, 0)
	for decoder.More() {
		if *autoIncrCount >= tableDumpMaxAutoIncr {
			return nil, moerr.NewInvalidInputNoCtxf(
				"table dump contains more than %d auto-increment columns", tableDumpMaxAutoIncr,
			)
		}
		(*autoIncrCount)++
		var item tableDumpAutoIncr
		if err = decoder.Decode(&item); err != nil {
			return nil, err
		}
		state = append(state, item)
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInvalidInputNoCtx("invalid table dump auto_increment array")
	}
	return state, nil
}

func decodeTableDumpObjects(decoder *json.Decoder, objectCount *int) ([]tableDumpObject, error) {
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if start == nil {
		return nil, nil
	}
	if start != json.Delim('[') {
		return nil, moerr.NewInvalidInputNoCtx("table dump objects must be a JSON array")
	}
	objects := make([]tableDumpObject, 0)
	for decoder.More() {
		if *objectCount >= tableDumpMaxObjects {
			return nil, moerr.NewInvalidInputNoCtxf(
				"table dump contains more than %d objects", tableDumpMaxObjects,
			)
		}
		(*objectCount)++
		var object tableDumpObject
		if err = decoder.Decode(&object); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInvalidInputNoCtx("invalid table dump objects array")
	}
	return objects, nil
}

func decodeTableDumpFieldName(decoder *json.Decoder) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	name, ok := token.(string)
	if !ok {
		return "", moerr.NewInvalidInputNoCtx("table dump object field name must be a string")
	}
	return name, nil
}

func skipTableDumpJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok || (delim != '{' && delim != '[') {
		return nil
	}
	depth := 1
	for depth > 0 {
		token, err = decoder.Token()
		if err != nil {
			return err
		}
		if delim, ok = token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}

func dumpTableRelationObjects(
	ctx context.Context,
	ref tableDumpRelationRef,
	metadataOnly bool,
	sourceFS, dumpFS fileservice.FileService,
	mp *mpool.MPool,
) (tableDumpRelation, []string, error) {
	result := ref.tableDumpRelation
	reader, err := newImmutableTableMetaReader(ctx, ref.relation, tableDumpMaxObjects)
	if err != nil {
		return result, nil, err
	}
	defer reader.Close()
	dataBatch := colexec.AllocCNS3ResultBat(false, mp)
	defer dataBatch.Clean(mp)
	tombstoneBatch := colexec.AllocCNS3ResultBat(true, mp)
	defer tombstoneBatch.Clean(mp)
	if _, err = reader.Read(ctx, nil, nil, mp, dataBatch); err != nil {
		return result, nil, err
	}
	if _, err = reader.Read(ctx, nil, nil, mp, tombstoneBatch); err != nil {
		return result, nil, err
	}

	result.Objects = make([]tableDumpObject, 0, dataBatch.RowCount()+tombstoneBatch.RowCount())
	written := make([]string, 0, cap(result.Objects))
	appendObjects := func(bat *batch.Batch, statsIndex int, tombstone bool) error {
		statsVec := bat.Vecs[statsIndex]
		// A data metadata batch has one BlockInfo row per block, but only one
		// ObjectStats entry per object.  Iterate the stats vector itself rather
		// than the batch row count, which follows the BlockInfo vector.
		for i := 0; i < statsVec.Length(); i++ {
			statsBytes := append([]byte(nil), statsVec.GetBytesAt(i)...)
			stats := objectio.ObjectStats(statsBytes)
			if stats.GetAppendable() {
				return moerr.NewInternalErrorNoCtxf(
					"table metadata reader returned appendable object %s for relation %s",
					stats.ObjectName().String(), ref.SourceTable,
				)
			}
			item := tableDumpObject{Name: stats.ObjectName().String(), Stats: statsBytes, Tombstone: tombstone}
			if !metadataOnly {
				item.FixturePath = path.Join("objects", item.Name)
			}
			result.Objects = append(result.Objects, item)
		}
		return nil
	}
	if err = appendObjects(dataBatch, 1, false); err != nil {
		return result, written, err
	}
	if err = appendObjects(tombstoneBatch, 0, true); err != nil {
		return result, written, err
	}
	if len(result.Objects) > tableDumpMaxObjects {
		return result, written, moerr.NewInvalidInputNoCtxf(
			"table dump relation contains more than %d objects", tableDumpMaxObjects,
		)
	}
	if !metadataOnly {
		written, err = copyTableDumpObjects(ctx, sourceFS, dumpFS, result.Objects)
		if err != nil {
			return result, written, err
		}
	}
	return result, written, nil
}

func handleDumpTable(ctx context.Context, ses *Session, stmt *tree.DumpTable) error {
	dumpFS, closeDumpFS, err := openTableDumpFS(ctx, ses, stmt.Path)
	if err != nil {
		return err
	}
	defer closeDumpFS()
	if err = fileservice.CheckExclusiveWrite(dumpFS); err != nil {
		return err
	}
	for _, name := range []string{tableDumpManifestName, tableDumpReadyName} {
		if _, err = dumpFS.StatFile(ctx, name); err == nil {
			return moerr.NewFileAlreadyExistsNoCtx(name)
		} else if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return err
		}
	}
	dbName, tableName, rel, err := getTableForDump(ctx, ses, stmt.Table)
	if err != nil {
		return err
	}
	def := rel.GetTableDef(ctx)
	if err = validateTableDumpSchema(def); err != nil {
		return err
	}
	refs, err := getTableDumpRelations(ctx, ses, dbName, rel)
	if err != nil {
		return err
	}
	manifestCreateSQL, err := tableDumpManifestCreateSQL(
		ctx, def, tableDumpLegacyTinyTextResolver(ses, dbName, nil),
	)
	if err != nil {
		return err
	}
	if err = validateTableDumpRelations(ctx, refs); err != nil {
		return err
	}

	manifest := &tableDumpManifest{
		Version: tableDumpFormatVersion, SourceDatabase: dbName, SourceTable: tableName,
		CreateSQL: manifestCreateSQL, SchemaHash: refs[0].SchemaHash, MetadataOnly: stmt.MetadataOnly,
		Relations: make([]tableDumpRelation, 0, len(refs)),
	}
	if tableDumpHasBoundExpressions(def) {
		var sensitive bool
		sensitive, err = sqlplan.AnalyzeTableDumpBindings(ses.GetTxnCompileCtx(), def, def)
		if err != nil {
			return err
		}
		if sensitive {
			manifest.Version = tableDumpBoundFormatVersion
			manifest.BoundExpressions, manifest.ExpressionHash, err = tableDumpBoundExpressions(def)
			if err != nil {
				return err
			}
		}
	}
	sourceFS, err := GetObjectFSProvider(ses)
	if err != nil {
		return err
	}

	// Multiple CNs may copy immutable source objects before one exclusively
	// creates the manifest. Object copies have no publication ownership. Do not delete
	// copied fixture objects on failure: that could remove files published by a
	// concurrent successful DUMP. Failed fixtures are reclaimed with the stage
	// fixture lifecycle.
	objectCount := 0
	for _, ref := range refs {
		autoIncrement, err := collectTableDumpAutoIncrementState(ctx, ses, dbName, ref)
		if err != nil {
			return err
		}
		ref.AutoIncrement = autoIncrement
		relationDump, _, err := dumpTableRelationObjects(
			ctx, ref, stmt.MetadataOnly, sourceFS, dumpFS, ses.GetMemPool(),
		)
		if err != nil {
			return err
		}
		objectCount += len(relationDump.Objects)
		if objectCount > tableDumpMaxObjects {
			return moerr.NewInvalidInputNoCtxf(
				"table dump contains more than %d objects", tableDumpMaxObjects,
			)
		}
		manifest.Relations = append(manifest.Relations, relationDump)
	}
	// These context-aware barriers let integration tests hold real SQL requests
	// at the publication boundaries without replacing storage operations.
	fault.TriggerFaultWithContext(ctx, "table_dump_before_manifest")
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = writeTableDumpJSON(ctx, dumpFS, tableDumpManifestName, manifest); err != nil {
		return err
	}
	fault.TriggerFaultWithContext(ctx, "table_dump_after_manifest")
	if err = ctx.Err(); err != nil {
		return err
	}
	if !stmt.MetadataOnly {
		if err = fileservice.WriteExclusive(ctx, dumpFS, fileservice.IOVector{FilePath: tableDumpReadyName, Entries: []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("1")}}}); err != nil {
			return err
		}
	}
	return nil
}

func verifyTableDumpObject(ctx context.Context, fs fileservice.FileService, name string, expectedSize int64, expectedHash string) error {
	if expectedHash == "" {
		entry, err := fs.StatFile(ctx, name)
		if err != nil {
			return err
		}
		if entry.Size != expectedSize {
			return moerr.NewInvalidInputNoCtxf("object %s does not match manifest size", name)
		}
		return nil
	}
	var reader io.ReadCloser
	if err := fs.Read(ctx, &fileservice.IOVector{
		FilePath: name, Entries: []fileservice.IOEntry{{Offset: 0, Size: -1, ReadCloserForRead: &reader}}, Policy: fileservice.SkipAllCache,
	}); err != nil {
		return err
	}
	defer reader.Close()
	hasher := sha256.New()
	n, err := io.Copy(hasher, reader)
	if err != nil {
		return err
	}
	if n != expectedSize || !strings.EqualFold(hex.EncodeToString(hasher.Sum(nil)), expectedHash) {
		return moerr.NewInvalidInputNoCtxf("object %s does not match manifest checksum", name)
	}
	return nil
}

func installTableDumpObject(ctx context.Context, dumpFS, targetFS fileservice.FileService, item tableDumpObject) (bool, error) {
	if _, err := targetFS.StatFile(ctx, item.Name); err == nil {
		return false, verifyTableDumpObject(ctx, targetFS, item.Name, item.Size, item.SHA256)
	} else if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return false, err
	}
	size, hash, providerCopied, err := copyTableDumpFile(ctx, dumpFS, targetFS, item.FixturePath, item.Name)
	if err != nil {
		return true, err
	}
	if providerCopied {
		if size != item.Size {
			return true, moerr.NewInvalidInputNoCtxf("object %s does not match manifest size", item.Name)
		}
		return true, nil
	}
	checksumMismatch := item.SHA256 != "" && hash != "" && !strings.EqualFold(hash, item.SHA256)
	if size != item.Size || checksumMismatch {
		return true, moerr.NewInvalidInputNoCtxf("object %s does not match manifest checksum", item.Name)
	}
	if item.SHA256 != "" && hash == "" {
		if err = verifyTableDumpObject(ctx, targetFS, item.Name, item.Size, item.SHA256); err != nil {
			return true, err
		}
	}
	return true, nil
}

func installTableDumpObjects(
	ctx context.Context,
	dumpFS, targetFS fileservice.FileService,
	items []tableDumpObject,
	onCreated func(string),
) error {
	if len(items) == 0 {
		return nil
	}
	concurrency := min(fileservice.DefaultObjectCopyConcurrency, len(items))
	var next atomic.Uint64
	group, copyCtx := errgroup.WithContext(ctx)
	for range concurrency {
		group.Go(func() error {
			for {
				if err := copyCtx.Err(); err != nil {
					return err
				}
				i := int(next.Add(1) - 1)
				if i >= len(items) {
					return nil
				}
				created, err := installTableDumpObject(copyCtx, dumpFS, targetFS, items[i])
				if created && onCreated != nil {
					onCreated(items[i].Name)
				}
				if err != nil {
					return err
				}
			}
		})
	}
	return group.Wait()
}

func setTableDumpObjectFlags(stats *objectio.ObjectStats, tombstone, hasFakePK bool) {
	level := stats.GetLevel()
	flags := stats.Marshal()
	flags[objectio.ObjectStatsLen-1] &= 0xe0
	if tombstone || !hasFakePK {
		flags[objectio.ObjectStatsLen-1] |= objectio.ObjectFlag_Sorted
	}
	flags[objectio.ObjectStatsLen-1] |= objectio.ObjectFlag_CNCreated
	stats.UnMarshal(flags)
	stats.SetLevel(level)
}

func submitTableDumpObjects(
	ctx context.Context,
	rel engine.Relation,
	objects []tableDumpObject,
	mp *mpool.MPool,
) ([]string, error) {
	def := rel.GetTableDef(ctx)
	hasFakePK := def != nil && def.Pkey != nil && catalog.IsFakePkName(def.Pkey.PkeyColName)
	dataStats := make([]objectio.ObjectStats, 0, len(objects))
	tombstoneStats := make([]objectio.ObjectStats, 0, len(objects))
	dataNames := make([]string, 0, len(objects))
	tombstoneNames := make([]string, 0, len(objects))
	submitted := make([]string, 0, len(objects))
	for _, item := range objects {
		if len(item.Stats) != objectio.ObjectStatsLen {
			return submitted, moerr.NewInvalidInputNoCtxf("invalid object stats for %s", item.Name)
		}
		stats := objectio.ObjectStats(item.Stats)
		if stats.GetAppendable() || stats.ObjectName().String() != item.Name {
			return submitted, moerr.NewInvalidInputNoCtxf("invalid immutable object metadata for %s", item.Name)
		}
		setTableDumpObjectFlags(&stats, item.Tombstone, hasFakePK)
		if item.Tombstone {
			tombstoneStats = append(tombstoneStats, stats)
			tombstoneNames = append(tombstoneNames, item.Name)
		} else {
			dataStats = append(dataStats, stats)
			dataNames = append(dataNames, item.Name)
		}
	}
	if len(dataStats) != 0 {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_text.ToType(), mp)
		bat.Vecs[1] = vector.NewVec(types.T_binary.ToType(), mp)
		if err := colexec.ExpandObjectStatsToBatch(mp, false, bat, true, dataStats...); err != nil {
			bat.Clean(mp)
			return submitted, err
		}
		if err := rel.Write(ctx, bat, []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}); err != nil {
			bat.Clean(mp)
			return submitted, err
		}
		submitted = append(submitted, dataNames...)
		bat.Clean(mp)
	}
	if len(tombstoneStats) != 0 {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_binary.ToType(), mp)
		for i := range tombstoneStats {
			if err := vector.AppendBytes(bat.Vecs[0], tombstoneStats[i].Marshal(), false, mp); err != nil {
				bat.Clean(mp)
				return submitted, err
			}
		}
		bat.SetRowCount(len(tombstoneStats))
		if err := rel.Delete(context.WithValue(ctx, defines.SkipTransferKey{}, true), bat, []string{catalog.ObjectMeta_ObjectStats}, ""); err != nil {
			bat.Clean(mp)
			return submitted, err
		}
		submitted = append(submitted, tombstoneNames...)
		bat.Clean(mp)
	}
	return submitted, nil
}

type tableDumpAutoIncrRestore struct {
	column incrservice.AutoColumn
	offset uint64
}

func validateTableDumpAutoIncrementRestore(
	ctx context.Context,
	rel engine.Relation,
	state []tableDumpAutoIncr,
) ([]tableDumpAutoIncrRestore, uint64, error) {
	def := rel.GetTableDef(ctx)
	if def == nil {
		return nil, 0, moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	columns := incrservice.GetAutoColumnFromDef(def)
	if len(state) == 0 {
		if len(columns) == 0 {
			return nil, 0, nil
		}
		return nil, 0, moerr.NewInvalidInputNoCtxf(
			"table dump auto-increment metadata does not match target relation %s",
			rel.GetTableName(),
		)
	}
	byName := make(map[string]incrservice.AutoColumn, len(columns))
	for _, column := range columns {
		byName[strings.ToLower(column.ColName)] = column
	}

	restores := make([]tableDumpAutoIncrRestore, 0, len(state))
	seen := make(map[string]struct{}, len(state))
	schemaOffset := def.AutoIncrOffset
	for _, item := range state {
		name := strings.ToLower(item.Column)
		column, ok := byName[name]
		if !ok {
			return nil, 0, moerr.NewInvalidInputNoCtxf(
				"auto-increment column %s is not present in target relation %s",
				item.Column, rel.GetTableName(),
			)
		}
		if _, ok = seen[name]; ok {
			return nil, 0, moerr.NewInvalidInputNoCtxf(
				"duplicate auto-increment column %s in table dump", item.Column,
			)
		}
		seen[name] = struct{}{}
		offset := item.HighWatermark
		colDef := def.Cols[column.ColIndex]
		if !colDef.Hidden {
			offset = max(offset, def.AutoIncrOffset)
			schemaOffset = max(schemaOffset, offset)
		}
		if err := incrservice.ValidateAutoColumnOffset(ctx, types.MustTypeFromPlan(colDef.Typ), offset); err != nil {
			return nil, 0, err
		}
		restores = append(restores, tableDumpAutoIncrRestore{column: column, offset: offset})
	}
	if len(seen) != len(columns) {
		return nil, 0, moerr.NewInvalidInputNoCtxf(
			"table dump auto-increment metadata does not match target relation %s",
			rel.GetTableName(),
		)
	}
	return restores, schemaOffset, nil
}

func applyTableDumpAutoIncrementRestore(
	ctx context.Context,
	ses *Session,
	rel engine.Relation,
	restores []tableDumpAutoIncrRestore,
	schemaOffset uint64,
) (resetInstalled bool, err error) {
	if len(restores) == 0 {
		return false, nil
	}
	proc := ses.GetProc()
	if proc == nil || proc.GetTxnOperator() == nil || proc.GetIncrService() == nil {
		return false, moerr.NewInternalErrorNoCtx("LOAD TABLE requires an auto-increment service")
	}
	def := rel.GetTableDef(ctx)
	if def == nil {
		return false, moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	tableID := rel.GetTableID(ctx)
	if err = rel.AlterTable(ctx, nil, []*api.AlterTableReq{
		api.NewUpdateAutoIncrementReq(def.DbId, tableID, schemaOffset, 0),
	}); err != nil {
		return false, err
	}
	resetInstalled = true
	for _, restore := range restores {
		if err = proc.GetIncrService().SetOffset(
			incrservice.WithAutoIDCachePolicy(ctx, def.TblId, def.AutoIdCache),
			tableID,
			restore.column.ColIndex,
			restore.column.ColName,
			restore.offset,
			proc.GetTxnOperator(),
		); err != nil {
			return resetInstalled, err
		}
	}
	return resetInstalled, nil
}

type cloneFileProtector interface {
	ProtectCloneFiles(names ...string)
}

type loadFileTracker interface {
	TrackLoadFiles(names ...string)
}

var lockTableForTableDump = lockop.LockTableWithContext

var lockTableDumpLoadTargets = func(
	ctx context.Context,
	ses *Session,
	refs []tableDumpRelationRef,
	lockObjectInstall bool,
) error {
	proc := ses.GetProc()
	if proc == nil || proc.GetTxnOperator() == nil {
		return moerr.NewInternalErrorNoCtx("LOAD TABLE requires an active transaction process")
	}
	if !proc.GetTxnOperator().Txn().IsPessimistic() {
		return moerr.NewNotSupportedNoCtx("LOAD TABLE in optimistic transactions")
	}
	ordered := append([]tableDumpRelationRef(nil), refs...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].relation.GetTableID(ctx) < ordered[j].relation.GetTableID(ctx)
	})
	eng := ses.GetTxnHandler().GetStorage()
	var previous uint64
	for i, ref := range ordered {
		tableID := ref.relation.GetTableID(ctx)
		if i != 0 && tableID == previous {
			continue
		}
		previous = tableID
		primaryKeys, err := ref.relation.GetPrimaryKeys(ctx)
		if err != nil {
			return err
		}
		if len(primaryKeys) != 1 {
			return moerr.NewInternalErrorNoCtxf("target relation %s has invalid primary key metadata", ref.relation.GetTableName())
		}
		changeDef := len(incrservice.GetAutoColumnFromDef(ref.relation.GetTableDef(ctx))) != 0
		if err = lockTableForTableDump(ctx, eng, proc, tableID, primaryKeys[0].Type, changeDef); err != nil {
			return err
		}
	}
	if lockObjectInstall {
		return lockTableForTableDump(
			ctx, eng, proc, tableDumpObjectInstallLockTableID, types.T_varchar.ToType(), false,
		)
	}
	return nil
}

// tableDumpReleasedPowerSchemaHash reconstructs the released fingerprint
// for v1 LOAD or for a bound re-dump of a released table. The original DDL
// must confirm the implicit table collation or automatic CHECK name.
func tableDumpReleasedPowerSchemaHash(ctx context.Context, target *plan.TableDef, manifest *tableDumpManifest, checkAliases map[string]string) (string, error) {
	if !canReconstructTableSchema(target) {
		return "", nil
	}
	sourceDDL := parseTableDumpCreateDeclaration(ctx, manifest.CreateSQL)
	if sourceDDL == nil {
		return "", nil
	}
	defer sourceDDL.Free()
	targetDDL := parseTableDumpCreateDeclaration(ctx, target.Createsql)
	if targetDDL == nil {
		return "", nil
	}
	defer targetDDL.Free()
	sourceMatches := tableDumpCreateTableMatches(&sourceDDL.Table, manifest.SourceDatabase, manifest.SourceTable)
	targetMatches := tableDumpCreateTableMatches(&targetDDL.Table, target.DbName, target.Name)
	sourceNames, sourceImplicit := collectTableDumpCheckDeclarations(sourceDDL)
	targetNames, targetImplicit := collectTableDumpCheckDeclarations(targetDDL)
	if len(sourceNames) != len(target.Checks) || len(targetNames) != len(target.Checks) {
		return "", nil
	}
	legacy := *target
	legacy.Checks = make([]*plan.CheckDef, len(target.Checks))
	var renderedTargetNames map[int]bool
	var sourceCompositeShow bool
	renamed := false
	for i, check := range target.Checks {
		if check == nil {
			return "", nil
		}
		releasedName := fmt.Sprintf("__mo_chk_%d", i+1)
		currentName := fmt.Sprintf("%s_chk_%d", target.Name, i+1)
		if !strings.EqualFold(sourceNames[i], targetNames[i]) {
			switch {
			case sourceNames[i] == "" && targetNames[i] == releasedName && check.Name == releasedName:
				// The release's SHOW CREATE explicitly spelled the anonymous
				// name. Verify the entire declaration after removing this label.
				if renderedTargetNames == nil {
					renderedTargetNames = make(map[int]bool)
				}
				renderedTargetNames[i] = true
			case manifest.Version == tableDumpBoundFormatVersion && !sourceImplicit &&
				sourceNames[i] == releasedName && targetNames[i] == "" &&
				(check.Name == currentName || check.Name == releasedName):
				// A v3 replayable SHOW CREATE for a packed composite key
				// must be proved below against the complete old declaration.
				sourceCompositeShow = true
			default:
				return "", nil
			}
		}
		copy := *check
		if targetNames[i] == "" && check.Name == currentName && (sourceNames[i] == "" || sourceCompositeShow && sourceNames[i] == releasedName) {
			copy.Name = releasedName
			if checkAliases != nil {
				checkAliases[strings.ToLower(check.Name)] = copy.Name
			}
			renamed = true
		}
		legacy.Checks[i] = &copy
	}
	if !sourceMatches || !targetMatches || len(renderedTargetNames) != 0 {
		// Older RENAME retained the original CREATE name. Compare complete
		// declarations, allowing only separately witnessed identity changes.
		if (!sourceMatches && !tableDumpOrdinaryRenameDeclaration(&sourceDDL.Table, manifest.SourceDatabase)) ||
			(!targetMatches && !tableDumpOrdinaryRenameDeclaration(&targetDDL.Table, target.DbName)) ||
			!sameTableDumpRenameDeclaration(sourceDDL, targetDDL, target, renderedTargetNames) {
			return "", nil
		}
	}
	if sourceCompositeShow && !tableDumpMatchesReleasedCompositeShow(ctx, &legacy, sourceDDL) {
		return "", nil
	}
	if !sourceImplicit && targetImplicit && manifest.Version == tableDumpBoundFormatVersion {
		// Some v3 writers rendered SHOW CREATE for the legacy packed
		// carrier. Accept only that complete, independently reproduced DDL.
		sourceImplicit = sourceCompositeShow || tableDumpMatchesReleasedCompositeShow(ctx, target, sourceDDL)
	}
	if sourceImplicit && targetImplicit && tableDumpV1CollationIndependent(target) {
		legacy.DefaultCharset = uint32(types.CharsetLegacy)
		if manifest.Version == tableDumpFormatVersion && tableDumpNativeCompositePK(target) {
			legacy.Cols = append([]*plan.ColDef(nil), target.Cols...)
			for i, col := range legacy.Cols {
				if col.Name == catalog.CPrimaryKeyColName {
					packed := *col
					packed.Typ.Charset = uint32(types.CharsetLegacy)
					legacy.Cols[i] = &packed
					break
				}
			}
			return tableDumpV1UndecoratedPackedHash(&legacy)
		}
	} else if manifest.Version == tableDumpBoundFormatVersion && !renamed {
		return "", nil
	}
	return tableSchemaHashWithResolver(ctx, &legacy, nil)
}

// A compatibility match requires the same full fingerprint in the header
// and in the main relation. Index relations keep their normal checks.
func tableDumpReleasedPowerMainHash(ctx context.Context, target *plan.TableDef, manifest *tableDumpManifest, checkAliases map[string]string) (string, error) {
	hash, err := tableDumpReleasedPowerSchemaHash(ctx, target, manifest, checkAliases)
	if hash == "" || hash != manifest.SchemaHash || err != nil {
		return "", err
	}
	if !tableDumpMainRelationHasHash(manifest, hash) {
		return "", nil
	}
	return hash, nil
}

func tableDumpMainRelationHasHash(manifest *tableDumpManifest, hash string) bool {
	for _, relation := range manifest.Relations {
		if relation.Role == "main" && strings.EqualFold(relation.SourceTable, manifest.SourceTable) && relation.SchemaHash == hash &&
			(relation.LogicalSchemaHash == hash || relation.LogicalSchemaHash == "" && manifest.Version == tableDumpFormatVersion) {
			return true
		}
	}
	return false
}

// Released regular packed indexes stored the same serialized tuples under
// a legacy VARCHAR collation tag. Prove the ordinary numeric/native key
// layout before reconstructing the old presentation and hashing everything.
func tableDumpReleasedPackedIndex(master, def *plan.TableDef, index tableDumpRelation) *plan.TableDef {
	if def == nil || def.TableType != catalog.SystemIndexRel || def.DefaultCharset != uint32(types.CharsetLegacy) ||
		master.KeyFormat != collation.KeyFormatLegacy || def.KeyFormat != collation.KeyFormatLegacy || len(def.Cols) != 3 ||
		def.GetPkey().GetPkeyColName() != catalog.IndexTableIndexColName {
		return nil
	}
	var metadata *plan.IndexDef
	for _, candidate := range master.Indexes {
		if candidate.GetIndexName() == index.IndexName && candidate.GetIndexAlgoTableType() == index.IndexAlgoTableType {
			metadata = candidate
			break
		}
	}
	if metadata == nil || metadata.Unique || metadata.KeyFormat != collation.KeyFormatLegacy || metadata.IndexTableName != def.Name ||
		index.IndexAlgoTableType != "" || len(metadata.Parts) < 2 ||
		(!catalog.IsNullIndexAlgo(metadata.IndexAlgo) && catalog.ToLower(metadata.IndexAlgo) != catalog.MoIndexBTreeAlgo.ToString()) {
		return nil
	}
	masterPKName := master.GetPkey().GetPkeyColName()
	compositePK := masterPKName == catalog.CPrimaryKeyColName
	for _, part := range metadata.Parts {
		if compositePK && catalog.IsAlias(part) && catalog.ResolveAlias(part) == masterPKName {
			// The hidden VARCHAR is the serialized primary tuple, not a
			// user text key. Check every actual primary key component.
			if !tableDumpNativeCompositePK(master) {
				return nil
			}
			continue
		}
		if !tableDumpNativePackedIndexPart(master, catalog.ResolveAlias(part)) {
			return nil
		}
	}
	key, primary, rowID := def.Cols[0], def.Cols[1], def.Cols[2]
	masterPK := sqlplan.FindColumn(master.Cols, masterPKName)
	if masterPK == nil || key.GetName() != catalog.IndexTableIndexColName ||
		primary.GetName() != catalog.IndexTablePrimaryColName || rowID.GetName() != catalog.Row_ID ||
		!tableDumpPackedVarchar(key.Typ) || !canReconstructTableSchema(def) {
		return nil
	}
	primaryType := masterPK.Typ
	// Both index builders copy the primary value's type without copying
	// its allocator, including when the main relation has a hidden PK.
	primaryType.AutoIncr = false
	if !sameTableDumpValueType(primary.Typ, primaryType) || compositePK && !tableDumpPackedVarchar(primary.Typ) {
		return nil
	}
	legacy := *def
	legacy.Cols = append([]*plan.ColDef(nil), def.Cols...)
	legacyKey := *key
	legacyKey.Typ.Charset = uint32(types.CharsetLegacy)
	legacy.Cols[0] = &legacyKey
	if compositePK {
		legacyPrimary := *primary
		legacyPrimary.Typ.Charset = uint32(types.CharsetLegacy)
		legacy.Cols[1] = &legacyPrimary
	}
	return &legacy
}

func tableDumpMatchesReleasedCompositeShow(ctx context.Context, target *plan.TableDef, source *tree.CreateTable) bool {
	if source == nil || !tableDumpNativeCompositePK(target) || !tableDumpV1CollationIndependent(target) {
		return false
	}
	witness := *target
	witness.DefaultCharset = uint32(types.CharsetUTF8MB4Bin)
	sql, _, err := sqlplan.ConstructCreateTableSQL(nil, &witness, nil, false, nil)
	if err != nil {
		return false
	}
	ddl := parseTableDumpCreateDeclaration(ctx, sql)
	if ddl == nil {
		return false
	}
	defer ddl.Free()
	return sameTableDumpRenameDeclaration(source, ddl, target, nil)
}

func tableDumpNativeCompositePK(def *plan.TableDef) bool {
	pk := def.GetPkey()
	if pk.GetPkeyColName() != catalog.CPrimaryKeyColName || len(pk.Names) < 2 || def.KeyFormat != collation.KeyFormatLegacy {
		return false
	}
	packed := sqlplan.FindColumn(def.Cols, catalog.CPrimaryKeyColName)
	if packed == nil || !packed.Hidden || !tableDumpPackedVarchar(packed.Typ) {
		return false
	}
	for _, name := range pk.Names {
		col := sqlplan.FindColumn(def.Cols, name)
		if col == nil || col.Hidden || !tableDumpV1CollationIndependentType(types.T(col.Typ.Id)) {
			return false
		}
	}
	return true
}

func tableDumpNativePackedIndexPart(master *plan.TableDef, name string) bool {
	col := sqlplan.FindColumn(master.Cols, name)
	return col != nil && tableDumpV1CollationIndependentType(types.T(col.Typ.Id))
}

func tableDumpPackedVarchar(typ plan.Type) bool {
	return typ.Id == int32(types.T_varchar) && typ.Width == types.MaxVarcharLen &&
		typ.CollationVersion == 0 && !typ.PadSpace &&
		(typ.Charset == uint32(types.CharsetBinary) || typ.Charset == uint32(types.CharsetLegacy))
}

func tableDumpReleasedIndexSchemaMatches(
	ctx context.Context, master, indexDef *plan.TableDef, target, dump tableDumpRelation, version int,
) (bool, error) {
	ordinaryMatch := tableDumpRelationSchemaMatches(target, dump)
	if ordinaryMatch && (version != tableDumpBoundFormatVersion || target.SchemaHash == dump.SchemaHash) {
		return true, nil
	}
	released := tableDumpReleasedPackedIndex(master, indexDef, target)
	if released == nil {
		return ordinaryMatch, nil
	}
	suffix, secondary := strings.CutPrefix(dump.SourceTable, catalog.SecondaryIndexTableNamePrefix)
	releasedName := secondary && tableDumpCanonicalUUIDv7(suffix)
	switch version {
	case tableDumpFormatVersion:
		// The released v1 writer used the raw catalog declaration, before
		// modern SHOW CREATE started spelling its legacy collation tags.
		masterPK := sqlplan.FindColumn(master.Cols, master.GetPkey().GetPkeyColName())
		if !releasedName || dump.LogicalSchemaHash != "" || masterPK.Typ.AutoIncr {
			return false, nil
		}
		hash, err := tableDumpV1UndecoratedPackedHash(released)
		return err == nil && hash == dump.SchemaHash, err
	case tableDumpBoundFormatVersion:
		physical, err := tableSchemaHashWithResolver(ctx, released, nil)
		if err != nil {
			return false, err
		}
		logical, err := tableLogicalSchemaHashWithResolver(ctx, released, nil)
		if err != nil {
			return false, err
		}
		if physical == dump.SchemaHash || logical == dump.LogicalSchemaHash {
			// Two halves from different layouts do not identify an index.
			return releasedName && physical == dump.SchemaHash && logical == dump.LogicalSchemaHash, nil
		}
		return ordinaryMatch, nil
	default:
		return false, nil
	}
}

func tableDumpV1UndecoratedPackedHash(def *plan.TableDef) (string, error) {
	canonical, err := tableDumpCanonicalCreate(def)
	if err != nil {
		return "", err
	}
	// The current renderer derives a table default from a legacy VARCHAR.
	// Packed key carriers did not contribute that suffix in v4.2.9.
	raw := strings.TrimSuffix(canonical, " COLLATE=utf8mb4_bin")
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:]), nil
}

func parseTableDumpCreateDeclaration(ctx context.Context, sql string) *tree.CreateTable {
	// Keep the original identifier spelling for the internal UUID check.
	stmt, err := mysql.ParseOne(ctx, sql, 0)
	if err != nil {
		return nil
	}
	create, ok := stmt.(*tree.CreateTable)
	if !ok || create.IsAsLike || create.IsAsSelect {
		stmt.Free()
		return nil
	}
	return create
}

func collectTableDumpCheckDeclarations(create *tree.CreateTable) (names []string, implicitCharset bool) {
	for _, def := range create.Defs {
		switch def := def.(type) {
		case *tree.CheckIndex:
			names = append(names, def.ConstraintSymbol)
		case *tree.ColumnTableDef:
			for _, attr := range def.Attributes {
				if check, ok := attr.(*tree.AttributeCheckConstraint); ok {
					names = append(names, check.Name)
				}
			}
		}
	}
	implicitCharset = true
	for _, option := range create.Options {
		switch option.(type) {
		case *tree.TableOptionCharset, *tree.TableOptionCollate:
			implicitCharset = false
		}
	}
	return names, implicitCharset
}

func tableDumpOrdinaryRenameDeclaration(table *tree.TableName, database string) bool {
	// Keep UUID-shaped suffixes on the stricter COPY path. A user's
	// ordinary historical name may include "_copy_" without a UUID.
	name := strings.ToLower(string(table.ObjectName))
	copyTag := strings.LastIndex(name, "_copy_")
	if copyTag >= 0 {
		suffix := name[copyTag+len("_copy_"):]
		if len(suffix) >= 24 && suffix[8] == '-' && suffix[13] == '-' && suffix[18] == '-' && suffix[23] == '-' {
			return false
		}
	}
	return table.AtTsExpr == nil && table.IcebergRef == nil && !table.ExplicitCatalog &&
		(!table.ExplicitSchema || strings.EqualFold(string(table.SchemaName), database))
}

func sameTableDumpRenameDeclaration(source, target *tree.CreateTable, catalogDef *plan.TableDef, renderedTargetNames map[int]bool) bool {
	left, leftPK, leftOwners := tableDumpCreateWithoutIdentity(source, nil)
	right, rightPK, rightOwners := tableDumpCreateWithoutIdentity(target, renderedTargetNames)
	// Formatting can erase expression syntax, including the distinction
	// between a string operand and an identifier. Compare the parsed trees.
	if leftPK != rightPK || !reflect.DeepEqual(left, right) {
		return false
	}
	for i, leftOwner := range leftOwners {
		rightOwner := rightOwners[i]
		if leftOwner == rightOwner {
			continue
		}
		if leftOwner != "" && rightOwner != "" {
			return false
		}
		owner := leftOwner
		if owner == "" {
			owner = rightOwner
		}
		if !tableDumpCheckBindsColumn(catalogDef, i, owner) {
			return false
		}
	}
	return true
}

func tableDumpCheckBindsColumn(def *plan.TableDef, ordinal int, owner string) bool {
	if ordinal >= len(def.Checks) || def.Checks[ordinal] == nil || def.Checks[ordinal].Check.GetExpr() == nil {
		return false
	}
	position := slices.IndexFunc(def.Cols, func(col *plan.ColDef) bool {
		return col != nil && !col.Hidden && col.Name == owner
	})
	if position < 0 {
		return false
	}
	err := plan.VisitExprTree(def.Checks[ordinal].Check, func(expr *plan.Expr) error {
		ref := expr.GetCol()
		if ref != nil && (ref.RelPos != 0 || int(ref.ColPos) != position) {
			return moerr.NewInvalidInputNoCtx("table dump column CHECK does not match its bound column")
		}
		return nil
	})
	return err == nil
}

func tableDumpCreateWithoutIdentity(create *tree.CreateTable, renderedNames map[int]bool) (*tree.CreateTable, string, []string) {
	clone := *create
	clone.Table = tree.TableName{}
	clone.IfNotExists = false
	pkColumns, tablePK := tableDumpPrimaryKeyColumns(create.Defs)
	var pk string
	if len(pkColumns) == 1 {
		pk = pkColumns[0]
	} else {
		tablePK = nil // Keep a composite PRIMARY KEY's declaration and order.
	}
	clone.Defs = make(tree.TableDefs, 0, len(create.Defs))
	var checks tree.TableDefs
	var owners []string
	checkOrdinal := 0
	for _, def := range create.Defs {
		if check, ok := def.(*tree.CheckIndex); ok {
			if renderedNames[checkOrdinal] {
				copy := *check
				copy.ConstraintSymbol = ""
				check = &copy
			}
			checkOrdinal++
			checks = append(checks, check)
			owners = append(owners, "")
			continue
		}
		if tablePK != nil && def == tablePK {
			continue
		}
		if col, ok := def.(*tree.ColumnTableDef); ok {
			isPK := slices.Contains(pkColumns, col.Name.ColName())
			plainNullable := !isPK && tableDumpNullableNullDeclaration(col)
			column := *col
			column.Attributes = nil
			for _, attr := range col.Attributes {
				switch node := attr.(type) {
				case *tree.AttributeCheckConstraint:
					name := node.Name
					if renderedNames[checkOrdinal] {
						name = ""
					}
					checks = append(checks, &tree.CheckIndex{ConstraintSymbol: name, Expr: node.Expr, Enforced: node.Enforced})
					owners = append(owners, col.Name.ColName())
					checkOrdinal++
					continue
				case *tree.AttributePrimaryKey, *tree.AttributeKey:
					if isPK {
						continue
					}
				case *tree.AttributeNull:
					if (isPK && !node.Is) || (plainNullable && node.Is) {
						continue
					}
				case *tree.AttributeDefault:
					if plainNullable {
						continue
					}
				}
				column.Attributes = append(column.Attributes, attr)
			}
			def = &column
		}
		clone.Defs = append(clone.Defs, def)
	}
	// SHOW CREATE emits both kinds of CHECK after keys, in catalog order.
	clone.Defs = append(clone.Defs, checks...)
	return &clone, pk, owners
}

// SHOW CREATE spells the implicit default of a nullable column as DEFAULT
// NULL. TIMESTAMP has a historical implicit default policy of its own.
func tableDumpNullableNullDeclaration(col *tree.ColumnTableDef) bool {
	typ, ok := col.Type.(*tree.T)
	if !ok || typ.InternalType.Family == tree.TimestampFamily {
		return false
	}
	var defaultNull bool
	for _, attr := range col.Attributes {
		switch attr := attr.(type) {
		case *tree.AttributeNull:
			if !attr.Is {
				return false
			}
		case *tree.AttributeAutoIncrement, *tree.AttributePrimaryKey, *tree.AttributeKey, *tree.AttributeGeneratedAlways:
			return false
		case *tree.AttributeDefault:
			literal, ok := attr.Expr.(*tree.NumVal)
			if defaultNull || !ok || literal.ValType != tree.P_null {
				return false
			}
			defaultNull = true
		}
	}
	return true
}

// Only an unnamed, unadorned PRIMARY KEY accounts for a NOT NULL emitted
// by SHOW. A single member can also move between a column and a table clause.
func tableDumpPrimaryKeyColumns(defs tree.TableDefs) ([]string, *tree.PrimaryKeyIndex) {
	var names []string
	var tablePK *tree.PrimaryKeyIndex
	for _, def := range defs {
		switch def := def.(type) {
		case *tree.PrimaryKeyIndex:
			if len(names) != 0 || def.Name != "" || def.ConstraintSymbol != "" || !def.Empty ||
				def.IndexOption != nil || len(def.KeyParts) == 0 {
				return nil, nil
			}
			for _, part := range def.KeyParts {
				if part.ColName == nil || part.ColName.NumParts != 1 || part.Expr != nil || part.Length != 0 ||
					part.Direction != tree.DefaultDirection {
					return nil, nil
				}
				names = append(names, part.ColName.ColName())
			}
			tablePK = def
		case *tree.ColumnTableDef:
			for _, attr := range def.Attributes {
				switch attr.(type) {
				case *tree.AttributePrimaryKey, *tree.AttributeKey:
					if len(names) != 0 {
						return nil, nil
					}
					names = []string{def.Name.ColName()}
				}
			}
		}
	}
	return names, tablePK
}

// The released COPY implementation left its internal CREATE in the catalog
// after renaming the relation. It used this qualified, canonical UUIDv7 name.
func tableDumpCreateTableMatches(table *tree.TableName, database, name string) bool {
	actual := string(table.ObjectName)
	if strings.EqualFold(actual, name) {
		return true
	}
	if database == "" || !table.ExplicitSchema || table.AtTsExpr != nil || table.IcebergRef != nil ||
		!strings.EqualFold(string(table.SchemaName), database) {
		return false
	}
	prefix := name + "_copy_"
	if len(actual) != len(prefix)+36 || !strings.EqualFold(actual[:len(prefix)], prefix) {
		return false
	}
	return tableDumpCanonicalUUIDv7(actual[len(prefix):])
}

func tableDumpCanonicalUUIDv7(suffix string) bool {
	id, err := uuid.Parse(suffix)
	return err == nil && id.Version() == 7 && id.Variant() == uuid.RFC4122 && id.String() == suffix
}

func tableDumpV1CollationIndependentType(typ types.T) bool {
	// These domains compare native values or bytes independent of the
	// default text collation.
	return typ.ToType().IsNumeric() || typ == types.T_bool || typ.IsDateRelate() ||
		typ == types.T_uuid || typ == types.T_binary || typ == types.T_varbinary
}

func tableDumpV1CollationIndependent(def *plan.TableDef) bool {
	expressions := def
	for i, col := range def.Cols {
		if col.Hidden {
			continue
		}
		typ := types.T(col.Typ.Id)
		// JSON and vectors are stored natively. Only accept them as columns
		// here: the separate expression walk still rejects dependencies on
		// their conversion or comparison contracts.
		if !tableDumpV1CollationIndependentType(typ) && typ != types.T_json && !typ.IsArray() {
			return false
		}
		if (typ == types.T_json || typ.IsArray()) && tableDumpDirectNullDefault(col) {
			// SHOW CREATE spells this implicit NULL explicitly. Exclude just
			// this default owner, leaving every other expression visible.
			if expressions == def {
				copy := *def
				copy.Cols = append([]*plan.ColDef(nil), def.Cols...)
				expressions = &copy
			}
			column, defaultValue := *col, *col.Default
			defaultValue.Expr = nil
			column.Default = &defaultValue
			expressions.Cols[i] = &column
		}
	}
	err := plan.VisitExpressionsInOwner(expressions, func(expr *plan.Expr) error {
		if !tableDumpV1CollationIndependentExpression(expr) {
			return moerr.NewInvalidInputNoCtx("table expression may depend on collation")
		}
		return nil
	})
	return err == nil
}

func tableDumpV1CollationIndependentExpression(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}
	typ := types.T(expr.Typ.Id)
	if !tableDumpV1CollationIndependentType(typ) && typ != types.T_any {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		for i, arg := range fn.Args {
			// A decimal literal is represented by a numeric cast from a text
			// token. The cast parses the token without consulting collation.
			if i == 0 && fn.Func.GetObjName() == "cast" && typ.IsDecimal() && arg.GetLit().GetSval() != "" {
				continue
			}
			if !tableDumpV1CollationIndependentExpression(arg) {
				return false
			}
		}
	}
	for _, arg := range expr.GetList().GetList() {
		if !tableDumpV1CollationIndependentExpression(arg) {
			return false
		}
	}
	return tableDumpV1CollationIndependentExpression(expr.GetLit().GetSrc())
}

func handleLoadTable(ctx context.Context, ses *Session, stmt *tree.LoadTable) (err error) {
	dbName, _, rel, err := getTableForDump(ctx, ses, stmt.Table)
	if err != nil {
		return err
	}
	targetDef := rel.GetTableDef(ctx)
	if err = validateTableDumpSchema(targetDef); err != nil {
		return err
	}
	dumpFS, closeDumpFS, err := openTableDumpFS(ctx, ses, stmt.Path)
	if err != nil {
		return err
	}
	defer closeDumpFS()
	manifest, err := readTableDumpManifest(ctx, dumpFS)
	if err != nil {
		return err
	}
	if !manifest.MetadataOnly {
		if _, err = dumpFS.StatFile(ctx, tableDumpReadyName); err != nil {
			return moerr.NewInvalidInputNoCtx("table dump is incomplete: READY marker is missing")
		}
	}
	targetRefs, err := getTableDumpRelations(ctx, ses, dbName, rel)
	if err != nil {
		return err
	}
	if err = validateTableDumpRelations(ctx, targetRefs); err != nil {
		return err
	}
	var restoredDef *plan.TableDef
	var restoreExpressions bool
	var releasedHash string
	var releasedPower bool
	var checkAliases map[string]string
	if manifest.Version == tableDumpFormatVersion && tableDumpHasBoundExpressions(targetDef) {
		restoredDef, releasedPower, err = sqlplan.RecoverTableDumpV1Bindings(ses.GetTxnCompileCtx(), targetDef)
		if err != nil {
			return err
		}
		restoreExpressions = restoredDef != nil
		if releasedPower && targetRefs[0].SchemaHash != manifest.SchemaHash {
			releasedHash, err = tableDumpReleasedPowerMainHash(ctx, targetDef, manifest, nil)
			if err != nil {
				return err
			}
		}
	}
	if manifest.Version == tableDumpBoundFormatVersion && targetRefs[0].SchemaHash != manifest.SchemaHash {
		checkAliases = make(map[string]string)
		releasedHash, err = tableDumpReleasedPowerMainHash(ctx, targetDef, manifest, checkAliases)
		if err != nil {
			return err
		}
	}
	if targetRefs[0].SchemaHash != manifest.SchemaHash && releasedHash == "" {
		return moerr.NewInvalidInputNoCtx("target table schema does not match table dump")
	}
	if manifest.Version == tableDumpLegacyBoundFormatVersion || manifest.Version == tableDumpBoundFormatVersion {
		releasedBindings := manifest.Version == tableDumpBoundFormatVersion && releasedHash != ""
		restoredDef, restoreExpressions, err = tableDumpRestoredExpressionsWithCheckAliases(
			targetDef, manifest.BoundExpressions, manifest.ExpressionHash, checkAliases, releasedBindings)
		if err != nil && !releasedBindings && manifest.Version == tableDumpBoundFormatVersion &&
			tableDumpNativeCompositePK(targetDef) && tableDumpMainRelationHasHash(manifest, targetRefs[0].SchemaHash) {
			sourceDDL := parseTableDumpCreateDeclaration(ctx, manifest.CreateSQL)
			if sourceDDL != nil {
				releasedBindings = tableDumpCreateTableMatches(&sourceDDL.Table, manifest.SourceDatabase, manifest.SourceTable) &&
					tableDumpMatchesReleasedCompositeShow(ctx, targetDef, sourceDDL)
				sourceDDL.Free()
			}
			if releasedBindings {
				restoredDef, restoreExpressions, err = tableDumpRestoredExpressionsWithCheckAliases(
					targetDef, manifest.BoundExpressions, manifest.ExpressionHash, checkAliases, true)
			}
		}
		if err != nil {
			return err
		}
		if releasedBindings || manifest.Version == tableDumpBoundFormatVersion && len(targetDef.Indexes) != 0 {
			releasedPower, err = sqlplan.ProveTableDumpReleasedPower(ses.GetTxnCompileCtx(), targetDef, restoredDef)
			if err != nil {
				return err
			}
			if releasedBindings && !releasedPower {
				return moerr.NewInvalidInputNoCtx("target table schema does not match table dump")
			}
		} else if _, err = sqlplan.AnalyzeTableDumpBindings(ses.GetTxnCompileCtx(), targetDef, restoredDef); err != nil {
			return err
		}
	}
	if releasedHash != "" {
		targetRefs[0].SchemaHash = releasedHash
		targetRefs[0].LogicalSchemaHash = releasedHash
	}
	if len(targetRefs) != len(manifest.Relations) {
		return moerr.NewInvalidInputNoCtx("target table index topology does not match table dump")
	}
	targetByKey := make(map[string]tableDumpRelationRef, len(targetRefs))
	for _, ref := range targetRefs {
		key := tableDumpRelationKey(ref.Role, ref.IndexName, ref.IndexAlgoTableType)
		targetByKey[key] = ref
	}

	targetFS, err := GetObjectFSProvider(ses)
	if err != nil {
		return err
	}
	txn := ses.GetTxnHandler().GetTxn()
	workspace := txn.GetWorkspace()
	workspace.SetCloneTxn(txn.Txn().SnapshotTS.PhysicalTime)
	if err = lockTableDumpLoadTargets(ctx, ses, targetRefs, !manifest.MetadataOnly); err != nil {
		return err
	}
	autoRestores := make(map[string][]tableDumpAutoIncrRestore, len(manifest.Relations))
	autoSchemaOffsets := make(map[string]uint64, len(manifest.Relations))
	for _, relationDump := range manifest.Relations {
		key := tableDumpRelationKey(relationDump.Role, relationDump.IndexName, relationDump.IndexAlgoTableType)
		targetRef, ok := targetByKey[key]
		if !ok {
			return moerr.NewInvalidInputNoCtx("target table index topology does not match table dump")
		}
		restores, schemaOffset, validateErr := validateTableDumpAutoIncrementRestore(
			ctx, targetRef.relation, relationDump.AutoIncrement,
		)
		if validateErr != nil {
			return validateErr
		}
		if len(restores) != 0 {
			autoRestores[key] = restores
			autoSchemaOffsets[key] = schemaOffset
		}
	}
	protector, ok := workspace.(cloneFileProtector)
	if !ok {
		return moerr.NewNotSupportedNoCtx("LOAD TABLE reusing existing objects with this transaction workspace")
	}
	var tracker loadFileTracker
	if !manifest.MetadataOnly {
		tracker, ok = workspace.(loadFileTracker)
		if !ok {
			return moerr.NewNotSupportedNoCtx("LOAD TABLE installing objects with this transaction workspace")
		}
	}
	seenRelations := make(map[string]struct{}, len(manifest.Relations))
	seenObjects := make(map[string]struct{})
	var totalBlocks uint64
	for _, relationDump := range manifest.Relations {
		key := tableDumpRelationKey(relationDump.Role, relationDump.IndexName, relationDump.IndexAlgoTableType)
		if _, ok := seenRelations[key]; ok {
			return moerr.NewInvalidInputNoCtx("duplicate relation in table dump")
		}
		seenRelations[key] = struct{}{}
		targetRef, ok := targetByKey[key]
		if !ok {
			return moerr.NewInvalidInputNoCtx("target table index topology does not match table dump")
		}
		matches := tableDumpRelationSchemaMatches(targetRef.tableDumpRelation, relationDump)
		if releasedPower && relationDump.Role == "index" {
			matches, err = tableDumpReleasedIndexSchemaMatches(
				ctx, targetDef, targetRef.relation.GetTableDef(ctx), targetRef.tableDumpRelation, relationDump, manifest.Version)
			if err != nil {
				return err
			}
		}
		if !matches {
			return moerr.NewInvalidInputNoCtx("target table index topology does not match table dump")
		}
		existingRows, err := targetRef.relation.Rows(ctx)
		if err != nil {
			return err
		}
		if existingRows != 0 {
			return moerr.NewInvalidInputNoCtxf("target relation %s must be empty", targetRef.relation.GetTableName())
		}
		for _, item := range relationDump.Objects {
			if len(item.Stats) != objectio.ObjectStatsLen {
				return moerr.NewInvalidInputNoCtxf("invalid object stats for %s", item.Name)
			}
			stats := objectio.ObjectStats(item.Stats)
			if stats.GetAppendable() || stats.ObjectName().String() != item.Name {
				return moerr.NewInvalidInputNoCtxf("invalid immutable object metadata for %s", item.Name)
			}
			totalBlocks += uint64(stats.BlkCnt())
			if totalBlocks > tableDumpMaxBlocks {
				return moerr.NewInvalidInputNoCtxf("table dump contains more than %d blocks", tableDumpMaxBlocks)
			}
			if manifest.MetadataOnly {
				if item.FixturePath != "" {
					return moerr.NewInvalidInputNoCtxf("metadata-only object %s contains a fixture path", item.Name)
				}
			} else if item.FixturePath != path.Join("objects", item.Name) || item.Size < 0 || item.SHA256 == "" {
				return moerr.NewInvalidInputNoCtxf("object %s is missing file metadata", item.Name)
			}
			if _, ok := seenObjects[item.Name]; ok {
				return moerr.NewInvalidInputNoCtxf("duplicate object %s in table dump", item.Name)
			}
			seenObjects[item.Name] = struct{}{}
			if manifest.MetadataOnly {
				if _, err = targetFS.StatFile(ctx, item.Name); err != nil {
					return moerr.NewInvalidInputNoCtxf("metadata-only object %s is not present in target storage", item.Name)
				}
			}
		}
	}
	if restoreExpressions {
		if err = sqlplan.RequirePersistedExpressionProtocol(ctx, ses.GetProc(), restoredDef); err != nil {
			return err
		}
		creator, owner, createdTime, ownerErr := tableDumpTargetOwnership(ctx, ses, targetDef.TblId)
		if ownerErr != nil {
			return ownerErr
		}
		if err = rel.AlterTable(ctx, nil, []*api.AlterTableReq{
			api.NewGuardedReplaceDefReq(
				targetDef.DbId, targetDef.TblId, targetDef.Version,
				creator, owner, createdTime, restoredDef),
		}); err != nil {
			return err
		}
	}
	sharedObjects := make([]string, 0)
	for _, relationDump := range manifest.Relations {
		for _, item := range relationDump.Objects {
			// Object creation has no cross-CN ownership token. Protect every
			// installed or reused name from transaction rollback and let the
			// reference-aware object GC reclaim files left by a failed LOAD.
			sharedObjects = append(sharedObjects, item.Name)
		}
		if len(sharedObjects) != 0 {
			protector.ProtectCloneFiles(sharedObjects...)
			sharedObjects = sharedObjects[:0]
		}
		if !manifest.MetadataOnly {
			if err = installTableDumpObjects(ctx, dumpFS, targetFS, relationDump.Objects, func(name string) {
				tracker.TrackLoadFiles(name)
			}); err != nil {
				return err
			}
		}
		// LOAD TABLE is an administrator-only restore operation, and its manifest
		// is produced by DUMP TABLE. Reopening every copied object to duplicate
		// the manifest's physical metadata makes load time proportional to the
		// object count and defeats provider-side CopyObject. Manifest validation,
		// bounded block counts, and copy/size checks are sufficient here.
	}
	for _, relationDump := range manifest.Relations {
		key := tableDumpRelationKey(relationDump.Role, relationDump.IndexName, relationDump.IndexAlgoTableType)
		_, submitErr := submitTableDumpObjects(
			ctx, targetByKey[key].relation, relationDump.Objects, ses.GetMemPool(),
		)
		if submitErr != nil {
			err = submitErr
			return err
		}
	}
	resetTables := make([]uint64, 0, len(autoRestores))
	defer func() {
		if err == nil || len(resetTables) == 0 {
			return
		}
		cleanupCtx := context.WithoutCancel(ctx)
		for _, tableID := range resetTables {
			_ = ses.GetProc().GetIncrService().DiscardOffsetReset(
				cleanupCtx, tableID, ses.GetProc().GetTxnOperator(),
			)
		}
	}()
	for _, relationDump := range manifest.Relations {
		key := tableDumpRelationKey(relationDump.Role, relationDump.IndexName, relationDump.IndexAlgoTableType)
		restores := autoRestores[key]
		installed, restoreErr := applyTableDumpAutoIncrementRestore(
			ctx, ses, targetByKey[key].relation, restores, autoSchemaOffsets[key],
		)
		if installed {
			resetTables = append(resetTables, targetByKey[key].relation.GetTableID(ctx))
		}
		if restoreErr != nil {
			err = restoreErr
			return err
		}
	}
	return nil
}
