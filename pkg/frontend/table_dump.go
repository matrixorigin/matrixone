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
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"golang.org/x/sync/errgroup"
)

const (
	tableDumpFormatVersion = 1
	tableDumpManifestName  = "manifest.json"
	tableDumpReadyName     = "READY"
	tableDumpMaxManifest   = 64 << 20
	tableDumpMaxObjects    = 250_000
)

type tableDumpManifest struct {
	Version        int                 `json:"version"`
	SourceDatabase string              `json:"source_database"`
	SourceTable    string              `json:"source_table"`
	CreateSQL      string              `json:"create_sql,omitempty"`
	SchemaHash     string              `json:"schema_hash"`
	MetadataOnly   bool                `json:"metadata_only"`
	Relations      []tableDumpRelation `json:"relations"`
}

type tableDumpRelation struct {
	Role               string            `json:"role"`
	IndexName          string            `json:"index_name,omitempty"`
	IndexAlgoTableType string            `json:"index_algo_table_type,omitempty"`
	SourceTable        string            `json:"source_table"`
	SchemaHash         string            `json:"schema_hash"`
	Objects            []tableDumpObject `json:"objects"`
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

func openLocalTableDump(path string) (fileservice.FileService, error) {
	if strings.HasPrefix(path, "file://") {
		path = strings.TrimPrefix(path, "file://")
	}
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
		etlFS, root, err := fileservice.GetForETL(ctx, nil, decoded)
		if err != nil {
			return nil, nil, err
		}
		return fileservice.SubPath(etlFS, root), func() { etlFS.Close(ctx) }, nil
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
	return dbName, tableName, rel, nil
}

func tableDumpRelationKey(role, indexName, indexAlgoTableType string) string {
	return role + "\x00" + indexName + "\x00" + indexAlgoTableType
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
	masterHash, err := tableSchemaHash(def)
	if err != nil {
		return nil, err
	}
	refs := []tableDumpRelationRef{{
		tableDumpRelation: tableDumpRelation{
			Role: "main", SourceTable: master.GetTableName(), SchemaHash: masterHash,
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
		seen[key] = struct{}{}
		refs = append(refs, tableDumpRelationRef{
			tableDumpRelation: tableDumpRelation{
				Role:               "index",
				IndexName:          index.IndexName,
				IndexAlgoTableType: index.IndexAlgoTableType,
				SourceTable:        index.IndexTableName,
			},
			relation: indexRel,
		})
	}
	return refs, nil
}

func tableSchemaHash(def *plan.TableDef) (string, error) {
	if def == nil {
		return "", moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	// For ordinary tables, reconstruct the DDL from the expanded TableDef. This
	// makes CREATE TABLE ... LIKE ... compare equal to an equivalent explicit
	// CREATE TABLE statement stored by another cluster.
	canReconstruct := def.TableType != catalog.SystemClusterRel &&
		def.TableType != catalog.SystemExternalRel &&
		def.Partition == nil && len(def.Fkeys) == 0 && def.ViewSql == nil && def.TblFunc == nil
	for _, col := range def.Cols {
		canReconstruct = canReconstruct && col != nil && col.Default != nil
	}
	if canReconstruct {
		clone := *def
		clone.Name = "__table_dump_target__"
		clone.DbName = ""
		clone.Createsql = ""
		canonical, _, err := sqlplan.ConstructCreateTableSQL(nil, &clone, nil, false, nil)
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
	clone.LogicalId = 0
	clone.OriginalName = ""
	data, err := clone.Marshal()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func copyTableDumpFile(ctx context.Context, srcFS, dstFS fileservice.FileService, src, dst string) (int64, string, error) {
	srcEntry, err := srcFS.StatFile(ctx, src)
	if err != nil {
		return 0, "", err
	}
	if copier, ok := dstFS.(fileservice.ObjectCopier); ok {
		copied, err := copier.CopyObject(ctx, srcFS, src, dst)
		if err != nil {
			return 0, "", err
		}
		if copied {
			dstEntry, err := dstFS.StatFile(ctx, dst)
			if err != nil {
				return 0, "", err
			}
			if dstEntry.Size != srcEntry.Size {
				return 0, "", moerr.NewInternalErrorNoCtxf(
					"server-side object copy size mismatch: source %d, destination %d",
					srcEntry.Size, dstEntry.Size,
				)
			}
			return srcEntry.Size, "", nil
		}
	}
	return streamTableDumpFile(ctx, srcFS, dstFS, src, dst)
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
		srcEntry, err := sourceFS.StatFile(ctx, objects[i].Name)
		if err != nil {
			return written, err
		}
		dstEntry, err := dumpFS.StatFile(ctx, objects[i].FixturePath)
		if err != nil {
			return written, err
		}
		if dstEntry.Size != srcEntry.Size {
			return written, moerr.NewInternalErrorNoCtxf(
				"server-side object copy size mismatch: source %d, destination %d",
				srcEntry.Size, dstEntry.Size,
			)
		}
		objects[i].Size = srcEntry.Size
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
	return fs.Write(ctx, fileservice.IOVector{
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
	var manifest tableDumpManifest
	if err = json.Unmarshal(vec.Entries[0].Data, &manifest); err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("invalid table dump manifest: %v", err)
	}
	if manifest.Version != tableDumpFormatVersion {
		return nil, moerr.NewInvalidInputNoCtxf("unsupported table dump format version %d", manifest.Version)
	}
	objectCount := 0
	for i := range manifest.Relations {
		objectCount += len(manifest.Relations[i].Objects)
		if objectCount > tableDumpMaxObjects {
			return nil, moerr.NewInvalidInputNoCtxf(
				"table dump contains more than %d objects", tableDumpMaxObjects,
			)
		}
	}
	return &manifest, nil
}

func dumpTableRelationObjects(
	ctx context.Context,
	ref tableDumpRelationRef,
	metadataOnly bool,
	sourceFS, dumpFS fileservice.FileService,
	mp *mpool.MPool,
) (tableDumpRelation, []string, error) {
	result := ref.tableDumpRelation
	reader, err := disttae.NewImmutableTableMetaReader(ctx, ref.relation, tableDumpMaxObjects)
	if err != nil {
		return result, nil, err
	}
	defer reader.Close()
	dataBatch := colexec.AllocCNS3ResultBat(false, false)
	defer dataBatch.Clean(mp)
	tombstoneBatch := colexec.AllocCNS3ResultBat(true, false)
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
	dbName, tableName, rel, err := getTableForDump(ctx, ses, stmt.Table)
	if err != nil {
		return err
	}
	def := rel.GetTableDef(ctx)
	if def == nil {
		return moerr.NewInternalErrorNoCtx("table definition is unavailable")
	}
	if def.Partition != nil {
		return moerr.NewNotSupportedNoCtx("DUMP TABLE with partitions")
	}
	for _, col := range def.Cols {
		if col.Typ.AutoIncr {
			return moerr.NewNotSupportedNoCtx("DUMP TABLE with auto-increment columns")
		}
	}
	schemaHash, err := tableSchemaHash(def)
	if err != nil {
		return err
	}
	refs, err := getTableDumpRelations(ctx, ses, dbName, rel)
	if err != nil {
		return err
	}

	manifest := &tableDumpManifest{
		Version: tableDumpFormatVersion, SourceDatabase: dbName, SourceTable: tableName,
		CreateSQL: def.Createsql, SchemaHash: schemaHash, MetadataOnly: stmt.MetadataOnly,
		Relations: make([]tableDumpRelation, 0, len(refs)),
	}
	dumpFS, closeDumpFS, err := openTableDumpFS(ctx, ses, stmt.Path)
	if err != nil {
		return err
	}
	defer closeDumpFS()
	sourceFS, err := GetObjectFSProvider(ses)
	if err != nil {
		return err
	}

	txn := ses.GetTxnHandler().GetTxn()
	txn.GetWorkspace().SetCloneTxn(txn.Txn().SnapshotTS.PhysicalTime)
	// A stage path can be targeted concurrently by multiple CNs, and object
	// stores do not give this workflow an exclusive create token. Do not delete
	// copied fixture objects on failure: that could remove files published by a
	// concurrent successful DUMP. Failed fixtures are reclaimed with the stage
	// fixture lifecycle.
	objectCount := 0
	for _, ref := range refs {
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
	if err = writeTableDumpJSON(ctx, dumpFS, tableDumpManifestName, manifest); err != nil {
		return err
	}
	if !stmt.MetadataOnly {
		if err = dumpFS.Write(ctx, fileservice.IOVector{FilePath: tableDumpReadyName, Entries: []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("1")}}}); err != nil {
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

func installTableDumpObject(ctx context.Context, dumpFS, targetFS fileservice.FileService, item tableDumpObject) error {
	if _, err := targetFS.StatFile(ctx, item.Name); err == nil {
		return verifyTableDumpObject(ctx, targetFS, item.Name, item.Size, item.SHA256)
	} else if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return err
	}
	size, hash, err := copyTableDumpFile(ctx, dumpFS, targetFS, item.FixturePath, item.Name)
	if err != nil {
		return err
	}
	checksumMismatch := item.SHA256 != "" && hash != "" && !strings.EqualFold(hash, item.SHA256)
	if size != item.Size || checksumMismatch {
		return moerr.NewInvalidInputNoCtxf("object %s does not match manifest checksum", item.Name)
	}
	if item.SHA256 != "" && hash == "" {
		if err = verifyTableDumpObject(ctx, targetFS, item.Name, item.Size, item.SHA256); err != nil {
			return err
		}
	}
	return nil
}

func installTableDumpObjects(
	ctx context.Context,
	dumpFS, targetFS fileservice.FileService,
	items []tableDumpObject,
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
				if err := installTableDumpObject(copyCtx, dumpFS, targetFS, items[i]); err != nil {
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
		bat.SetAttributes([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
		bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_binary.ToType())
		if err := colexec.ExpandObjectStatsToBatch(mp, false, bat, true, dataStats...); err != nil {
			bat.Clean(mp)
			return submitted, err
		}
		if err := rel.Write(ctx, bat); err != nil {
			bat.Clean(mp)
			return submitted, err
		}
		submitted = append(submitted, dataNames...)
		bat.Clean(mp)
	}
	if len(tombstoneStats) != 0 {
		bat := batch.NewWithSize(1)
		bat.SetAttributes([]string{catalog.ObjectMeta_ObjectStats})
		bat.Vecs[0] = vector.NewVec(types.T_binary.ToType())
		for i := range tombstoneStats {
			if err := vector.AppendBytes(bat.Vecs[0], tombstoneStats[i].Marshal(), false, mp); err != nil {
				bat.Clean(mp)
				return submitted, err
			}
		}
		bat.SetRowCount(len(tombstoneStats))
		if err := rel.Delete(context.WithValue(ctx, defines.SkipTransferKey{}, true), bat, ""); err != nil {
			bat.Clean(mp)
			return submitted, err
		}
		submitted = append(submitted, tombstoneNames...)
		bat.Clean(mp)
	}
	return submitted, nil
}

type cloneFileProtector interface {
	ProtectCloneFiles(names ...string)
}

func handleLoadTable(ctx context.Context, ses *Session, stmt *tree.LoadTable) (err error) {
	dbName, _, rel, err := getTableForDump(ctx, ses, stmt.Table)
	if err != nil {
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
	targetHash, err := tableSchemaHash(rel.GetTableDef(ctx))
	if err != nil {
		return err
	}
	if targetHash != manifest.SchemaHash {
		return moerr.NewInvalidInputNoCtx("target table schema does not match table dump")
	}
	targetRefs, err := getTableDumpRelations(ctx, ses, dbName, rel)
	if err != nil {
		return err
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
	sharedObjects := make([]string, 0)
	seenRelations := make(map[string]struct{}, len(manifest.Relations))
	seenObjects := make(map[string]struct{})
	for _, relationDump := range manifest.Relations {
		key := tableDumpRelationKey(relationDump.Role, relationDump.IndexName, relationDump.IndexAlgoTableType)
		if _, ok := seenRelations[key]; ok {
			return moerr.NewInvalidInputNoCtx("duplicate relation in table dump")
		}
		seenRelations[key] = struct{}{}
		targetRef, ok := targetByKey[key]
		if !ok || targetRef.SchemaHash != relationDump.SchemaHash {
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
			if manifest.MetadataOnly {
				if item.FixturePath != "" {
					return moerr.NewInvalidInputNoCtxf("metadata-only object %s contains a fixture path", item.Name)
				}
			} else if item.FixturePath != path.Join("objects", item.Name) || item.Size < 0 {
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
			// Object creation has no cross-CN ownership token. Protect every
			// installed or reused name from transaction rollback and let the
			// reference-aware object GC reclaim files left by a failed LOAD.
			sharedObjects = append(sharedObjects, item.Name)
		}
		if !manifest.MetadataOnly {
			if err = installTableDumpObjects(ctx, dumpFS, targetFS, relationDump.Objects); err != nil {
				return err
			}
		}
	}
	if len(sharedObjects) != 0 {
		protector, ok := workspace.(cloneFileProtector)
		if !ok {
			return moerr.NewNotSupportedNoCtx("LOAD TABLE reusing existing objects with this transaction workspace")
		}
		protector.ProtectCloneFiles(sharedObjects...)
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
	return nil
}
