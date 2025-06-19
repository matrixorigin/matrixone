// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/spf13/cobra"

	"go.uber.org/zap"
)

func init() {
	fileservice.RegisterAppConfig(&fileservice.AppConfig{
		Name: DumpTableDir,
		GCFn: GCDumpTableFiles,
	})
}

const (
	DumpTableDir = "dumpTable"
)

const (
	DumpTableFileTTL = time.Hour * 24 * 30
)

func DecodeDumpTableDir(dir string) (tid uint64, createTime time.Time, snapshotTS types.TS, err error) {
	parts := strings.Split(dir, "_")
	if len(parts) != 3 {
		return 0, time.Time{}, types.TS{}, moerr.NewInternalErrorNoCtx("invalid dump table directory")
	}
	tid, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, time.Time{}, types.TS{}, err
	}
	createTime, err = time.Parse("2006-01-02.15.04.05.MST", parts[1])
	if err != nil {
		return 0, time.Time{}, types.TS{}, err
	}
	snapshotTS = types.StringToTS(parts[2])
	return
}

func GCDumpTableFiles(filePath string, fs fileservice.FileService) (neesGC bool, err error) {
	_, createTime, _, err := DecodeDumpTableDir(filePath)
	if err != nil {
		return
	}
	_, injected := objectio.GCDumpTableInjected()
	if createTime.Add(time.Hour*24).Before(time.Now()) || injected {
		neesGC = true
		ctx := context.Background()
		ctx, cancel := context.WithTimeoutCause(ctx, 5*time.Second, moerr.CauseClearPersistTable)
		defer cancel()
		entrys := fs.List(ctx, filePath)
		var entry *fileservice.DirEntry
		for entry, err = range entrys {
			if err != nil {
				continue
			}
			if err = fs.Delete(ctx, path.Join(filePath, entry.Name)); err != nil {
				return
			}
		}
		if err = fs.Delete(ctx, filePath); err != nil {
			return
		}
		return
	}
	return
}

func GetDumpTableDir(tid uint64, snapshotTS types.TS) string {
	dir := fmt.Sprintf("%d_%v_%s", tid, time.Now().Format("2006-01-02.15.04.05.MST"), snapshotTS.ToString())
	return dir
}

const (
	DumpTableObjectList = "object_list"
	DumpTableSchema     = "schema"
	DumpTableTable      = "table"
)

const (
	ObjectListAttr_ObjectType  = "object_type"
	ObjectListAttr_ID          = "id"
	ObjectListAttr_CreateTS    = "create_ts"
	ObjectListAttr_DeleteTS    = "delete_ts"
	ObjectListAttr_IsPersisted = "is_persisted"
)

const (
	ObjectListAttr_ObjectType_Idx = iota
	ObjectListAttr_ID_Idx
	ObjectListAttr_CreateTS_Idx
	ObjectListAttr_DeleteTS_Idx
	ObjectListAttr_IsPersisted_Idx
)

var ObjectListAttrs = []string{
	ObjectListAttr_ObjectType,
	ObjectListAttr_ID,
	ObjectListAttr_CreateTS,
	ObjectListAttr_DeleteTS,
	ObjectListAttr_IsPersisted,
}

var ObjectListTypes = []types.Type{
	types.T_int8.ToType(),
	types.T_varchar.ToType(),
	types.T_TS.ToType(),
	types.T_TS.ToType(),
	types.T_bool.ToType(),
}
var ObjectListSeqnums = []uint16{0, 1, 2, 3, 4}

func NewObjectListBatch() *batch.Batch {
	return batch.NewWithSchema(false, ObjectListAttrs, ObjectListTypes)
}

type DumpTableArg struct {
	ctx             context.Context
	txn             txnif.AsyncTxn
	table           *catalog.TableEntry
	dir             string
	inspectContext  *inspectContext
	objectListBatch *batch.Batch
	mp              *mpool.MPool
	srcfs, dstfs    fileservice.FileService
}

// for UT
func NewDumpTableArg(
	ctx context.Context,
	table *catalog.TableEntry,
	dir string,
	inspectContext *inspectContext,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *DumpTableArg {
	return &DumpTableArg{
		ctx:             ctx,
		table:           table,
		dir:             dir,
		inspectContext:  inspectContext,
		objectListBatch: NewObjectListBatch(),
		mp:              mp,
		dstfs:           fs,
		srcfs:           fs,
	}
}
func (c *DumpTableArg) PrepareCommand() *cobra.Command {
	dumpTableCmd := &cobra.Command{
		Use:   "dump-table",
		Short: "Dump table",
		Run:   RunFactory(c),
	}
	dumpTableCmd.SetUsageTemplate(c.Usage())

	dumpTableCmd.Flags().IntP("tid", "t", 0, "set table id")
	dumpTableCmd.Flags().IntP("did", "d", 0, "set database id")
	return dumpTableCmd
}

func (c *DumpTableArg) FromCommand(cmd *cobra.Command) (err error) {
	tid, _ := cmd.Flags().GetInt("tid")
	did, _ := cmd.Flags().GetInt("did")
	if cmd.Flag("ictx") != nil {
		c.inspectContext = cmd.Flag("ictx").Value.(*inspectContext)
		c.mp = common.DefaultAllocator
		if c.dstfs, err = c.inspectContext.db.Opts.TmpFs.GetOrCreateApp(
			&fileservice.AppConfig{
				Name: DumpTableDir,
				GCFn: GCDumpTableFiles,
			},
		); err != nil {
			return
		}
		c.srcfs = c.inspectContext.db.Opts.Fs
		c.ctx = context.Background()
		database, err := c.inspectContext.db.Catalog.GetDatabaseByID(uint64(did))
		if err != nil {
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("get database by id %d failed", did))
			return err
		}
		c.table, err = database.GetTableEntryByID(uint64(tid))
		if err != nil {
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("get table by id %d-%d failed", did, tid))
			return err
		}
		c.objectListBatch = NewObjectListBatch()
	} else {
		return moerr.NewInternalErrorNoCtx("inspect context not found")
	}
	return nil
}

func (c *DumpTableArg) String() string {
	return "dump-table"
}

func (c *DumpTableArg) Usage() (res string) {
	res += "Available Commands:\n"
	res += fmt.Sprintf("  %-5v dump table data\n", "dump-table")

	res += "\n"
	res += "Usage:\n"
	res += "inspect table [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect table <command> --help\" for more information about a given command.\n"

	return
}
func (c *DumpTableArg) Run() (err error) {
	if c.txn, err = c.inspectContext.db.StartTxn(nil); err != nil {
		return
	}
	if c.dir == "" {
		c.dir = GetDumpTableDir(c.table.ID, c.txn.GetStartTS())
	}
	defer c.txn.Commit(c.ctx)
	logutil.Info(
		"DUMP-TABLE-START",
		zap.String(
			"table",
			fmt.Sprintf(
				"%d-%v, %d-%s",
				c.table.GetDB().ID,
				c.table.GetDB().GetFullName(),
				c.table.ID,
				c.table.GetFullName(),
			),
		),
		zap.String("dir", c.dir),
	)
	defer c.objectListBatch.Clean(c.mp)
	txn, err := c.inspectContext.db.StartTxn(nil)
	if err != nil {
		return err
	}
	defer txn.Commit(c.ctx)

	if err := c.flushTableSchema(); err != nil {
		return err
	}
	if err := c.flushTableEntry(); err != nil {
		return err
	}

	p := &catalog.LoopProcessor{}
	p.ObjectFn = c.onObject
	p.TombstoneFn = c.onObject
	if err = c.table.RecurLoop(p); err != nil {
		return err
	}
	if err := c.flush(DumpTableObjectList, c.objectListBatch); err != nil {
		return err
	}
	logutil.Info(
		"DUMP-TABLE-END",
		zap.String(
			"table",
			fmt.Sprintf(
				"%d-%v, %d-%s",
				c.table.GetDB().ID,
				c.table.GetDB().GetFullName(),
				c.table.ID,
				c.table.GetFullName(),
			),
		),
		zap.String("dir", c.dir),
		zap.Int("object_count", c.objectListBatch.RowCount()),
	)
	return nil
}

func (c *DumpTableArg) flushTableSchema() (err error) {
	bat := containers.NewBatch()
	typs := catalog.SystemColumnSchema.AllTypes()
	attrs := catalog.SystemColumnSchema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	for _, def := range catalog.SystemColumnSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		txnimpl.FillColumnRow(c.table, c.table.GetLastestSchema(false), def.Name, bat.Vecs[def.Idx])
	}
	cnBatch := containers.ToCNBatch(bat)
	if err := c.flush(DumpTableSchema, cnBatch); err != nil {
		return err
	}
	return nil
}

func (c *DumpTableArg) flushTableEntry() (err error) {
	bat := containers.NewBatch()
	typs := catalog.SystemTableSchema.AllTypes()
	attrs := catalog.SystemTableSchema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	for _, def := range catalog.SystemTableSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		txnimpl.FillTableRow(c.table, c.table.GetLastestSchema(false), def.Name, bat.Vecs[def.Idx])
	}
	cnBatch := containers.ToCNBatch(bat)
	if err := c.flush(DumpTableTable, cnBatch); err != nil {
		return err
	}
	return
}

func (c *DumpTableArg) onObject(e *catalog.ObjectEntry) error {
	startTS := c.txn.GetStartTS()
	if e.CreatedAt.EQ(&txnif.UncommitTS) || e.DeleteBefore(startTS) {
		return nil
	}
	bat, err := c.visitObjectData(e)
	if err != nil {
		return err
	}
	defer bat.Close()
	var isPersisted bool
	if isPersisted, err = c.collectObjectList(e); err != nil {
		return err
	}
	cnBatch := containers.ToCNBatch(bat)
	if isPersisted {
		name := e.ObjectStats.ObjectName().String()
		dstName := path.Join(c.dir, name)
		if _, err = fileservice.DoWithRetry(
			"CopyFile",
			func() ([]byte, error) {
				return copyFile(c.ctx, c.srcfs, c.dstfs, name, dstName)
			},
			64,
			fileservice.IsRetryableError,
		); err != nil {
			return err
		}
		logutil.Info(
			"DUMP-TABLE-FLUSH",
			zap.String(
				"table",
				fmt.Sprintf(
					"%d-%v, %d-%s",
					c.table.GetDB().ID,
					c.table.GetDB().GetFullName(),
					c.table.ID,
					c.table.GetFullName(),
				),
			),
			zap.String("dir", c.dir),
			zap.String("name", name),
		)
	} else {
		objectName := objectio.BuildObjectNameWithObjectID(e.ID())
		if err := c.flush(objectName.String(), cnBatch); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(
	ctx context.Context,
	srcFS, dstFS fileservice.FileService,
	srcName, dstName string,
) ([]byte, error) {
	var reader io.ReadCloser
	ioVec := &fileservice.IOVector{
		FilePath: srcName,
		Entries: []fileservice.IOEntry{
			{
				ReadCloserForRead: &reader,
				Offset:            0,
				Size:              -1,
			},
		},
		Policy: fileservice.SkipAllCache,
	}

	err := srcFS.Read(ctx, ioVec)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	// hash
	hasher := sha256.New()
	hashingReader := io.TeeReader(reader, hasher)
	dstIoVec := fileservice.IOVector{
		FilePath: dstName,
		Entries: []fileservice.IOEntry{
			{
				ReaderForWrite: hashingReader,
				Offset:         0,
				Size:           -1,
			},
		},
		Policy: fileservice.SkipAllCache,
	}

	err = dstFS.Write(ctx, dstIoVec)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func (c *DumpTableArg) flush(name string, bat *batch.Batch) (err error) {
	nameWithDir := fmt.Sprintf("%s/%s", c.dir, name)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterDumpTable, nameWithDir, c.dstfs)
	if err != nil {
		return
	}
	if _, err = writer.Write(bat); err != nil {
		return
	}
	_, err = writer.WriteEnd(c.ctx)
	if err != nil {
		return
	}
	logutil.Info(
		"DUMP-TABLE-FLUSH",
		zap.String(
			"table",
			fmt.Sprintf(
				"%d-%v, %d-%s",
				c.table.GetDB().ID,
				c.table.GetDB().GetFullName(),
				c.table.ID,
				c.table.GetFullName(),
			),
		),
		zap.String("dir", c.dir),
		zap.String("name", name),
	)
	return
}

func (c *DumpTableArg) collectObjectList(e *catalog.ObjectEntry) (isPersisted bool, err error) {
	startTS := c.txn.GetStartTS()
	var objectType int8
	if e.IsTombstone {
		objectType = ckputil.ObjectType_Tombstone
	} else {
		objectType = ckputil.ObjectType_Data
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_ObjectType_Idx], objectType, false, c.mp,
	); err != nil {
		return false, err
	}
	if err := vector.AppendBytes(
		c.objectListBatch.Vecs[ObjectListAttr_ID_Idx], []byte(e.ObjectStats[:]), false, c.mp,
	); err != nil {
		return false, err
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_CreateTS_Idx], e.CreatedAt, false, c.mp,
	); err != nil {
		return false, err
	}
	var deleteTS types.TS
	if e.DeletedAt.EQ(&txnif.UncommitTS) || e.DeletedAt.LT(&startTS) {
		deleteTS = types.TS{}
	} else {
		deleteTS = e.DeletedAt
	}
	if e.GetAppendable() && deleteTS.IsEmpty() {
		isPersisted = false
	} else {
		isPersisted = true
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_DeleteTS_Idx], deleteTS, false, c.mp,
	); err != nil {
		return false, err
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_IsPersisted_Idx], isPersisted, false, c.mp,
	); err != nil {
		return false, err
	}
	c.objectListBatch.SetRowCount(c.objectListBatch.Vecs[0].Length())
	return
}

func (c *DumpTableArg) visitObjectData(e *catalog.ObjectEntry) (bat *containers.Batch, err error) {
	schema := e.GetTable().GetLastestSchema(e.IsTombstone)
	colIdxes := make([]int, 0)
	// user rows, rowID, commitTS
	for i := range schema.ColDefs {
		colIdxes = append(colIdxes, i)
	}
	colIdxes = append(colIdxes, objectio.SEQNUM_COMMITTS)
	if err = e.GetObjectData().Scan(c.ctx, &bat, c.txn, schema, 0, colIdxes, c.mp); err != nil {
		return
	}
	return
}
