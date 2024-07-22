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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/spf13/cobra"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	invalidId = 0x3f3f3f3f

	brief    = 0
	standard = 1
	detailed = 2
)

func getInputs(input string, result *[]int) error {
	*result = make([]int, 0)
	if input == "" {
		return nil
	}
	items := strings.Split(input, ",")
	for _, item := range items {
		item = strings.TrimSpace(item)
		num, err := strconv.Atoi(item)
		if err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("invalid number '%s'", item))
		}
		*result = append(*result, num)
	}
	return nil
}

func formatBytes(bytes int) string {
	const (
		_      = iota
		KB int = 1 << (10 * iota)
		MB
		GB
		TB
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

type MoInspectArg struct {
}

func (c *MoInspectArg) PrepareCommand() *cobra.Command {
	moInspectCmd := &cobra.Command{
		Use:   "mo_inspect",
		Short: "mo_inspect",
		Run:   RunFactory(c),
	}

	obj := ObjArg{}
	moInspectCmd.AddCommand(obj.PrepareCommand())

	table := TableArg{}
	moInspectCmd.AddCommand(table.PrepareCommand())

	return moInspectCmd
}

func (c *MoInspectArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *MoInspectArg) String() string {
	return "mo_inspect"
}

func (c *MoInspectArg) Run() (err error) {
	return
}

type ObjArg struct {
}

func (c *ObjArg) PrepareCommand() *cobra.Command {
	objCmd := &cobra.Command{
		Use:   "obj",
		Short: "obj",
		Run:   RunFactory(c),
	}

	stat := moObjStatArg{}
	objCmd.AddCommand(stat.PrepareCommand())

	get := objGetArg{}
	objCmd.AddCommand(get.PrepareCommand())

	return objCmd
}

func (c *ObjArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *ObjArg) String() string {
	return "obj"
}

func (c *ObjArg) Run() (err error) {
	return
}

type moObjStatArg struct {
	ctx    *inspectContext
	level  int
	dir    string
	name   string
	id     int
	fs     fileservice.FileService
	reader *objectio.ObjectReader
	res    string
	local  bool
}

func (c *moObjStatArg) PrepareCommand() *cobra.Command {
	var statCmd = &cobra.Command{
		Use:   "stat",
		Short: "obj stat",
		Run:   RunFactory(c),
	}

	statCmd.Flags().IntP("id", "i", invalidId, "id")
	statCmd.Flags().IntP("level", "l", brief, "level")
	statCmd.Flags().StringP("name", "n", "", "name")
	statCmd.Flags().BoolP("local", "", false, "local")

	return statCmd
}

func (c *moObjStatArg) FromCommand(cmd *cobra.Command) (err error) {
	c.id, _ = cmd.Flags().GetInt("id")
	c.level, _ = cmd.Flags().GetInt("level")
	c.local, _ = cmd.Flags().GetBool("local")
	path, _ := cmd.Flags().GetString("name")
	c.dir, c.name = filepath.Split(path)
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	}

	return nil
}

func (c *moObjStatArg) String() string {
	return fmt.Sprintf("\n%v", c.res)
}

func (c *moObjStatArg) Run() (err error) {
	if err = c.checkInputs(); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid inputs: %v\n", err))
	}

	if c.ctx != nil {
		c.fs = c.ctx.db.Runtime.Fs.Service
	}

	if err = c.InitReader(c.name); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init reader %v", err))
	}

	c.res, err = c.GetStat()

	return
}

func (c *moObjStatArg) initFs(local bool) (err error) {
	if local {
		cfg := fileservice.Config{
			Name:    defines.LocalFileServiceName,
			Backend: "DISK",
			DataDir: c.dir,
			Cache:   fileservice.DisabledCacheConfig,
		}
		c.fs, err = fileservice.NewFileService(context.Background(), cfg, nil)
		return
	}

	arg := fileservice.ObjectStorageArguments{
		Name:     defines.SharedFileServiceName,
		Endpoint: "DISK",
		Bucket:   c.dir,
	}
	c.fs, err = fileservice.NewS3FS(context.Background(), arg, fileservice.DisabledCacheConfig, nil, false, true)
	return
}

func (c *moObjStatArg) InitReader(name string) (err error) {
	if c.fs == nil {
		err = c.initFs(c.local)
		if err != nil {
			return err
		}
	}

	c.reader, err = objectio.NewObjectReaderWithStr(name, c.fs)

	return err
}

func (c *moObjStatArg) checkInputs() error {
	if c.level != brief && c.level != standard && c.level != detailed {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid level %v, should be 0, 1, 2 ", c.level))
	}

	if c.name == "" {
		return moerr.NewInfoNoCtx("empty name")
	}

	return nil
}

func (c *moObjStatArg) GetStat() (res string, err error) {
	var m *mpool.MPool
	var meta objectio.ObjectMeta
	if m, err = mpool.NewMPool("data", 0, mpool.NoFixed); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to init mpool, err: %v", err))
		return
	}
	if meta, err = c.reader.ReadAllMeta(context.Background(), m); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to read meta, err: %v", err))
		return
	}

	switch c.level {
	case brief:
		res, err = c.GetBriefStat(&meta)
	case standard:
		res, err = c.GetStandardStat(&meta)
	case detailed:
		res, err = c.GetDetailedStat(&meta)
	}
	return
}

func (c *moObjStatArg) GetBriefStat(obj *objectio.ObjectMeta) (res string, err error) {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx("no data")
		return
	}

	cnt := data.BlockCount()
	header := data.BlockHeader()
	ext := c.reader.GetMetaExtent()
	res = fmt.Sprintf("object %v has %v blocks, %v rows, %v cols, object size %v", c.name, cnt, header.Rows(), header.ColumnCount(), ext.Length())

	return
}

func (c *moObjStatArg) GetStandardStat(obj *objectio.ObjectMeta) (res string, err error) {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx("no data")
		return
	}

	var blocks []objectio.BlockObject
	cnt := data.BlockCount()

	if c.id != invalidId {
		println(uint32(c.id))
		if uint32(c.id) > cnt {
			err = moerr.NewInfoNoCtx(fmt.Sprintf("id %3d out of block count %3d", c.id, cnt))
			return
		}
		blocks = append(blocks, data.GetBlockMeta(uint32(c.id)))
	} else {
		for i := range cnt {
			blk := data.GetBlockMeta(i)
			blocks = append(blocks, blk)
		}
	}

	header := data.BlockHeader()
	ext := c.reader.GetMetaExtent()
	res += fmt.Sprintf("object %v has %v blocks, %v rows, %v cols, object size %v\n", c.name, cnt, header.Rows(), header.ColumnCount(), ext.Length())
	for _, blk := range blocks {
		res += fmt.Sprintf("block %3d: rows %4v, cols %3v\n", blk.GetID(), blk.GetRows(), blk.GetColumnCount())
	}

	return
}

func (c *moObjStatArg) GetDetailedStat(obj *objectio.ObjectMeta) (res string, err error) {
	meta := *obj
	data, ok := meta.DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx("no data")
		return
	}

	var blocks []objectio.BlockObject
	cnt := data.BlockCount()
	if c.id != invalidId {
		if uint32(c.id) >= cnt {
			err = moerr.NewInfoNoCtx(fmt.Sprintf("id %v out of block count %v", c.id, cnt))
			return
		}
		blocks = append(blocks, data.GetBlockMeta(uint32(c.id)))
	} else {
		for i := range cnt {
			blk := data.GetBlockMeta(i)
			blocks = append(blocks, blk)
		}
	}

	res += fmt.Sprintf("object %v has %3d blocks\n", c.name, cnt)
	for _, blk := range blocks {
		cnt := blk.GetColumnCount()
		res += fmt.Sprintf("block %3d has %3d cloumns\n", blk.GetID(), cnt)

		for i := range cnt {
			col := blk.ColumnMeta(i)
			res += fmt.Sprintf("    cloumns %3d, ndv %3d, null cnt %3d, zonemap %v\n", i, col.Ndv(), col.NullCnt(), col.ZoneMap())
		}
	}

	return
}

type objGetArg struct {
	ctx        *inspectContext
	dir        string
	name       string
	id         int
	cols, rows []int
	col, row   string
	fs         fileservice.FileService
	reader     *objectio.ObjectReader
	res        string
	local      bool
}

func (c *objGetArg) PrepareCommand() *cobra.Command {
	var getCmd = &cobra.Command{
		Use:   "get",
		Short: "obj get",
		Run:   RunFactory(c),
	}
	getCmd.Flags().IntP("id", "i", invalidId, "id")
	getCmd.Flags().StringP("name", "n", "", "name")
	getCmd.Flags().StringP("col", "c", "", "col")
	getCmd.Flags().StringP("row", "r", "", "row")
	getCmd.Flags().BoolP("local", "", false, "local")

	return getCmd
}

func (c *objGetArg) FromCommand(cmd *cobra.Command) (err error) {
	c.id, _ = cmd.Flags().GetInt("id")
	c.col, _ = cmd.Flags().GetString("col")
	c.row, _ = cmd.Flags().GetString("row")
	c.local, _ = cmd.Flags().GetBool("local")
	path, _ := cmd.Flags().GetString("name")
	c.dir, c.name = filepath.Split(path)
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	}

	return nil
}

func (c *objGetArg) String() string {
	return fmt.Sprintf("\n%v", c.res)
}

func (c *objGetArg) Run() (err error) {
	if err = c.checkInputs(); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid inputs: %v\n", err))
	}

	if c.ctx != nil {
		c.fs = c.ctx.db.Runtime.Fs.Service
	}

	if err = c.InitReader(c.name); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init reader: %v", err))
	}

	c.res, err = c.GetData()

	return
}

func (c *objGetArg) initFs(local bool) (err error) {
	if local {
		cfg := fileservice.Config{
			Name:    defines.LocalFileServiceName,
			Backend: "DISK",
			DataDir: c.dir,
			Cache:   fileservice.DisabledCacheConfig,
		}
		c.fs, err = fileservice.NewFileService(context.Background(), cfg, nil)
		return
	}

	arg := fileservice.ObjectStorageArguments{
		Name:     defines.SharedFileServiceName,
		Endpoint: "DISK",
		Bucket:   c.dir,
	}
	c.fs, err = fileservice.NewS3FS(context.Background(), arg, fileservice.DisabledCacheConfig, nil, false, true)
	return
}

func (c *objGetArg) InitReader(name string) (err error) {
	if c.fs == nil {
		err = c.initFs(c.local)
		if err != nil {
			return err
		}
	}

	c.reader, err = objectio.NewObjectReaderWithStr(name, c.fs)

	return err
}

func (c *objGetArg) checkInputs() error {
	if err := getInputs(c.col, &c.cols); err != nil {
		return err
	}
	if err := getInputs(c.row, &c.rows); err != nil {
		return err
	}
	if len(c.rows) > 2 || (len(c.rows) == 2 && c.rows[0] >= c.rows[1]) {
		return moerr.NewInfoNoCtx("invalid rows, need two inputs [leftm, right)")
	}
	if c.name == "" {
		return moerr.NewInfoNoCtx("empty name")
	}

	return nil
}

func (c *objGetArg) GetData() (res string, err error) {
	var m *mpool.MPool
	var meta objectio.ObjectMeta
	if m, err = mpool.NewMPool("data", 0, mpool.NoFixed); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to init mpool, err: %v", err))
		return
	}
	if meta, err = c.reader.ReadAllMeta(context.Background(), m); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to read meta, err: %v", err))
		return
	}

	cnt := meta.DataMetaCount()
	if c.id == invalidId || uint16(c.id) >= cnt {
		err = moerr.NewInfoNoCtx("invalid id")
		return
	}

	blocks, _ := meta.DataMeta()
	blk := blocks.GetBlockMeta(uint32(c.id))
	cnt = blk.GetColumnCount()
	idxs := make([]uint16, 0)
	typs := make([]types.Type, 0)
	if len(c.cols) == 0 {
		for i := range cnt {
			c.cols = append(c.cols, int(i))
		}
	}
	for _, i := range c.cols {
		idx := uint16(i)
		if idx >= cnt {
			err = moerr.NewInfoNoCtx(fmt.Sprintf("column %v out of colum count %v", idx, cnt))
			return
		}
		col := blk.ColumnMeta(idx)
		idxs = append(idxs, idx)
		tp := types.T(col.DataType()).ToType()
		typs = append(typs, tp)
	}

	v, _ := c.reader.ReadOneBlock(context.Background(), idxs, typs, uint16(c.id), m)
	for i, entry := range v.Entries {
		obj, _ := objectio.Decode(entry.CachedData.Bytes())
		vec := obj.(*vector.Vector)
		if len(c.rows) != 0 {
			var left, right int
			left = c.rows[0]
			if len(c.rows) == 1 {
				right = left + 1
			} else {
				right = c.rows[1]
			}
			if uint32(left) >= blk.GetRows() || uint32(right) > blk.GetRows() {
				err = moerr.NewInfoNoCtx(fmt.Sprintf("invalid rows %v out of row count %v", c.rows, blk.GetRows()))
				return
			}
			vec, _ = vec.Window(left, right)
		}
		res += fmt.Sprintf("col %d:\n%v\n", c.cols[i], vec)
	}

	return
}

type TableArg struct {
}

func (c *TableArg) PrepareCommand() *cobra.Command {
	tableCmd := &cobra.Command{
		Use:   "table",
		Short: "table",
		Run:   RunFactory(c),
	}

	stat := tableStatArg{}
	tableCmd.AddCommand(stat.PrepareCommand())

	return tableCmd
}

func (c *TableArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *TableArg) String() string {
	return "table"
}

func (c *TableArg) Run() error {
	return nil
}

type tableStatArg struct {
	ctx                     *inspectContext
	did, tid, ori, com, cnt int
	name                    string
}

func (c *tableStatArg) PrepareCommand() *cobra.Command {
	moInspectCmd := &cobra.Command{
		Use:   "stat",
		Short: "table stat",
		Run:   RunFactory(c),
	}

	moInspectCmd.Flags().IntP("tid", "t", 0, "set table id")
	moInspectCmd.Flags().IntP("did", "d", 0, "set database id")
	return moInspectCmd
}

func (c *tableStatArg) FromCommand(cmd *cobra.Command) (err error) {
	c.tid, _ = cmd.Flags().GetInt("tid")
	c.did, _ = cmd.Flags().GetInt("did")
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*inspectContext)

	}
	return nil
}

func (c *tableStatArg) String() string {
	return fmt.Sprintf("table %v has %v objects, compacted size %v, original size %v", c.name, c.cnt, formatBytes(c.com), formatBytes(c.ori))
}

func (c *tableStatArg) Run() (err error) {
	if c.ctx == nil {
		return moerr.NewInfoNoCtx("it is an online command")
	}
	if c.did == 0 {
		return moerr.NewInfoNoCtx("invalid database id")
	}
	if c.tid == 0 {
		return moerr.NewInfoNoCtx("invalid table id")
	}
	db, err := c.ctx.db.Catalog.GetDatabaseByID(uint64(c.did))
	if err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get db %v", c.did))
	}
	table, err := db.GetTableEntryByID(uint64(c.tid))
	if err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get table %v", c.tid))
	}
	c.name = table.GetFullName()
	it := table.MakeObjectIt(true)
	defer it.Release()
	for it.Next() {
		entry := it.Item()
		c.ori += entry.GetOriginSize()
		c.com += entry.GetCompSize()
		c.cnt++
	}

	return
}
