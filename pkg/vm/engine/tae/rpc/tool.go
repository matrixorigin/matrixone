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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/spf13/cobra"
)

const (
	invalidId    = 0xffffff
	invalidLimit = 0xffffff

	brief    = 0
	standard = 1
	detailed = 2
)

type ColumnJson struct {
	Index       uint16 `json:"col_index"`
	Ndv         uint32 `json:"ndv,omitempty"`
	NullCnt     uint32 `json:"null_count,omitempty"`
	DataSize    string `json:"data_size,omitempty"`
	OriDataSize string `json:"original_data_size,omitempty"`
	Zonemap     string `json:"zonemap,omitempty"`
	Data        string `json:"data,omitempty"`
}

type BlockJson struct {
	Index   uint16       `json:"block_index"`
	Rows    uint32       `json:"row_count,omitempty"`
	Cols    uint16       `json:"column_count,omitempty"`
	Columns []ColumnJson `json:"columns,omitempty"`
}

type ObjectJson struct {
	Name        string       `json:"name"`
	Rows        uint32       `json:"row_count,omitempty"`
	Cols        uint16       `json:"column_count,omitempty"`
	BlkCnt      uint32       `json:"block_count,omitempty"`
	MetaSize    string       `json:"meta_size,omitempty"`
	OriMetaSize string       `json:"original_meta_size,omitempty"`
	DataSize    string       `json:"data_size,omitempty"`
	OriDataSize string       `json:"original_data_size,omitempty"`
	Zonemap     string       `json:"zonemap,omitempty"`
	Columns     []ColumnJson `json:"columns,omitempty"`
	Blocks      []BlockJson  `json:"blocks,omitempty"`
}

type tableStatJson struct {
	Name         string `json:"name"`
	ObjectCount  int    `json:"object_count"`
	Size         string `json:"size"`
	OriginalSize string `json:"original_size"`
}

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

func formatBytes(bytes uint32) string {
	const (
		_         = iota
		KB uint32 = 1 << (10 * iota)
		MB
		GB
	)

	switch {
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
		Use:   "inspect",
		Short: "Mo inspect",
		Long:  "Mo inspect is a visualization analysis tool",
		Run:   RunFactory(c),
	}

	moInspectCmd.SetUsageTemplate(c.Usage())

	obj := ObjArg{}
	moInspectCmd.AddCommand(obj.PrepareCommand())

	table := TableArg{}
	moInspectCmd.AddCommand(table.PrepareCommand())

	ckp := CheckpointArg{}
	moInspectCmd.AddCommand(ckp.PrepareCommand())

	return moInspectCmd
}

func (c *MoInspectArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *MoInspectArg) String() string {
	return "mo_inspect"
}

func (c *MoInspectArg) Usage() (res string) {
	res += "Offline Commands:\n"
	res += fmt.Sprintf("  %-8v show object information\n", "object")

	res += "\n"
	res += "Online Commands:\n"
	res += fmt.Sprintf("  %-8v show table information\n", "table")

	res += "\n"
	res += "Usage:\n"
	res += "inspect [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect <command> --help\" for more information about a given command.\n"

	return
}

func (c *MoInspectArg) Run() (err error) {
	return
}

type ObjArg struct {
}

func (c *ObjArg) PrepareCommand() *cobra.Command {
	objCmd := &cobra.Command{
		Use:   "object",
		Short: "object",
		Long:  "Display information about a given object",
		Run:   RunFactory(c),
	}

	objCmd.SetUsageTemplate(c.Usage())

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
	return "object"
}

func (c *ObjArg) Usage() (res string) {
	res += "Available Commands:\n"
	res += fmt.Sprintf("  %-5v show object information\n", "stat")
	res += fmt.Sprintf("  %-5v get data from object\n", "get")

	res += "\n"
	res += "Usage:\n"
	res += "inspect object [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect object <command> --help\" for more information about a given command.\n"

	return
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
	col    int
	fs     fileservice.FileService
	reader *objectio.ObjectReader
	res    string
	local  bool
}

func (c *moObjStatArg) PrepareCommand() *cobra.Command {
	var statCmd = &cobra.Command{
		Use:   "stat",
		Short: "object stat",
		Long:  "Display status about a given object",
		Run:   RunFactory(c),
	}

	statCmd.SetUsageTemplate(c.Usage())

	statCmd.Flags().IntP("id", "i", invalidId, "block id")
	statCmd.Flags().IntP("col", "c", invalidId, "column id")
	statCmd.Flags().IntP("level", "l", brief, "level")
	statCmd.Flags().StringP("name", "n", "", "name")
	statCmd.Flags().BoolP("local", "", false, "local")

	return statCmd
}

func (c *moObjStatArg) FromCommand(cmd *cobra.Command) (err error) {
	c.id, _ = cmd.Flags().GetInt("id")
	c.col, _ = cmd.Flags().GetInt("col")
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
	return c.res
}

func (c *moObjStatArg) Usage() (res string) {
	res += "Examples:\n"
	res += "  # Display information about object\n"
	res += "  inspect object stat -n /your/path/obj-name\n"
	res += "\n"
	res += "  # Display information about object block idx with level 1\n"
	res += "  inspect object stat -n /your/path/obj-name -i idx -l 1\n"
	res += "\n"
	res += "  # Display information about object with local fs\n"
	res += "  inspect object stat -n /your/path/obj-name --local\n"

	res += "\n"
	res += "Options:\n"
	res += "  -n, --name='':\n"
	res += "    The path to the object file\n"
	res += "  -i, --idx=invalidId:\n"
	res += "    The sequence number of the block in the object\n"
	res += "  -c, --col=invalidId:\n"
	res += "    The sequence number of the column in the object\n"
	res += "  -l, --level=0:\n"
	res += "    The level of detail of the information, should be 0(brief), 1(standard), 2(detailed)\n"
	res += "  --local=false:\n"
	res += "    If the file is downloaded from a standalone machine, you should use this flag\n"

	return
}

func (c *moObjStatArg) Run() (err error) {
	ctx := context.Background()
	if err = c.checkInputs(); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid inputs: %v\n", err))
	}

	if c.ctx != nil {
		c.fs = c.ctx.db.Runtime.Fs.Service
	}

	if err = c.InitReader(ctx, c.name); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init reader %v", err))
	}

	c.res, err = c.GetStat(ctx)

	return
}

func (c *moObjStatArg) initFs(ctx context.Context, local bool) (err error) {
	if local {
		cfg := fileservice.Config{
			Name:    defines.LocalFileServiceName,
			Backend: "DISK",
			DataDir: c.dir,
			Cache:   fileservice.DisabledCacheConfig,
		}
		c.fs, err = fileservice.NewFileService(ctx, cfg, nil)
		return
	}

	arg := fileservice.ObjectStorageArguments{
		Name:     defines.SharedFileServiceName,
		Endpoint: "DISK",
		Bucket:   c.dir,
	}
	c.fs, err = fileservice.NewS3FS(ctx, arg, fileservice.DisabledCacheConfig, nil, false, true)
	return
}

func (c *moObjStatArg) InitReader(ctx context.Context, name string) (err error) {
	if c.fs == nil {
		err = c.initFs(ctx, c.local)
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

func (c *moObjStatArg) GetStat(ctx context.Context) (res string, err error) {
	var m *mpool.MPool
	var meta objectio.ObjectMeta
	if m, err = mpool.NewMPool("data", 0, mpool.NoFixed); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to init mpool, err: %v", err))
		return
	}
	if meta, err = c.reader.ReadAllMeta(ctx, m); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to read meta, err: %v", err))
		return
	}

	if c.level < standard && c.col != invalidId {
		c.level = standard
	}

	if c.level < detailed && c.id != invalidId {
		c.level = detailed
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

func (c *moObjStatArg) GetObjSize(data *objectio.ObjectDataMeta) []string {
	var dataSize, oriDataSize uint32
	header := data.BlockHeader()
	cnt := header.ColumnCount()

	for i := range cnt {
		col := data.MustGetColumn(i)
		dataSize += col.Location().Length()
		oriDataSize += col.Location().OriginSize()
	}

	return []string{
		formatBytes(dataSize),
		formatBytes(oriDataSize),
	}
}

func (c *moObjStatArg) GetBriefStat(obj *objectio.ObjectMeta) (res string, err error) {
	data, ok := (*obj).DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx("no data")
		return
	}

	header := data.BlockHeader()
	ext := c.reader.GetMetaExtent()
	size := c.GetObjSize(&data)

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	o := ObjectJson{
		Name:        c.name,
		Rows:        header.Rows(),
		Cols:        header.ColumnCount(),
		BlkCnt:      data.BlockCount(),
		MetaSize:    formatBytes(ext.Length()),
		OriMetaSize: formatBytes(ext.OriginSize()),
		DataSize:    size[0],
		OriDataSize: size[1],
	}

	data, err = json.MarshalIndent(o, "", "  ")
	if err != nil {
		return
	}

	res = string(data)
	return
}

func (c *moObjStatArg) GetStandardStat(obj *objectio.ObjectMeta) (res string, err error) {
	data, ok := (*obj).DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx("no data")
		return
	}

	header := data.BlockHeader()

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	colCnt := header.ColumnCount()
	if c.col != invalidId && c.col >= int(colCnt) {
		return "", moerr.NewInfoNoCtx("invalid column count")
	}
	cols := make([]ColumnJson, 0, colCnt)
	addColumn := func(idx uint16) {
		col := data.MustGetColumn(idx)
		cols = append(cols, ColumnJson{
			Index:       idx,
			DataSize:    formatBytes(col.Location().Length()),
			OriDataSize: formatBytes(col.Location().OriginSize()),
			Zonemap:     col.ZoneMap().String(),
		})
	}
	if c.col != invalidId {
		addColumn(uint16(c.col))
	} else {
		for i := range colCnt {
			addColumn(i)
		}
	}

	ext := c.reader.GetMetaExtent()
	size := c.GetObjSize(&data)

	o := ObjectJson{
		Name:        c.name,
		Rows:        header.Rows(),
		Cols:        header.ColumnCount(),
		BlkCnt:      data.BlockCount(),
		MetaSize:    formatBytes(ext.Length()),
		OriMetaSize: formatBytes(ext.OriginSize()),
		DataSize:    size[0],
		OriDataSize: size[1],
		Columns:     cols,
	}

	data, err = json.MarshalIndent(o, "", "  ")
	if err != nil {
		return
	}

	res = string(data)

	return
}

func (c *moObjStatArg) GetDetailedStat(obj *objectio.ObjectMeta) (res string, err error) {
	data, ok := (*obj).DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx("no data")
		return
	}

	cnt := data.BlockCount()
	blocks := make([]objectio.BlockObject, 0, cnt)
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

	blks := make([]BlockJson, 0, len(blocks))
	for _, blk := range blocks {
		colCnt := blk.GetColumnCount()

		cols := make([]ColumnJson, 0, colCnt)
		addColumn := func(idx uint16) {
			col := data.MustGetColumn(idx)
			cols = append(cols, ColumnJson{
				Index:       idx,
				DataSize:    formatBytes(col.Location().Length()),
				OriDataSize: formatBytes(col.Location().OriginSize()),
				Zonemap:     col.ZoneMap().String(),
			})
		}
		if c.col != invalidId {
			addColumn(uint16(c.col))
		} else {
			for i := range colCnt {
				addColumn(i)
			}
		}
		blkjson := BlockJson{
			Index:   blk.GetID(),
			Rows:    blk.GetRows(),
			Cols:    blk.GetColumnCount(),
			Columns: cols,
		}
		blks = append(blks, blkjson)
	}

	header := data.BlockHeader()
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	colCnt := header.ColumnCount()
	cols := make([]ColumnJson, colCnt)
	for i := range colCnt {
		col := data.MustGetColumn(i)
		cols[i] = ColumnJson{
			Index:       i,
			DataSize:    formatBytes(col.Location().Length()),
			OriDataSize: formatBytes(col.Location().OriginSize()),
			Zonemap:     col.ZoneMap().String(),
		}
	}

	ext := c.reader.GetMetaExtent()
	size := c.GetObjSize(&data)

	o := ObjectJson{
		Name:        c.name,
		Rows:        header.Rows(),
		Cols:        header.ColumnCount(),
		BlkCnt:      data.BlockCount(),
		MetaSize:    formatBytes(ext.Length()),
		OriMetaSize: formatBytes(ext.OriginSize()),
		DataSize:    size[0],
		OriDataSize: size[1],
		Blocks:      blks,
		Columns:     cols,
	}

	data, err = json.MarshalIndent(o, "", "  ")
	if err != nil {
		return
	}

	res = string(data)

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
		Short: "object get",
		Long:  "Display data about a given object",
		Run:   RunFactory(c),
	}

	getCmd.SetUsageTemplate(c.Usage())

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
	return c.res
}

func (c *objGetArg) Usage() (res string) {
	res += "Examples:\n"
	res += "  # Display the data of the idxth block of this object\n"
	res += "  inspect object get -n /your/path/obj-name -i idx\n"
	res += "\n"
	res += "  # Display the data of the specified row and column of the idxth block of this object\n"
	res += "  inspect object get -n /your/path/obj-name -i idx -c \"1,2,4\" -r\"0,50\"\n"

	res += "\n"
	res += "Options:\n"
	res += "  -n, --name='':\n"
	res += "    The path to the object file\n"
	res += "  -i, --idx=invalidId:\n"
	res += "    The sequence number of the block in the object\n"
	res += "  -c, --col='':\n"
	res += "    Specify the columns to display, should be \"0,2,19\"\n"
	res += "  -r, --row='':\n"
	res += "    Specify the rows to display, should be \"left,right\", means [left,right)\n"
	res += "  --local=false:\n"
	res += "    If the file is downloaded from a standalone machine, you should use this flag\n"

	return
}

func (c *objGetArg) Run() (err error) {
	ctx := context.Background()
	if err = c.checkInputs(); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("invalid inputs: %v\n", err))
	}

	if c.ctx != nil {
		c.fs = c.ctx.db.Runtime.Fs.Service
	}

	if err = c.InitReader(ctx, c.name); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init reader: %v", err))
	}

	c.res, err = c.GetData(ctx)

	return
}

func (c *objGetArg) initFs(ctx context.Context, local bool) (err error) {
	if local {
		cfg := fileservice.Config{
			Name:    defines.LocalFileServiceName,
			Backend: "DISK",
			DataDir: c.dir,
			Cache:   fileservice.DisabledCacheConfig,
		}
		c.fs, err = fileservice.NewFileService(ctx, cfg, nil)
		return
	}

	arg := fileservice.ObjectStorageArguments{
		Name:     defines.SharedFileServiceName,
		Endpoint: "DISK",
		Bucket:   c.dir,
	}
	c.fs, err = fileservice.NewS3FS(ctx, arg, fileservice.DisabledCacheConfig, nil, false, true)
	return
}

func (c *objGetArg) InitReader(ctx context.Context, name string) (err error) {
	if c.fs == nil {
		err = c.initFs(ctx, c.local)
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

func (c *objGetArg) GetData(ctx context.Context) (res string, err error) {
	var m *mpool.MPool
	var meta objectio.ObjectMeta
	if m, err = mpool.NewMPool("data", 0, mpool.NoFixed); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to init mpool, err: %v", err))
		return
	}
	ctx1, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if meta, err = c.reader.ReadAllMeta(ctx1, m); err != nil {
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
		col.ZoneMap()
		idxs = append(idxs, idx)
		tp := types.T(col.DataType()).ToType()
		typs = append(typs, tp)
	}

	ctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	v, _ := c.reader.ReadOneBlock(ctx2, idxs, typs, uint16(c.id), m)
	defer v.Release()
	cols := make([]ColumnJson, 0, len(v.Entries))
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

		col := ColumnJson{
			Index: uint16(c.cols[i]),
			Data:  vec.String(),
		}
		cols = append(cols, col)
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	o := BlockJson{
		Index:   blk.GetID(),
		Cols:    blk.GetColumnCount(),
		Columns: cols,
	}
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return
	}

	res = string(data)

	return
}

type TableArg struct {
}

func (c *TableArg) PrepareCommand() *cobra.Command {
	tableCmd := &cobra.Command{
		Use:   "table",
		Short: "table",
		Long:  "Display information about a given object",
		Run:   RunFactory(c),
	}

	tableCmd.SetUsageTemplate(c.Usage())

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

func (c *TableArg) Usage() (res string) {
	res += "Available Commands:\n"
	res += fmt.Sprintf("  %-5v show table information\n", "stat")

	res += "\n"
	res += "Usage:\n"
	res += "inspect table [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect table <command> --help\" for more information about a given command.\n"

	return
}

func (c *TableArg) Run() error {
	return nil
}

type tableStatArg struct {
	ctx                     *inspectContext
	did, tid, ori, com, cnt int
	name, res               string
}

func (c *tableStatArg) PrepareCommand() *cobra.Command {
	statCmd := &cobra.Command{
		Use:   "stat",
		Short: "table stat",
		Long:  "Display status about a given table",
		Run:   RunFactory(c),
	}

	statCmd.SetUsageTemplate(c.Usage())

	statCmd.Flags().IntP("tid", "t", 0, "set table id")
	statCmd.Flags().IntP("did", "d", 0, "set database id")
	return statCmd
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
	return c.res
}

func (c *tableStatArg) Usage() (res string) {
	res += "Examples:\n"
	res += "  # Display the information of table\n"
	res += "  inspect table stat -d did -t tid\n"

	res += "\n"
	res += "Options:\n"
	res += "  -d, --did=invalidId:\n"
	res += "    The id of databases\n"
	res += "  -t, --tid=invalidId:\n"
	res += "    The id of table\n"

	return
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
		c.ori += int(entry.OriginSize())
		c.com += int(entry.Size())
		c.cnt++
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	o := tableStatJson{
		Name:         c.name,
		ObjectCount:  c.cnt,
		Size:         formatBytes(uint32(c.com)),
		OriginalSize: formatBytes(uint32(c.ori)),
	}

	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return
	}

	c.res = string(data)

	return
}

type CheckpointArg struct {
}

func (c *CheckpointArg) PrepareCommand() *cobra.Command {
	checkpointCmd := &cobra.Command{
		Use:   "checkpoint",
		Short: "checkpoint",
		Long:  "Display information about a given checkpoint",
		Run:   RunFactory(c),
	}

	checkpointCmd.SetUsageTemplate(c.Usage())

	stat := ckpStatArg{}
	checkpointCmd.AddCommand(stat.PrepareCommand())

	list := ckpListArg{}
	checkpointCmd.AddCommand(list.PrepareCommand())

	return checkpointCmd
}

func (c *CheckpointArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *CheckpointArg) String() string {
	return "table"
}

func (c *CheckpointArg) Usage() (res string) {
	res += "Available Commands:\n"
	res += fmt.Sprintf("  %-5v show table information\n", "stat")
	res += fmt.Sprintf("  %-5v display checkpoint or table information\n", "list")

	res += "\n"
	res += "Usage:\n"
	res += "inspect table [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect table <command> --help\" for more information about a given command.\n"

	return
}

func (c *CheckpointArg) Run() error {
	return nil
}

type ckpStatArg struct {
	ctx   *inspectContext
	cid   uint64
	tid   uint64
	limit int
	all   bool
	res   string
}

func (c *ckpStatArg) PrepareCommand() *cobra.Command {
	ckpStatCmd := &cobra.Command{
		Use:   "stat",
		Short: "checkpoint stat",
		Long:  "Display information about a given checkpoint",
		Run:   RunFactory(c),
	}

	ckpStatCmd.SetUsageTemplate(c.Usage())

	ckpStatCmd.Flags().IntP("cid", "c", invalidId, "checkpoint lsn")
	ckpStatCmd.Flags().IntP("tid", "t", invalidId, "checkpoint tid")
	ckpStatCmd.Flags().IntP("limit", "l", invalidLimit, "checkpoint limit")
	ckpStatCmd.Flags().BoolP("all", "a", false, "checkpoint all tables")

	return ckpStatCmd
}

func (c *ckpStatArg) FromCommand(cmd *cobra.Command) (err error) {
	id, _ := cmd.Flags().GetInt("cid")
	c.cid = uint64(id)
	id, _ = cmd.Flags().GetInt("tid")
	c.tid = uint64(id)
	c.all, _ = cmd.Flags().GetBool("all")
	c.limit, _ = cmd.Flags().GetInt("limit")
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	}
	return nil
}

func (c *ckpStatArg) String() string {
	return c.res
}

func (c *ckpStatArg) Usage() (res string) {
	res += "Examples:\n"
	res += "  # Display all table information for the given checkpoint\n"
	res += "  inspect checkpoint stat -c ckp_lsn\n"
	res += "  # Display information for the given table\n"
	res += "  inspect checkpoint stat -c ckp_lsn -t tid\n"

	res += "\n"
	res += "Options:\n"
	res += "  -c, --cid=invalidId:\n"
	res += "    The lsn of checkpoint\n"
	res += "  -t, --tid=invalidId:\n"
	res += "    The id of table\n"
	res += "  -l, --limit=invalidLimit:\n"
	res += "    The limit length of the return value\n"
	res += "  -a, --all=false:\n"
	res += "    Show all tables\n"

	return
}

func (c *ckpStatArg) Run() (err error) {
	if c.ctx == nil {
		return moerr.NewInfoNoCtx("it is an online command")
	}
	ctx := context.Background()
	var checkpointJson *logtail.ObjectInfoJson
	entries := c.ctx.db.BGCheckpointRunner.GetAllCheckpoints()
	for _, entry := range entries {
		if entry.LSN() == c.cid {
			var data *logtail.CheckpointData
			data, err = getCkpData(ctx, entry, c.ctx.db.Runtime.Fs)
			if err != nil {
				return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get checkpoint data %v, %v", c.cid, err))
			}
			if c.all {
				c.tid = 0
			}
			if checkpointJson, err = data.GetCheckpointMetaInfo(c.tid, c.limit); err != nil {
				return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get checkpoint data %v, %v", c.cid, err))
			}
		}
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	jsonData, err := json.MarshalIndent(checkpointJson, "", "  ")
	if err != nil {
		return
	}
	c.res = string(jsonData)

	return
}

func getCkpData(ctx context.Context, entry *checkpoint.CheckpointEntry, fs *objectio.ObjectFS) (data *logtail.CheckpointData, err error) {
	if data, err = entry.PrefetchMetaIdx(ctx, fs); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to get checkpoint data %v", err))
		return
	}
	if err = entry.ReadMetaIdx(ctx, fs, data); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to get checkpoint data %v", err))
		return
	}
	if err = entry.Prefetch(ctx, fs, data); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to get checkpoint data %v", err))
		return
	}
	if err = entry.Read(ctx, fs, data); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to get checkpoint data %v", err))
		return
	}

	return
}

type CkpEntry struct {
	Index  int      `json:"index"`
	LSN    string   `json:"lsn"`
	Detail string   `json:"detail"`
	Table  []uint64 `json:"table,omitempty"`
}

type CkpEntries struct {
	Count       int        `json:"count"`
	Checkpoints []CkpEntry `json:"checkpoints"`
}

type ckpListArg struct {
	ctx   *inspectContext
	cid   uint64
	limit int
	res   string
}

func (c *ckpListArg) PrepareCommand() *cobra.Command {
	ckpStatCmd := &cobra.Command{
		Use:   "list",
		Short: "checkpoint list",
		Long:  "Display all checkpoints",
		Run:   RunFactory(c),
	}

	ckpStatCmd.SetUsageTemplate(c.Usage())
	ckpStatCmd.Flags().IntP("cid", "c", invalidId, "checkpoint id")
	ckpStatCmd.Flags().IntP("limit", "l", invalidId, "limit")

	return ckpStatCmd
}

func (c *ckpListArg) FromCommand(cmd *cobra.Command) (err error) {
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	}
	id, _ := cmd.Flags().GetInt("cid")
	c.cid = uint64(id)
	c.limit, _ = cmd.Flags().GetInt("limit")
	return nil
}

func (c *ckpListArg) String() string {
	return c.res
}

func (c *ckpListArg) Usage() (res string) {
	res += "Examples:\n"
	res += "  # Display all checkpoints in memory\n"
	res += "  inspect checkpoint list\n"
	res += "  # Display all tables for the given checkpoint\n"
	res += "  inspect checkpoint list -c ckp_lsn\n"

	res += "\n"
	res += "Options:\n"
	res += "  -c, --cid=invalidId:\n"
	res += "    The lsn of checkpoint\n"
	res += "  -l, --limit=invalidLimit:\n"
	res += "    The limit length of the return value\n"
	return
}

func (c *ckpListArg) Run() (err error) {
	if c.ctx == nil {
		return moerr.NewInfoNoCtx("it is an online command")
	}
	ctx := context.Background()
	if c.cid == invalidId {
		c.res, err = c.getCkpList()
	} else {
		c.res, err = c.getTableList(ctx)
	}

	return
}

func (c *ckpListArg) getCkpList() (res string, err error) {
	entries := c.ctx.db.BGCheckpointRunner.GetAllCheckpoints()
	ckpEntries := make([]CkpEntry, 0, len(entries))
	for i, entry := range entries {
		if i >= c.limit {
			break
		}
		ckpEntries = append(ckpEntries, CkpEntry{
			Index:  i,
			LSN:    entry.LSNString(),
			Detail: entry.String(),
		})
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	ckpEntriesJson := CkpEntries{
		Count:       len(ckpEntries),
		Checkpoints: ckpEntries,
	}
	jsonData, err := json.MarshalIndent(ckpEntriesJson, "", "  ")
	if err != nil {
		return
	}
	res = string(jsonData)

	return
}

type TableIds struct {
	TableCnt int      `json:"table_count"`
	Ids      []uint64 `json:"tables"`
}

func (c *ckpListArg) getTableList(ctx context.Context) (res string, err error) {
	entries := c.ctx.db.BGCheckpointRunner.GetAllCheckpoints()
	var ids []uint64
	for _, entry := range entries {
		if entry.LSN() != c.cid {
			continue
		}

		data, _ := getCkpData(ctx, entry, c.ctx.db.Runtime.Fs)
		ids = data.GetTableIds()
	}
	if c.limit < len(ids) {
		ids = ids[:c.limit]
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	tables := TableIds{
		TableCnt: len(ids),
		Ids:      ids,
	}
	jsonData, err := json.MarshalIndent(tables, "", "  ")
	if err != nil {
		return
	}
	res = string(jsonData)

	return
}
