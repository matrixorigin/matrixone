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

package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/backup"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/spf13/cobra"
)

const (
	invalidId    = -1
	invalidLimit = -1

	brief    = 0
	standard = 1
	detailed = 2

	sid           = "inspect"
	checkpointDir = "ckp/"
)

//func init() {
//	logutil.SetupMOLogger(&logutil.LogConfig{
//		Level:  "fatal",
//		Format: "console",
//	})
//}

func initPipeline() {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)
	blockio.Start(sid)
}

type ColumnJson struct {
	Index       uint16   `json:"col_index"`
	Type        string   `json:"col_type,omitempty"`
	Ndv         uint32   `json:"ndv,omitempty"`
	NullCnt     uint32   `json:"null_count,omitempty"`
	DataSize    string   `json:"data_size,omitempty"`
	OriDataSize string   `json:"original_data_size,omitempty"`
	Zonemap     string   `json:"zonemap,omitempty"`
	Data        []string `json:"data,omitempty"`
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

type InputJson struct {
	FileName   string `json:"file_name,omitempty"`
	Level      int    `json:"level,omitempty"`
	BlockId    int    `json:"block_id,omitempty"`
	Columns    []int  `json:"columns,omitempty"`
	Rows       []int  `json:"rows,omitempty"`
	TableId    int    `json:"table_id,omitempty"`
	DatabaseId int    `json:"db_id,omitempty"`
	Local      bool   `json:"local,omitempty"`
	All        bool   `json:"all,omitempty"`
	Download   bool   `json:"download,omitempty"`
	Target     string `json:"target,omitempty"`
	Method     string `json:"method,omitempty"`
	CkpEndTs   string `json:"ckp_end_ts,omitempty"`
	Limit      int    `json:"limit,omitempty"`
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
			return moerr.NewInfoNoCtx(fmt.Sprintf("invalid number '%s'\n", item))
		}
		*result = append(*result, num)
	}
	return nil
}

type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func formatBytes[T Integer](size T) string {
	bytes := uint64(size)
	const (
		_         = iota
		KB uint64 = 1 << (10 * iota)
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

func initFs(ctx context.Context, dir string, local bool) (fs fileservice.FileService, err error) {
	if local {
		cfg := fileservice.Config{
			Name:    defines.LocalFileServiceName,
			Backend: "DISK",
			DataDir: dir,
			Cache:   fileservice.DisabledCacheConfig,
		}
		fs, err = fileservice.NewFileService(ctx, cfg, nil)
		return
	}

	arg := fileservice.ObjectStorageArguments{
		Name:     defines.SharedFileServiceName,
		Endpoint: "DISK",
		Bucket:   dir,
	}
	return fileservice.NewS3FS(ctx, arg, fileservice.DisabledCacheConfig, nil, false, true)
}

func InitReader(fs fileservice.FileService, name string) (reader *objectio.ObjectReader, err error) {
	return objectio.NewObjectReaderWithStr(name, fs)
}

type InspectContext struct {
	Db     *db.DB
	Acinfo *db.AccessInfo
	Args   []string
	Out    io.Writer
	Resp   *db.InspectResp
}

// impl Pflag.Value interface
func (i *InspectContext) String() string   { return "" }
func (i *InspectContext) Set(string) error { return nil }
func (i *InspectContext) Type() string     { return "ictx" }

type InspectCmd interface {
	FromCommand(cmd *cobra.Command) error
	String() string
	Run() error
}

func RunFactory[T InspectCmd](t T) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := t.FromCommand(cmd); err != nil {
			cmd.OutOrStdout().Write([]byte(fmt.Sprintf("parse err: %v", err)))
			return
		}
		err := t.Run()
		if err != nil {
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("run err: %v", err)),
			)
		} else {
			cmd.OutOrStdout().Write(
				[]byte(fmt.Sprintf("%v", t.String())),
			)
		}
	}
}

func RunInspect(ctx context.Context, inspectCtx *InspectContext) {
	rootCmd := initCommand(ctx, inspectCtx)
	rootCmd.Execute()
}

func initCommand(_ context.Context, inspectCtx *InspectContext) *cobra.Command {
	rootCmd := &cobra.Command{
		Use: "tools",
	}

	rootCmd.PersistentFlags().VarPF(inspectCtx, "ictx", "", "").Hidden = true

	rootCmd.SetArgs(inspectCtx.Args)
	rootCmd.SetErr(inspectCtx.Out)
	rootCmd.SetOut(inspectCtx.Out)

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	inspectArg := &MoInspectArg{}
	rootCmd.AddCommand(inspectArg.PrepareCommand())

	return rootCmd
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
	return c.Usage()
}

func (c *MoInspectArg) Usage() (res string) {
	res += "Commands:\n"
	res += fmt.Sprintf("  %-15v object analysis tool \n", "object")
	res += fmt.Sprintf("  %-15v table analysis tool \n", "table")
	res += fmt.Sprintf("  %-15v checkpoint analysis tool \n", "checkpoint")

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
	return c.Usage()
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
	ctx    *InspectContext
	level  int
	dir    string
	name   string
	id     int
	col    int
	fs     fileservice.FileService
	reader *objectio.ObjectReader
	res    string
	input  string
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
	statCmd.Flags().StringP("input", "", "", "input file")

	return statCmd
}

func (c *moObjStatArg) FromCommand(cmd *cobra.Command) (err error) {
	c.id, _ = cmd.Flags().GetInt("id")
	c.col, _ = cmd.Flags().GetInt("col")
	c.level, _ = cmd.Flags().GetInt("level")
	c.local, _ = cmd.Flags().GetBool("local")
	path, _ := cmd.Flags().GetString("name")
	c.input, _ = cmd.Flags().GetString("input")
	c.dir, c.name = filepath.Split(path)
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*InspectContext)
	}

	return nil
}

func (c *moObjStatArg) String() string {
	return c.res + "\n"
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
	if c.input != "" {
		if err = c.getInputs(); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get inputs: %v\n", err))
		}
	}

	if c.ctx != nil {
		c.fs = c.ctx.Db.Runtime.Fs.Service
	} else {
		if c.fs, err = initFs(ctx, c.dir, c.local); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init fs: %v\n", err))
		}
	}

	if c.reader, err = InitReader(c.fs, c.name); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init reader %v\n", err))
	}

	c.res, err = c.GetStat(ctx)

	return
}

func (c *moObjStatArg) getInputs() error {
	var input InputJson
	data, err := os.ReadFile(c.input)
	if err != nil {
		return err
	}
	if err = jsoniter.Unmarshal(data, &input); err != nil {
		return err
	}
	c.dir, c.name = filepath.Split(input.FileName)
	c.id = input.BlockId
	if len(input.Columns) > 0 {
		c.col = input.Columns[0]
	}
	c.level = input.Level
	c.local = input.Local
	return nil
}

func (c *moObjStatArg) GetStat(ctx context.Context) (res string, err error) {
	var meta objectio.ObjectMeta
	if meta, err = c.reader.ReadAllMeta(ctx, common.DefaultAllocator); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to read meta, err: %v\n", err))
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
	default:
		return "", moerr.NewInfoNoCtx("invalid level, should be 0, 1, 2\n")
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
		err = moerr.NewInfoNoCtx("no data\n")
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
		err = moerr.NewInfoNoCtx("no data\n")
		return
	}

	header := data.BlockHeader()

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	colCnt := header.ColumnCount()
	if c.col != invalidId && c.col >= int(colCnt) {
		return "", moerr.NewInfoNoCtx("invalid column count\n")
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
		err = moerr.NewInfoNoCtx("no data\n")
		return
	}

	cnt := data.BlockCount()
	blocks := make([]objectio.BlockObject, 0, cnt)
	if c.id != invalidId {
		if uint32(c.id) >= cnt {
			err = moerr.NewInfoNoCtx(fmt.Sprintf("id %v out of block count %v\n", c.id, cnt))
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
			col := blk.ColumnMeta(idx)
			cols = append(cols, ColumnJson{
				Index:       idx,
				Type:        types.T(col.DataType()).String(),
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
	ctx        *InspectContext
	dir        string
	name       string
	id         int
	cols, rows []int
	col, row   string
	fs         fileservice.FileService
	reader     *objectio.ObjectReader
	res        string
	target     string
	method     string
	input      string
	local, all bool
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
	getCmd.Flags().StringP("method", "m", "", "method")
	getCmd.Flags().StringP("find", "f", "", "find")
	getCmd.Flags().BoolP("all", "a", false, "all")
	getCmd.Flags().StringP("input", "", "", "input file")

	return getCmd
}

func (c *objGetArg) FromCommand(cmd *cobra.Command) (err error) {
	c.id, _ = cmd.Flags().GetInt("id")
	c.col, _ = cmd.Flags().GetString("col")
	c.row, _ = cmd.Flags().GetString("row")
	c.local, _ = cmd.Flags().GetBool("local")
	c.target, _ = cmd.Flags().GetString("find")
	c.method, _ = cmd.Flags().GetString("method")
	c.all, _ = cmd.Flags().GetBool("all")
	c.input, _ = cmd.Flags().GetString("input")
	path, _ := cmd.Flags().GetString("name")
	c.dir, c.name = filepath.Split(path)
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*InspectContext)
	}

	return nil
}

func (c *objGetArg) String() string {
	return c.res + "\n"
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

func (c *objGetArg) getInputs() error {
	var input InputJson
	data, err := os.ReadFile(c.input)
	if err != nil {
		return err
	}
	if err = jsoniter.Unmarshal(data, &input); err != nil {
		return err
	}
	c.dir, c.name = filepath.Split(input.FileName)
	c.id = input.BlockId
	c.cols = input.Columns
	c.rows = input.Rows
	c.local = input.Local
	c.all = input.All
	c.target = input.Target
	c.method = input.Method

	return nil
}

func (c *objGetArg) Run() (err error) {
	ctx := context.Background()
	if c.input != "" {
		if err = c.getInputs(); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get inputs: %v\n", err))
		}
	} else {
		if err = c.checkInputs(); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("invalid inputs: %v\n", err))
		}
	}

	if c.ctx != nil {
		c.fs = c.ctx.Db.Runtime.Fs.Service
	} else {
		if c.fs, err = initFs(ctx, c.dir, c.local); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init fs: %v\n", err))
		}
	}

	if c.reader, err = InitReader(c.fs, c.name); err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init reader %v\n", err))
	}

	c.res, err = c.GetData(ctx)

	return
}

func (c *objGetArg) checkInputs() error {
	if err := getInputs(c.col, &c.cols); err != nil {
		return err
	}
	if err := getInputs(c.row, &c.rows); err != nil {
		return err
	}
	if len(c.rows) > 2 || (len(c.rows) == 2 && c.rows[0] >= c.rows[1]) {
		return moerr.NewInfoNoCtx("invalid rows, need two inputs [leftm, right)\n")
	}
	if c.name == "" {
		return moerr.NewInfoNoCtx("empty name\n")
	}

	return nil
}

func (c *objGetArg) GetData(ctx context.Context) (res string, err error) {
	var meta objectio.ObjectMeta
	ctx1, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if meta, err = c.reader.ReadAllMeta(ctx1, common.DefaultAllocator); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to read meta, err: %v\n", err))
		return
	}

	cnt := meta.DataMetaCount()
	if c.id == invalidId || uint16(c.id) >= cnt {
		err = moerr.NewInfoNoCtx("invalid id\n")
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
			err = moerr.NewInfoNoCtx(fmt.Sprintf("column %v out of colum count %v\n", idx, cnt))
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
	v, _ := c.reader.ReadOneBlock(ctx2, idxs, typs, uint16(c.id), common.DefaultAllocator)
	defer v.Release()
	cols := make([]ColumnJson, 0, len(v.Entries))
	for i, entry := range v.Entries {
		obj, _ := objectio.Decode(entry.CachedData.Bytes())
		vec := obj.(*vector.Vector)
		if ret, ok := executeVecMethod(vec, c.method); ok {
			col := ColumnJson{
				Index: uint16(c.cols[i]),
				Type:  vec.GetType().String(),
				Data:  ret,
			}
			cols = append(cols, col)
			continue
		}

		ret := c.getPrintableVec(vec)
		left, right := 0, 0
		if len(c.rows) != 0 {
			left = c.rows[0]
			right = c.rows[0]
			if len(c.rows) > 1 {
				right = c.rows[1]
			}
			ret = ret[left:right]
		} else if !c.all {
			right = min(len(ret), 10)
			ret = ret[left:right]
		}
		col := ColumnJson{
			Index: uint16(c.cols[i]),
			Type:  vec.GetType().String(),
			Data:  ret,
		}
		cols = append(cols, col)
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	o := BlockJson{
		Index:   blk.GetID(),
		Rows:    blk.GetRows(),
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

func (c *objGetArg) getPrintableVec(v *vector.Vector) []string {
	oid := v.GetType().Oid
	switch oid {
	case types.T_bool:
		return getDataFromVec[bool](v, oid, c.method, c.target)
	case types.T_bit:
		return getDataFromVec[uint64](v, oid, c.method, c.target)
	case types.T_int8:
		return getDataFromVec[int8](v, oid, c.method, c.target)
	case types.T_int16:
		return getDataFromVec[int16](v, oid, c.method, c.target)
	case types.T_int32:
		return getDataFromVec[int32](v, oid, c.method, c.target)
	case types.T_int64:
		return getDataFromVec[int64](v, oid, c.method, c.target)
	case types.T_uint8:
		return getDataFromVec[uint8](v, oid, c.method, c.target)
	case types.T_uint16:
		return getDataFromVec[uint16](v, oid, c.method, c.target)
	case types.T_uint32:
		return getDataFromVec[uint32](v, oid, c.method, c.target)
	case types.T_uint64:
		return getDataFromVec[uint64](v, oid, c.method, c.target)
	case types.T_float32:
		return getDataFromVec[float32](v, oid, c.method, c.target)
	case types.T_float64:
		return getDataFromVec[float64](v, oid, c.method, c.target)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		area := v.GetArea()
		vec := vector.MustFixedCol[types.Varlena](v)
		data := make([][]byte, len(vec))
		for i := range vec {
			data[i] = vec[i].GetByteSlice(area)
		}
		return getVarcharFromVec(data, oid, c.method, c.target)
	case types.T_date:
		return getDataFromVec[types.Date](v, oid, c.method, c.target)
	case types.T_datetime:
		return getDataFromVec[types.Datetime](v, oid, c.method, c.target)
	case types.T_time:
		return getDataFromVec[types.Time](v, oid, c.method, c.target)
	case types.T_timestamp:
		return getDataFromVec[types.Timestamp](v, oid, c.method, c.target)
	case types.T_enum:
		return getDataFromVec[types.Enum](v, oid, c.method, c.target)
	case types.T_uuid:
		return getDataFromVec[types.Uuid](v, oid, c.method, c.target)
	case types.T_TS:
		return getDataFromVec[types.TS](v, oid, c.method, c.target)
	case types.T_Rowid:
		return getDataFromVec[types.Rowid](v, oid, c.method, c.target)
	case types.T_Blockid:
		return getDataFromVec[types.Blockid](v, oid, c.method, c.target)
	default:
		return []string{v.String()}
	}
}

func executeVecMethod(v *vector.Vector, method string) ([]string, bool) {
	oid := v.GetType().Oid
	switch method {
	case "sum":
		if !checkSumType(oid) {
			return []string{fmt.Sprintf("method %v dose not support type %v", method, oid.String())}, true
		}
		_, val := v.GetSumValue()
		return decodeVec(oid, val), true
	case "min":
		if !checkMinMaxType(oid) {
			return []string{fmt.Sprintf("method %v dose not support type %v", method, oid.String())}, true
		}
		_, val, _ := v.GetMinMaxValue()
		return decodeVec(oid, val), true
	case "max":
		if !checkMinMaxType(oid) {
			return []string{fmt.Sprintf("method %v dose not support type %v", method, oid.String())}, true
		}
		_, _, val := v.GetMinMaxValue()
		return decodeVec(oid, val), true
	default:
		return nil, false
	}
}

func checkSumType(oid types.T) bool {
	switch oid {
	case types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8,
		types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64, types.T_decimal64:
		return true
	default:
		return false
	}
}

func checkMinMaxType(oid types.T) bool {
	switch oid {
	case types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8,
		types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64, types.T_date,
		types.T_datetime, types.T_time, types.T_timestamp, types.T_enum, types.T_decimal64, types.T_decimal128,
		types.T_TS, types.T_uuid, types.T_Rowid, types.T_char, types.T_varchar, types.T_json, types.T_binary,
		types.T_varbinary, types.T_blob, types.T_text, types.T_datalink, types.T_array_float32, types.T_array_float64:
		return true
	default:
		return false
	}
}

func executeMethod(v []string, oid types.T, method string) []string {
	switch method {
	case "":
		return v
	case "location":
		if oid != types.T_varchar {
			return []string{fmt.Sprintf("method %v dose not support type %v", method, oid.String())}
		}
		res := make([]string, len(v))
		for i := range v {
			res[i] = objectio.Location(v[i]).String()
		}
		return res
	case "rowid":
		if oid != types.T_varchar {
			return []string{fmt.Sprintf("method %v dose not support type %v", method, oid.String())}
		}
		res := make([]string, len(v))
		for i := range v {
			rowid := types.Rowid([]byte(v[i]))
			res[i] = rowid.String()
		}
		return res
	case "blkid":
		if oid != types.T_varchar {
			return []string{fmt.Sprintf("method %v dose not support type %v", method, oid.String())}
		}
		res := make([]string, len(v))
		for i := range v {
			rowid := types.Blockid([]byte(v[i]))
			res[i] = rowid.String()
		}
		return res
	default:
		return []string{fmt.Sprintf("unsupport method %s", method)}
	}
}

func getDataFromVec[T any](v *vector.Vector, oid types.T, method, target string) []string {
	vec := vector.MustFixedCol[T](v)
	str := make([]string, 0, len(vec))
	res := make([]string, 0, len(vec))
	for _, val := range vec {
		str = append(str, getString(oid, val))
	}
	data := executeMethod(str, oid, method)
	for i, val := range data {
		if target == "" || target == val {
			res = append(res, fmt.Sprintf("idx: %-4v, val: %v", i, val))
		}
	}

	return res
}

func getVarcharFromVec(vec [][]byte, oid types.T, method, target string) []string {
	str := make([]string, 0, len(vec))
	res := make([]string, 0, len(vec))
	for _, val := range vec {
		str = append(str, getString(oid, val))
	}
	data := executeMethod(str, oid, method)
	for i, val := range data {
		if target == "" || target == val {
			res = append(res, fmt.Sprintf("idx: %-4v, val: %v", i, val))
		}
	}

	return res
}

func getString(oid types.T, v any) string {
	switch oid {
	case types.T_bool:
		val := v.(bool)
		if val {
			return "true"
		} else {
			return "false"
		}
	case types.T_bit:
		return strconv.FormatUint(v.(uint64), 10)
	case types.T_int8:
		return strconv.FormatInt(int64(v.(int8)), 10)
	case types.T_int16:
		return strconv.FormatInt(int64(v.(int16)), 10)
	case types.T_int32:
		return strconv.FormatInt(int64(v.(int32)), 10)
	case types.T_int64:
		return strconv.FormatInt(v.(int64), 10)
	case types.T_uint8:
		return strconv.FormatUint(uint64(v.(uint8)), 10)
	case types.T_uint16:
		return strconv.FormatUint(uint64(v.(uint16)), 10)
	case types.T_uint32:
		return strconv.FormatUint(uint64(v.(uint32)), 10)
	case types.T_uint64:
		return strconv.FormatUint(v.(uint64), 10)
	case types.T_float32:
		return strconv.FormatFloat(float64(v.(float32)), 'f', -1, 32)
	case types.T_float64:
		return strconv.FormatFloat(v.(float64), 'f', -1, 64)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		return string(v.([]byte))
	case types.T_date:
		val := v.(types.Date)
		return val.String()
	case types.T_datetime:
		val := v.(types.Datetime)
		return val.String()
	case types.T_time:
		val := v.(types.Time)
		return val.String()
	case types.T_timestamp:
		val := v.(types.Timestamp)
		return val.String()
	case types.T_enum:
		val := v.(types.Enum)
		return val.String()
	case types.T_uuid:
		val := v.(types.Uuid)
		return val.String()
	case types.T_TS:
		val := v.(types.TS)
		return val.ToString()
	case types.T_Rowid:
		val := v.(types.Rowid)
		return val.String()
	case types.T_Blockid:
		val := v.(types.Blockid)
		return val.String()
	default:
		return ""
	}
}

func decodeVec(oid types.T, v []byte) []string {
	switch oid {
	case types.T_bit:
		val := types.DecodeUint64(v)
		return []string{getString(oid, val)}
	case types.T_int8:
		val := types.DecodeInt8(v)
		return []string{getString(oid, val)}
	case types.T_int16:
		val := types.DecodeInt16(v)
		return []string{getString(oid, val)}
	case types.T_int32:
		val := types.DecodeInt32(v)
		return []string{getString(oid, val)}
	case types.T_int64:
		val := types.DecodeInt64(v)
		return []string{getString(oid, val)}
	case types.T_uint8:
		val := types.DecodeUint8(v)
		return []string{getString(oid, val)}
	case types.T_uint16:
		val := types.DecodeUint16(v)
		return []string{getString(oid, val)}
	case types.T_uint32:
		val := types.DecodeUint32(v)
		return []string{getString(oid, val)}
	case types.T_uint64:
		val := types.DecodeUint64(v)
		return []string{getString(oid, val)}
	case types.T_float32:
		val := types.DecodeFloat32(v)
		return []string{getString(oid, val)}
	case types.T_float64:
		val := types.DecodeFloat64(v)
		return []string{getString(oid, val)}
	case types.T_date:
		val := types.DecodeDate(v)
		return []string{getString(oid, val)}
	case types.T_datetime:
		val := types.DecodeDatetime(v)
		return []string{getString(oid, val)}
	case types.T_time:
		val := types.DecodeTime(v)
		return []string{getString(oid, val)}
	case types.T_timestamp:
		val := types.DecodeTimestamp(v)
		return []string{getString(oid, val)}
	case types.T_enum:
		val := types.DecodeEnum(v)
		return []string{getString(oid, val)}
	case types.T_uuid:
		val := types.DecodeUuid(v)
		return []string{getString(oid, val)}
	case types.T_TS:
		val := *(*types.TS)(unsafe.Pointer(&v[0]))
		return []string{getString(oid, val)}
	default:
		return []string{fmt.Sprintf("unsupported type %s", oid.String())}
	}
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
	return c.Usage()
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
	ctx                     *InspectContext
	did, tid, ori, com, cnt int
	name, res, input        string
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
		c.ctx = cmd.Flag("ictx").Value.(*InspectContext)
	}
	return nil
}

func (c *tableStatArg) String() string {
	return c.res + "\n"
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
	if c.input != "" {
		if err = c.getInputs(); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get inputs: %v\n", err))
		}
	}
	if c.ctx == nil {
		return moerr.NewInfoNoCtx("it is an online command\n")
	}
	db, err := c.ctx.Db.Catalog.GetDatabaseByID(uint64(c.did))
	if err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get db %v\n", c.did))
	}
	table, err := db.GetTableEntryByID(uint64(c.tid))
	if err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get table %v\n", c.tid))
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

func (c *tableStatArg) getInputs() error {
	var input InputJson
	data, err := os.ReadFile(c.input)
	if err != nil {
		return err
	}
	if err = jsoniter.Unmarshal(data, &input); err != nil {
		return err
	}
	c.did = input.DatabaseId
	c.tid = input.TableId

	return nil
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
	return c.Usage()
}

func (c *CheckpointArg) Usage() (res string) {
	res += "Available Commands:\n"
	res += fmt.Sprintf("  %-5v display checkpoint meta information\n", "stat")
	res += fmt.Sprintf("  %-5v display checkpoint or table information\n", "list")

	res += "\n"
	res += "Usage:\n"
	res += "inspect checkpoint [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect checkpoint <command> --help\" for more information about a given command.\n"

	return
}

func (c *CheckpointArg) Run() error {
	return nil
}

type ckpStatArg struct {
	ctx   *InspectContext
	cid   string
	tid   uint64
	limit int
	all   bool

	path, name, res, input string
}

func (c *ckpStatArg) PrepareCommand() *cobra.Command {
	ckpStatCmd := &cobra.Command{
		Use:   "stat",
		Short: "checkpoint stat",
		Long:  "Display checkpoint meta information",
		Run:   RunFactory(c),
	}

	ckpStatCmd.SetUsageTemplate(c.Usage())

	ckpStatCmd.Flags().StringP("cid", "c", "", "checkpoint end ts")
	ckpStatCmd.Flags().IntP("tid", "t", invalidId, "checkpoint tid")
	ckpStatCmd.Flags().IntP("limit", "l", invalidLimit, "checkpoint limit")
	ckpStatCmd.Flags().BoolP("all", "a", false, "checkpoint all tables")
	ckpStatCmd.Flags().StringP("name", "n", "", "checkpoint name")
	ckpStatCmd.Flags().StringP("input", "", "", "checkpoint input")

	return ckpStatCmd
}

func (c *ckpStatArg) FromCommand(cmd *cobra.Command) (err error) {
	c.cid, _ = cmd.Flags().GetString("cid")
	id, _ := cmd.Flags().GetInt("tid")
	c.tid = uint64(id)
	c.all, _ = cmd.Flags().GetBool("all")
	c.limit, _ = cmd.Flags().GetInt("limit")
	c.input, _ = cmd.Flags().GetString("input")
	dir, _ := cmd.Flags().GetString("name")
	c.path, c.name = filepath.Split(dir)
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*InspectContext)
	}
	return nil
}

func (c *ckpStatArg) String() string {
	return c.res + "\n"
}

func (c *ckpStatArg) Usage() (res string) {
	res += "Examples:\n"
	res += "  # Display meta information for the latest checkpoints\n"
	res += "  inspect checkpoint stat\n"
	res += "  # Display information for the given checkpoint\n"
	res += "  inspect checkpoint stat -c ckp_end_ts\n"
	res += "  # [Offline] display latest checkpoints in the meta file\n"
	res += "  inspect checkpoint stat -n /your/path/meta_file\n"

	res += "\n"
	res += "Options:\n"
	res += "  -c, --cid=invalidId:\n"
	res += "    The end ts of checkpoint\n"
	res += "  -t, --tid=invalidId:\n"
	res += "    The id of table\n"
	res += "  -l, --limit=invalidLimit:\n"
	res += "    The limit length of the tables\n"
	res += "  -n, --name=\"\":\n"
	res += "    If you want to use this command offline, specify the file to be analyzed by this flag\n"
	res += "  -a, --all=false:\n"
	res += "    Show all tables\n"

	return
}

func (c *ckpStatArg) getInputs() error {
	var input InputJson
	data, err := os.ReadFile(c.input)
	if err != nil {
		return err
	}
	if err = jsoniter.Unmarshal(data, &input); err != nil {
		return err
	}

	c.path, c.name = filepath.Split(input.FileName)
	c.cid = input.CkpEndTs
	c.tid = uint64(input.TableId)
	c.limit = input.Limit
	c.all = input.All

	return nil
}

func (c *ckpStatArg) Run() (err error) {
	if c.ctx == nil {
		initPipeline()
	}
	if c.input != "" {
		if err = c.getInputs(); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get inputs, err %v\n", err))
		}
	}
	ctx := context.Background()
	var fs fileservice.FileService
	if c.ctx == nil {
		fs, err = initFs(ctx, c.path, true)
		if err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to init fs %v, %v\n", c.cid, err))
		}
	} else {
		fs = c.ctx.Db.Runtime.Fs.Service
	}
	checkpointJson := logtail.ObjectInfoJson{}
	entries, err := getCkpEntries(ctx, c.ctx, c.path, c.name, false)
	if err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get ckp entries %s\n", err))
	}
	tables := make(map[uint64]*logtail.TableInfoJson)
	locations := make([]objectio.Location, 0, len(entries))
	versions := make([]uint32, 0, len(entries))
	for _, entry := range entries {
		if c.cid == "" || entry.GetEnd().ToString() == c.cid {
			var data *logtail.CheckpointData
			data, err = getCkpData(ctx, entry, &objectio.ObjectFS{
				Service: fs,
				Dir:     c.path,
			})
			if err != nil {
				return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get checkpoint data %v, %v\n", c.cid, err))
			}
			var res *logtail.ObjectInfoJson
			if res, err = data.GetCheckpointMetaInfo(); err != nil {
				return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get checkpoint data %v, %v\n", c.cid, err))
			}

			for i, val := range res.Tables {
				table, ok := tables[val.ID]
				if ok {
					table.Add += val.Add
					table.Delete += val.Delete
					table.TombstoneRows += val.TombstoneRows
					table.TombstoneCount += val.TombstoneCount
				} else {
					tables[val.ID] = &res.Tables[i]
				}
			}
			checkpointJson.ObjectCnt += res.ObjectCnt
			checkpointJson.ObjectAddCnt += res.ObjectAddCnt
			checkpointJson.ObjectDelCnt += res.ObjectDelCnt
			checkpointJson.TombstoneCnt += res.TombstoneCnt

			locations = append(locations, entry.GetLocation())
			versions = append(versions, entry.GetVersion())
		}
	}

	tableins := make(map[uint64]uint64)
	tabledel := make(map[uint64]uint64)
	ins, del, err := logtail.GetStorageUsageHistory(
		ctx, sid, locations, versions,
		fs, common.CheckpointAllocator,
	)
	if err != nil {
		return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get storage usage %v\n", err))
	}
	for _, datas := range ins {
		for _, data := range datas {
			tableins[data.TblId] += data.Size
		}
	}
	for _, datas := range del {
		for _, data := range datas {
			tabledel[data.TblId] += data.Size
		}
	}

	for i := range tables {
		if size, ok := tableins[tables[i].ID]; ok {
			tables[i].InsertSize = formatBytes(size)
		}
		if size, ok := tabledel[tables[i].ID]; ok {
			tables[i].DeleteSize = formatBytes(size)
		}
		if c.all || c.limit != invalidLimit || c.tid == tables[i].ID {
			checkpointJson.Tables = append(checkpointJson.Tables, *tables[i])
		}
	}
	checkpointJson.TableCnt = len(checkpointJson.Tables)

	sort.Slice(checkpointJson.Tables, func(i, j int) bool {
		return checkpointJson.Tables[i].Add > checkpointJson.Tables[j].Add
	})

	if c.limit < checkpointJson.TableCnt {
		checkpointJson.Tables = checkpointJson.Tables[:c.limit]
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	jsonData, err := json.MarshalIndent(checkpointJson, "", "  ")
	if err != nil {
		return
	}
	c.res = string(jsonData)

	return
}

func readCkpFromDisk(ctx context.Context, path, name string) (res []*checkpoint.CheckpointEntry, err error) {
	fs, err := initFs(ctx, path, true)
	if err != nil {
		return nil, err
	}
	res, err = checkpoint.ReplayCheckpointEntry(ctx, sid, name, fs)
	if err != nil {
		return nil, err
	}
	sort.Slice(res, func(i, j int) bool {
		end1 := res[i].GetEnd()
		end2 := res[j].GetEnd()
		return end1.LessEq(&end2)
	})
	var idx int
	for i := range res {
		if !res[i].IsIncremental() {
			idx = i
			break
		}
	}
	res = res[idx:]

	return
}

func getCkpEntries(ctx context.Context, context *InspectContext, path, name string, all bool,
) (entries []*checkpoint.CheckpointEntry, err error) {
	if context == nil {
		entries, err = readCkpFromDisk(ctx, path, name)
		for _, entry := range entries {
			entry.SetSid(sid)
		}
	} else {
		if all {
			entries = context.Db.BGCheckpointRunner.GetAllGlobalCheckpoints()
			entries = append(entries, context.Db.BGCheckpointRunner.GetAllIncrementalCheckpoints()...)
		} else {
			entries = context.Db.BGCheckpointRunner.GetAllCheckpoints()
		}
	}

	return
}

func getCkpData(
	ctx context.Context,
	entry *checkpoint.CheckpointEntry,
	fs *objectio.ObjectFS,
) (data *logtail.CheckpointData, err error) {
	if data, err = entry.PrefetchMetaIdx(ctx, fs); err != nil {
		return nil, err
	}
	if err = entry.ReadMetaIdx(ctx, fs, data); err != nil {
		return nil, err
	}
	if err = entry.Prefetch(ctx, fs, data); err != nil {
		return nil, err
	}
	if err = entry.Read(ctx, fs, data); err != nil {
		return nil, err
	}

	return
}

type CkpEntry struct {
	Index int    `json:"index"`
	LSN   string `json:"lsn"`
	Type  string `json:"type"`
	State int    `json:"state"`
	Start string `json:"start"`
	End   string `json:"end"`
}

type CkpEntries struct {
	Count       int        `json:"count"`
	Checkpoints []CkpEntry `json:"checkpoints"`
}

type ckpListArg struct {
	ctx   *InspectContext
	cid   string
	limit int
	input string

	all, download   bool
	res, path, name string
}

func (c *ckpListArg) PrepareCommand() *cobra.Command {
	ckpListCmd := &cobra.Command{
		Use:   "list",
		Short: "checkpoint list",
		Long:  "Display all checkpoints",
		Run:   RunFactory(c),
	}

	ckpListCmd.SetUsageTemplate(c.Usage())
	ckpListCmd.Flags().StringP("cid", "c", "", "checkpoint id")
	ckpListCmd.Flags().IntP("limit", "l", invalidLimit, "limit")
	ckpListCmd.Flags().BoolP("all", "a", false, "all")
	ckpListCmd.Flags().StringP("name", "n", "", "name")
	ckpListCmd.Flags().BoolP("download", "d", false, "download")
	ckpListCmd.Flags().StringP("input", "", "", "input")

	return ckpListCmd
}

func (c *ckpListArg) FromCommand(cmd *cobra.Command) (err error) {
	if cmd.Flag("ictx") != nil {
		c.ctx = cmd.Flag("ictx").Value.(*InspectContext)
	}
	c.cid, _ = cmd.Flags().GetString("cid")
	c.limit, _ = cmd.Flags().GetInt("limit")
	c.all, _ = cmd.Flags().GetBool("all")
	c.download, _ = cmd.Flags().GetBool("download")
	c.input, _ = cmd.Flags().GetString("input")
	dir, _ := cmd.Flags().GetString("name")
	c.path, c.name = filepath.Split(dir)
	return nil
}

func (c *ckpListArg) String() string {
	return c.res + "\n"
}

func (c *ckpListArg) Usage() (res string) {
	res += "Examples:\n"
	res += "  # Display latest checkpoints in memory\n"
	res += "  inspect checkpoint list\n"
	res += "  # Display all tables for the given checkpoint\n"
	res += "  inspect checkpoint list -c ckp_end_ts\n"
	res += "  # Download latest checkpoints, the dir is mo-data/local/ckp\n"
	res += "  inspect checkpoint list -d\n"
	res += "  # [Offline] display all checkpoints in the meta file\n"
	res += "  inspect checkpoint list -n /your/path/meta_file\n"

	res += "\n"
	res += "Options:\n"
	res += "  -c, --cid=invalidId:\n"
	res += "    Display all table IDs of the ckp specified by end ts\n"
	res += "  -l, --limit=invalidLimit:\n"
	res += "    The limit length of the return checkpoints\n"
	res += "  -n, --name=\"\":\n"
	res += "    If you want to use this command offline, specify the file to be analyzed by this flag\n"
	res += "  -a, --all=false:\n"
	res += "    Display all checkpoints \n"
	res += "  -d, --download=false:\n"
	res += "    Download latest checkpoints, the dir is mo-data/local/ckp \n"
	return
}

func (c *ckpListArg) Run() (err error) {
	if c.ctx == nil {
		initPipeline()
	}
	if c.input != "" {
		if err = c.getInputs(); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get inputs, err %v\n", err))
		}
	}
	ctx := context.Background()
	if c.download {
		if c.ctx == nil {
			return moerr.NewInfoNoCtx("can not download checkpoints offline\n")
		}
		cnt, err := c.DownLoadEntries(ctx)
		if err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to download entries, err %v\n", err))
		}
		c.res = fmt.Sprintf("downloaded %d entries\n", cnt)
		return nil
	}

	if c.cid == "" {
		c.res, err = c.getCkpList()
	} else {
		c.res, err = c.getTableList(ctx)
	}

	return
}

func (c *ckpListArg) getInputs() error {
	var input InputJson
	data, err := os.ReadFile(c.input)
	if err != nil {
		return err
	}
	if err = jsoniter.Unmarshal(data, &input); err != nil {
		return err
	}

	c.path, c.name = filepath.Split(input.FileName)
	c.cid = input.CkpEndTs
	c.limit = input.Limit
	c.all = input.All
	c.download = input.Download

	return nil
}

func (c *ckpListArg) getCkpList() (res string, err error) {
	ctx := context.Background()
	entries, err := getCkpEntries(ctx, c.ctx, c.path, c.name, c.all)
	if err != nil {
		return "", err
	}

	ckpEntries := make([]CkpEntry, 0, len(entries))
	for i, entry := range entries {
		if i >= c.limit {
			break
		}
		var global string
		if entry.IsIncremental() {
			global = "Incremental"
		} else {
			global = "Global"
		}
		ckpEntries = append(ckpEntries, CkpEntry{
			Index: i,
			LSN:   entry.LSNString(),
			Type:  global,
			State: int(entry.GetState()),
			Start: entry.GetStart().ToString(),
			End:   entry.GetEnd().ToString(),
		})
	}

	sort.Slice(ckpEntries, func(i, j int) bool {
		return ckpEntries[i].End < ckpEntries[j].End
	})

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
	entries, err := getCkpEntries(ctx, c.ctx, c.path, c.name, true)
	if err != nil {
		return "", err
	}
	var fs fileservice.FileService
	if c.ctx == nil {
		if fs, err = initFs(ctx, c.path, true); err != nil {
			return "", moerr.NewInfoNoCtx(fmt.Sprintf("failed to init fileservice, err %v\n", err))
		}
	} else {
		fs = c.ctx.Db.Runtime.Fs.Service
	}
	var ids []uint64
	for _, entry := range entries {
		if entry.GetEnd().ToString() != c.cid {
			continue
		}
		data, _ := getCkpData(ctx, entry, &objectio.ObjectFS{
			Service: fs,
			Dir:     c.path,
		})
		ids = data.GetTableIds()
	}
	cnt := len(ids)
	if c.limit < cnt {
		ids = ids[:c.limit]
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	tables := TableIds{
		TableCnt: cnt,
		Ids:      ids,
	}
	jsonData, err := json.MarshalIndent(tables, "", "  ")
	if err != nil {
		return
	}
	res = string(jsonData)

	return
}

func (c *ckpListArg) DownLoadEntries(ctx context.Context) (cnt int, err error) {
	entries := c.ctx.Db.BGCheckpointRunner.GetAllCheckpoints()
	entry := entries[len(entries)-1]
	metaDir := "meta_0-0_" + entry.GetEnd().ToString()
	metaName := "meta_" + entry.GetStart().ToString() + "_" + entry.GetEnd().ToString() + ".ckp"

	down := func(name, newName, dir string) error {
		cnt++
		return backup.DownloadFile(
			ctx,
			c.ctx.Db.Runtime.Fs.Service,
			c.ctx.Db.Runtime.LocalFs.Service,
			name,
			newName,
			dir,
			checkpointDir+metaDir,
		)
	}

	if err = down(metaName, "", checkpointDir); err != nil {
		return 0, err
	}

	for _, entry := range entries {
		if err = down(entry.GetTNLocation().Name().String(), "", ""); err != nil {
			return 0, err
		}
		if entry.GetLocation().Name().String() != entry.GetTNLocation().Name().String() {
			if err = down(entry.GetLocation().Name().String(), "", ""); err != nil {
				return 0, err
			}
		}
		data, err := getCkpData(context.Background(), entry, c.ctx.Db.Runtime.Fs)
		if err != nil {
			return 0, err
		}
		locs := data.GetLocations()
		for _, loc := range locs {
			if err = down(loc.Name().String(), "", ""); err != nil {
				return 0, err
			}
		}
	}

	return
}
