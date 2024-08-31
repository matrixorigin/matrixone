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

package commands

import (
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"unsafe"
)

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

	stat := objStatArg{}
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

type objStatArg struct {
	ctx    *InspectContext
	level  int
	dir    string
	name   string
	id     int
	col    string
	cols   []int
	fs     fileservice.FileService
	reader *objectio.ObjectReader
	res    string
	input  string
	local  bool
}

func (c *objStatArg) PrepareCommand() *cobra.Command {
	var statCmd = &cobra.Command{
		Use:   "stat",
		Short: "object stat",
		Long:  "Display status about a given object",
		Run:   RunFactory(c),
	}

	statCmd.SetUsageTemplate(c.Usage())

	statCmd.Flags().IntP("id", "i", invalidId, "block id")
	statCmd.Flags().StringP("col", "c", "", "column id")
	statCmd.Flags().IntP("level", "l", brief, "level")
	statCmd.Flags().StringP("name", "n", "", "name")
	statCmd.Flags().BoolP("local", "", false, "local")
	statCmd.Flags().StringP("input", "", "", "input file")

	return statCmd
}

func (c *objStatArg) FromCommand(cmd *cobra.Command) (err error) {
	c.id, _ = cmd.Flags().GetInt("id")
	c.col, _ = cmd.Flags().GetString("col")
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

func (c *objStatArg) String() string {
	return c.res + "\n"
}

func (c *objStatArg) Usage() (res string) {
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
	res += "  # Display information about object with input file\n"
	res += "  inspect object stat --input /your/path/input.json\n"

	res += "\n"
	res += "Options:\n"
	res += "  -n, --name='':\n"
	res += "    The path to the object file\n"
	res += "  -i, --idx=invalidId:\n"
	res += "    The sequence number of the block in the object\n"
	res += "  -c, --col=\"\":\n"
	res += "    The sequence number of the column in the object\n"
	res += "  -l, --level=0:\n"
	res += "    The level of detail of the information, should be 0(brief), 1(standard), 2(detailed)\n"
	res += "  --local=false:\n"
	res += "    If the file is downloaded from a standalone machine, you should use this flag\n"

	res += "\n"
	res += "Input Json Template:\n"
	res += "{\n"
	res += "  \"file_name\": \"/path/to/your/file\",\n"
	res += "  \"block_id\": -1,\n"
	res += "  \"level\": 0,\n"
	res += "  \"columns\": [],\n"
	res += "  \"local\": false,\n"
	res += "}\n"

	return
}

func (c *objStatArg) Run() (err error) {
	if c.ctx == nil {
		offlineInit()
	}
	ctx := context.Background()
	if c.input != "" {
		if err = c.getInputs(); err != nil {
			return moerr.NewInfoNoCtx(fmt.Sprintf("failed to get inputs: %v\n", err))
		}
	} else {
		if err = getInputs(c.col, &c.cols); err != nil {
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

	c.res, err = c.GetStat(ctx)

	return
}

func (c *objStatArg) getInputs() error {
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
	c.level = input.Level
	c.local = input.Local
	return nil
}

func (c *objStatArg) GetStat(ctx context.Context) (res string, err error) {
	var meta objectio.ObjectMeta
	if meta, err = c.reader.ReadAllMeta(ctx, common.DefaultAllocator); err != nil {
		err = moerr.NewInfoNoCtx(fmt.Sprintf("failed to read meta, err: %v\n", err))
		return
	}

	if c.level < standard && len(c.cols) != 0 {
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

func (c *objStatArg) GetObjSize(data *objectio.ObjectDataMeta) []string {
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

func (c *objStatArg) GetBriefStat(obj *objectio.ObjectMeta) (res string, err error) {
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

func (c *objStatArg) GetStandardStat(obj *objectio.ObjectMeta) (res string, err error) {
	data, ok := (*obj).DataMeta()
	if !ok {
		err = moerr.NewInfoNoCtx("no data\n")
		return
	}

	header := data.BlockHeader()

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	colCnt := header.ColumnCount()
	if len(c.cols) == 0 {
		c.cols = make([]int, colCnt)
		for i := range colCnt {
			c.cols[i] = int(i)
		}
	}
	for _, col := range c.cols {
		if col >= int(colCnt) {
			err = moerr.NewInfoNoCtx(fmt.Sprintf("column %v out of column count %v\n", col, colCnt))
			return
		}
	}
	cols := make([]ColumnJson, 0, colCnt)
	for _, idx := range c.cols {
		col := data.MustGetColumn(uint16(idx))
		cols = append(cols, ColumnJson{
			Index:       uint16(idx),
			DataSize:    formatBytes(col.Location().Length()),
			OriDataSize: formatBytes(col.Location().OriginSize()),
			Zonemap:     col.ZoneMap().String(),
		})
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

func (c *objStatArg) GetDetailedStat(obj *objectio.ObjectMeta) (res string, err error) {
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

	header := data.BlockHeader()
	colCnt := header.ColumnCount()
	if len(c.cols) == 0 {
		c.cols = make([]int, colCnt)
		for i := range colCnt {
			c.cols[i] = int(i)
		}
	}

	blks := make([]BlockJson, 0, len(blocks))
	for _, blk := range blocks {
		colCnt := blk.GetColumnCount()
		var dataSize, oriDataSize uint32
		cols := make([]ColumnJson, 0, colCnt)
		for _, i := range c.cols {
			if i >= int(colCnt) {
				err = moerr.NewInfoNoCtx(fmt.Sprintf("column %v out of column count %v\n", i, colCnt))
				return
			}
			col := blk.ColumnMeta(uint16(i))
			dataSize += col.Location().Length()
			oriDataSize += col.Location().OriginSize()

			cols = append(cols, ColumnJson{
				Index:       uint16(i),
				Type:        types.T(col.DataType()).String(),
				DataSize:    formatBytes(col.Location().Length()),
				OriDataSize: formatBytes(col.Location().OriginSize()),
				Zonemap:     col.ZoneMap().String(),
			})
		}
		blkjson := BlockJson{
			Index:       blk.GetID(),
			Rows:        blk.GetRows(),
			Cols:        blk.GetColumnCount(),
			DataSize:    formatBytes(dataSize),
			OriDataSize: formatBytes(oriDataSize),
			Columns:     cols,
		}
		blks = append(blks, blkjson)
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

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
	res += "  # Display the data with input file\n"
	res += "  inspect object get --input /your/path/input.json\n"
	res += "\n"
	res += "  # Display the data with method\n"
	res += "  inspect object get -n /your/path/obj-name -i idx -m \"sum\"\n"
	res += "\n"
	res += "  # Display the data with find\n"
	res += "  inspect object get -n /your/path/obj-name -i idx -f \"target\"\n"

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
	res += "  -m, --method='':\n"
	res += "    Specify the method to execute on the column\n"
	res += "    Supported methods: \n"
	res += "        sum: calculate the sum of the column\n"
	res += "        max: calculate the max of the column\n"
	res += "        min: calculate the min of the column\n"
	res += "        rowid: explain the column as rowid\n"
	res += "        blkid: explain the column as blkid\n"
	res += "        location: explain the column as location\n"
	res += "  -f, --find='':\n"
	res += "    Specify the target to find in the column\n"
	res += "  -a, --all=false:\n"
	res += "    Display all rows\n"
	res += "  --input='':\n"
	res += "    Specify the input file\n"

	res += "\n"
	res += "Input Json Template:\n"
	res += "{\n"
	res += "  \"file_name\": \"/path/to/your/file\",\n"
	res += "  \"block_id\": -1,\n"
	res += "  \"columns\": [],\n"
	res += "  \"rows\": [],\n"
	res += "  \"target\": \"\",\n"
	res += "  \"method\": \"\",\n"
	res += "  \"local\": false,\n"
	res += "  \"all\": false,\n"
	res += "}\n"

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
	if c.ctx == nil {
		offlineInit()
	}
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
