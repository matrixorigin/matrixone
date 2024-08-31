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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/spf13/cobra"
	"io"
	"strconv"
	"strings"
)

const (
	invalidId    = -1
	invalidLimit = 0xffffff

	brief    = 0
	standard = 1
	detailed = 2

	sid           = "inspect"
	checkpointDir = "ckp/"
)

func offlineInit() {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "fatal",
		Format: "console",
	})
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
	Index       uint16       `json:"block_index"`
	Rows        uint32       `json:"row_count,omitempty"`
	Cols        uint16       `json:"column_count,omitempty"`
	DataSize    string       `json:"data_size,omitempty"`
	OriDataSize string       `json:"original_data_size,omitempty"`
	Columns     []ColumnJson `json:"columns,omitempty"`
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

	obj := &ObjArg{}
	rootCmd.AddCommand(obj.PrepareCommand())

	table := &TableArg{}
	rootCmd.AddCommand(table.PrepareCommand())

	return rootCmd
}

type InspectArg struct {
}

func (c *InspectArg) PrepareCommand() *cobra.Command {
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

	//ckp := CheckpointArg{}
	//moInspectCmd.AddCommand(ckp.PrepareCommand())

	return moInspectCmd
}

func (c *InspectArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *InspectArg) String() string {
	return c.Usage()
}

func (c *InspectArg) Usage() (res string) {
	res += "Commands:\n"
	res += fmt.Sprintf("  %-15v object analysis tool \n", "object")
	res += fmt.Sprintf("  %-15v table analysis tool \n", "table")
	//res += fmt.Sprintf("  %-15v checkpoint analysis tool \n", "checkpoint")

	res += "\n"
	res += "Usage:\n"
	res += "inspect [flags] [options]\n"

	res += "\n"
	res += "Use \"mo-tool inspect <command> --help\" for more information about a given command.\n"

	return
}

func (c *InspectArg) Run() (err error) {
	return
}
