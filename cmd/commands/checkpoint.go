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
	"github.com/matrixorigin/matrixone/pkg/backup"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"sort"
)

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
	res += "\n"
	res += "  # Display information for the given checkpoint\n"
	res += "  inspect checkpoint stat -c ckp_end_ts\n"
	res += "\n"
	res += "  # [Offline] display latest checkpoints in the meta file\n"
	res += "  inspect checkpoint stat -n /your/path/meta_file\n"
	res += "\n"
	res += "  # Display the checkpoints stat with input file\n"
	res += "  inspect checkpoint stat --input /your/path/input.json\n"

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

	res += "\n"
	res += "Input Json Template:\n"
	res += "{\n"
	res += "  \"file_name\": \"/path/to/your/file\",\n"
	res += "  \"ckp_end_ts\": \"\",\n"
	res += "  \"table_id\": -1,\n"
	res += "  \"limit\": 10000,\n"
	res += "  \"all\": false,\n"
	res += "  \"download\": false\n"
	res += "}\n"

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
		offlineInit()
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
	fmt.Printf("prefetch meta idx %v\n", data)
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
	res += "\n"
	res += "  # Display all tables for the given checkpoint\n"
	res += "  inspect checkpoint list -c ckp_end_ts\n"
	res += "\n"
	res += "  # Download latest checkpoints, the dir is mo-data/local/ckp\n"
	res += "  inspect checkpoint list -d\n"
	res += "\n"
	res += "  # [Offline] display all checkpoints in the meta file\n"
	res += "  inspect checkpoint list -n /your/path/meta_file\n"
	res += "\n"
	res += "  # Display the checkpoints with input file\n"
	res += "  inspect checkpoint list --input /your/path/input.json\n"

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
	res += "  --input=\"\":\n"
	res += "    The input json file\n"

	res += "\n"
	res += "Input Json Template:\n"
	res += "{\n"
	res += "  \"file_name\": \"/path/to/your/file\",\n"
	res += "  \"ckp_end_ts\": \"\",\n"
	res += "  \"limit\": 10000,\n"
	res += "  \"all\": false,\n"
	res += "  \"download\": false\n"
	res += "}\n"

	return
}

func (c *ckpListArg) Run() (err error) {
	if c.ctx == nil {
		offlineInit()
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
