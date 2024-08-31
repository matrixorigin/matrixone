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
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/spf13/cobra"
	"os"
)

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
