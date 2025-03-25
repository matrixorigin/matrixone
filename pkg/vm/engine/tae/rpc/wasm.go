// Copyright 2021 Matrix Origin
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
	"errors"
	"fmt"
	"net/url"

	extism "github.com/extism/go-sdk"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/spf13/cobra"
)

func preparePlugin(address string) (*extism.Plugin, error) {
	extism.SetLogLevel(extism.LogLevelInfo)
	wurl, err := url.Parse(address)
	if err != nil {
		return nil, err
	}

	var manifest extism.Manifest
	if wurl.Scheme == "https" || wurl.Scheme == "http" {
		manifest = extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmUrl{
					Url: address,
				},
			},
		}
	} else {
		manifest = extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmFile{
					Path: address,
				},
			},
		}
	}
	config := extism.PluginConfig{
		EnableWasi: true,
	}
	ctx := context.Background()
	return extism.NewPlugin(ctx, manifest, config, []extism.HostFunction{})
}

type wasmArg struct {
	ctx      *inspectContext
	wasm     string
	function string
	dryrun   bool
	tbl      *catalog.TableEntry
}

func (c *wasmArg) FromCommand(cmd *cobra.Command) error {
	c.ctx = cmd.Flag("ictx").Value.(*inspectContext)
	c.wasm, _ = cmd.Flags().GetString("wasm")
	c.function, _ = cmd.Flags().GetString("function")
	if c.wasm == "" {
		return moerr.NewInvalidInputNoCtx("wasm is required")
	}
	c.dryrun, _ = cmd.Flags().GetBool("dryrun")
	address, _ := cmd.Flags().GetString("target")
	var err error
	c.tbl, err = parseTableTarget(address, c.ctx.acinfo, c.ctx.db)
	if err != nil {
		return err
	}
	if c.tbl == nil {
		return moerr.NewInvalidInputNoCtx("table not found")
	}
	return nil
}

func (c *wasmArg) String() string {
	t := "*"
	if c.tbl != nil {
		t = fmt.Sprintf("%d-%s", c.tbl.ID, c.tbl.GetLastestSchemaLocked(false).Name)
	}
	return fmt.Sprintf("wasm: %v, dryrun: %v, table: %v", c.wasm, c.dryrun, t)
}

func collectObjectStats(c *wasmArg) []byte {
	it := c.tbl.MakeDataVisibleObjectIt(txnbase.MockTxnReaderWithNow())
	input := make([]byte, 0, objectio.ObjectStatsLen*128)
	for it.Next() {
		obj := it.Item()
		if !merge.ObjectValid(obj) {
			continue
		}
		input = append(input, obj.ObjectStats[:]...)
	}
	return input
}

func (c *wasmArg) dispatchMergeTask(tbl *catalog.TableEntry, stats objectio.ObjectStatsSlice) error {
	mObjs := make([]*catalog.ObjectEntry, 0, stats.Len())
	for i := 0; i < stats.Len(); i++ {
		entry, err := tbl.GetObjectByID(stats.Get(i).ObjectName().ObjectId(), false)
		if err != nil {
			continue
		}
		mObjs = append(mObjs, entry)
	}

	scopes := make([]common.ID, 0, len(mObjs))
	for _, obj := range mObjs {
		scopes = append(scopes, *obj.AsCommonID())
	}
	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		txn.GetMemo().IsFlushOrMerge = true
		return jobs.NewMergeObjectsTask(ctx, txn, mObjs, c.ctx.db.Runtime, common.DefaultMaxOsizeObjMB*common.Const1MBytes, false)
	}
	task, err := c.ctx.db.Runtime.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, tasks.DataCompactionTask, scopes, factory)
	if err != nil {
		if !errors.Is(err, tasks.ErrScheduleScopeConflict) {
			logutil.Info(
				"MergeExecutorError",
				common.OperationField("schedule-merge-task"),
				common.AnyField("error", err),
				common.AnyField("task", task.Name()),
			)
		}
		return err
	}
	err = task.WaitDone(context.Background())
	if err != nil {
		return err
	}
	tbl.Stats.SetLastMergeTime()
	return nil
}

func (c *wasmArg) Run() error {
	plugin, err := preparePlugin(c.wasm)
	if err != nil {
		return err
	}

	input := collectObjectStats(c)
	_, out, err := plugin.Call(c.function, input)
	if err != nil {
		return err
	}

	selected := objectio.ObjectStatsSlice(out)
	if c.dryrun || len(selected) < 2 {
		c.ctx.resp.Payload = []byte(fmt.Sprintf("dryrun(%d):\n", len(selected)/objectio.ObjectStatsLen))
		for i := 0; i < selected.Len(); i++ {
			stat := selected.Get(i)
			c.ctx.resp.Payload = append(c.ctx.resp.Payload, []byte(fmt.Sprintf("%s %d %d\n", stat.ObjectName().ObjectId().ShortStringEx(), stat.OriginSize(), stat.Rows()))...)
		}
		return nil
	}
	return c.dispatchMergeTask(c.tbl, selected)
}

func (c *wasmArg) PrepareCommand() *cobra.Command {
	wasmCmd := &cobra.Command{
		Use:   "wasm",
		Short: "run wasm policy for table",
		Run:   RunFactory(c),
	}
	wasmCmd.Flags().StringP("target", "t", "*", "format: db.table")
	wasmCmd.Flags().StringP("wasm", "w", "", "wasm file path")
	wasmCmd.Flags().StringP("function", "f", "filterStats", "wasm function")
	wasmCmd.Flags().BoolP("dryrun", "d", false, "dryrun")
	return wasmCmd
}
