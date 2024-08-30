// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"context"
	"regexp"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

var MaxRows = regexp.MustCompile(`rows:(\d+)`)
var MaxBlks = regexp.MustCompile(`blks:(\d+)`)

func CreateRelation(
	_ context.Context,
	dbH handle.Database,
	name string,
	id uint64,
	defs []engine.TableDef) (err error) {
	schema, err := catalog.DefsToSchema(name, defs)
	if err != nil {
		return
	}
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.ObjectMaxBlocks = options.DefaultBlocksPerObject
	if len(schema.Comment) > 0 {
		if r := MaxRows.FindStringSubmatch(schema.Comment); len(r) > 0 {
			if maxrows, err := strconv.Atoi(r[1]); err == nil {
				schema.BlockMaxRows = uint32(maxrows)
			}
		}
		if r := MaxBlks.FindStringSubmatch(schema.Comment); len(r) > 0 {
			if maxblks, err := strconv.Atoi(r[1]); err == nil {
				schema.ObjectMaxBlocks = uint16(maxblks)
			}
		}
	}
	_, err = dbH.CreateRelationWithID(schema, id)
	return err
}

func TableDefs(rel handle.Relation) ([]engine.TableDef, error) {
	schema := rel.Schema(false).(*catalog.Schema)
	return catalog.SchemaToDefs(schema)
}

func TableNamesOfDB(db handle.Database) ([]string, error) {
	var names []string

	it := db.MakeRelationIt()
	for it.Valid() {
		names = append(names, it.GetRelation().Schema(false).(*catalog.Schema).Name)
		it.Next()
	}
	return names, nil
}

func AppendDataToTable(ctx context.Context, rel handle.Relation, bat *batch.Batch) (err error) {
	tnBat := containers.ToTNBatch(bat, common.WorkspaceAllocator)
	defer tnBat.Close()
	err = rel.Append(ctx, tnBat)
	return
}

func GetHideKeysOfTable(rel handle.Relation) ([]*engine.Attribute, error) {
	schema := rel.Schema(false).(*catalog.Schema)
	if schema.PhyAddrKey == nil {
		return nil, moerr.NewNotSupportedNoCtx("system table has no rowid")
	}
	key := new(engine.Attribute)
	key.Name = schema.PhyAddrKey.Name
	key.Type = schema.PhyAddrKey.Type
	key.IsRowId = true
	// key.IsHidden = true
	logutil.Debugf("GetHideKey: %v", key)
	return []*engine.Attribute{key}, nil
}
