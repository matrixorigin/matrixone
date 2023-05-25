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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

func CreateRelation(
	_ context.Context,
	dbH handle.Database,
	name string,
	id uint64,
	defs []engine.TableDef) (err error) {
	schema, err := DefsToSchema(name, defs)
	if err != nil {
		return
	}
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.SegmentMaxBlocks = options.DefaultBlocksPerSegment
	_, err = dbH.CreateRelationWithID(schema, id)
	return err
}

func TableDefs(rel handle.Relation) ([]engine.TableDef, error) {
	schema := rel.Schema().(*catalog.Schema)
	return SchemaToDefs(schema)
}

func TableNamesOfDB(db handle.Database) ([]string, error) {
	var names []string

	it := db.MakeRelationIt()
	for it.Valid() {
		names = append(names, it.GetRelation().Schema().(*catalog.Schema).Name)
		it.Next()
	}
	return names, nil
}

func AppendDataToTable(ctx context.Context, rel handle.Relation, bat *batch.Batch) (err error) {
	dnBat := containers.ToDNBatch(bat)
	defer dnBat.Close()
	err = rel.Append(ctx, dnBat)
	return
}

func GetHideKeysOfTable(rel handle.Relation) ([]*engine.Attribute, error) {
	schema := rel.Schema().(*catalog.Schema)
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
