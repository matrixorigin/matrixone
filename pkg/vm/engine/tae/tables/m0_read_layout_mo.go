//go:build mo_m0

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

package tables

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
)

func init() { activeM0PersistentReadLayout = persistentM0ReadLayout }
func persistentM0ReadLayout(ctx context.Context, schema *catalog.Schema, id *common.ID, location objectio.Location, rt *dbutils.Runtime) (*m0read.ObjectReadLayout, error) {
	if schema == nil || id == nil {
		return nil, moerr.NewInternalErrorNoCtx("M0: persistent read owner is missing")
	}
	result := &m0read.ObjectReadLayout{ObjectName: location.Name().String()}
	if schema.Extra == nil || schema.Extra.M0Evolution == nil {
		result.LegacyNullColumns = legacyLogicalIDColumns(id, schema)
		return result, nil
	}
	c := schema.Extra.M0Evolution
	if err := m0read.ValidateEvolution(c, m0read.EvolutionBinding{Account: schema.AcInfo.TenantID, Database: id.DbID, Relation: id.TableID}); err != nil {
		return nil, err
	}
	count := 0
	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		if int(col.SeqNum) != count || count >= len(c.Columns) {
			return nil, moerr.NewInternalErrorNoCtx("M0: persistent schema identity differs from contract")
		}
		sem, err := catalog.EvolutionSchemaColumnSemantic(schema, col, c.Columns[count].ColumnId)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(sem, c.Columns[count].Semantics) {
			return nil, moerr.NewInternalErrorNoCtx("M0: persistent schema semantics differ from contract")
		}
		count++
	}
	if count != len(c.Columns) {
		return nil, moerr.NewInternalErrorNoCtx("M0: persistent schema does not cover contract")
	}
	layout, err := catalog.M0AppendLayoutFromSchema(schema)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReaderWithStr(location.Name().String(), rt.Fs)
	if err != nil {
		return nil, err
	}
	header, err := reader.ReadHeader(ctx, common.DebugAllocator)
	if err != nil {
		return nil, err
	}
	meta, err := reader.ReadAllMeta(ctx, common.DebugAllocator)
	if err != nil {
		return nil, err
	}
	width, err := m0read.ValidateM0ReadBlock(layout, header, meta.MustDataMeta(), uint32(location.ID()))
	if err != nil {
		return nil, err
	}
	result.WrittenSize = width
	result.SchemaSize = layout.NextColSeqnum
	result.Evolution = true
	result.BlockScoped = true
	result.BlockID = location.ID()
	return result, nil
}
