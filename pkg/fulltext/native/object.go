// Copyright 2025 Matrix Origin
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

package native

import (
	"context"
	"encoding/json"
	"math"
	"strings"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type IndexDefinition struct {
	Name       string
	TableName  string
	Parts      []string
	Param      fulltext.FullTextParserParam
	SkipReason string
}

type ObjectIndexer struct {
	schema   *catalog.Schema
	pkName   string
	pkType   types.T
	indexes  []IndexDefinition
	builders map[string]*Builder
	nextBlk  uint16
}

type resolvedIndex struct {
	def       IndexDefinition
	builder   *Builder
	partIdxes []int
	partTypes []types.T
}

func ExtractIndexDefinitions(schema *catalog.Schema) ([]IndexDefinition, error) {
	if len(schema.Constraint) == 0 {
		return nil, nil
	}

	cstrDef := new(engine.ConstraintDef)
	if err := cstrDef.UnmarshalBinary(schema.Constraint); err != nil {
		return nil, err
	}

	defs := make([]IndexDefinition, 0, 4)
	for _, ct := range cstrDef.Cts {
		idxDef, ok := ct.(*engine.IndexDef)
		if !ok {
			continue
		}
		for _, idx := range idxDef.Indexes {
			if idx == nil || !pkgcatalog.IsFullTextIndexAlgo(idx.IndexAlgo) {
				continue
			}
			param, err := parseIndexParam(idx)
			if err != nil {
				return nil, err
			}
			def := IndexDefinition{
				Name:      idx.IndexName,
				TableName: idx.IndexTableName,
				Parts:     append([]string(nil), idx.Parts...),
				Param:     param,
			}
			if containsDatalink(schema, idx.Parts) {
				def.SkipReason = "datalink columns require query-time tokenization fallback"
			}
			defs = append(defs, def)
		}
	}
	return defs, nil
}

func NewObjectIndexer(schema *catalog.Schema) (*ObjectIndexer, error) {
	indexes, err := ExtractIndexDefinitions(schema)
	if err != nil {
		return nil, err
	}
	pk := schema.GetPrimaryKey()
	ret := &ObjectIndexer{
		schema:   schema,
		pkName:   pk.Name,
		pkType:   pk.Type.Oid,
		indexes:  indexes,
		builders: make(map[string]*Builder, len(indexes)),
	}
	for _, idx := range indexes {
		if idx.SkipReason != "" {
			continue
		}
		ret.builders[idx.TableName] = NewBuilder(idx.Param, nil)
	}
	return ret, nil
}

func (o *ObjectIndexer) Empty() bool {
	return len(o.builders) == 0
}

func (o *ObjectIndexer) IndexCount() int {
	return len(o.indexes)
}

func (o *ObjectIndexer) ActiveIndexCount() int {
	return len(o.builders)
}

func (o *ObjectIndexer) AddBatch(bat *batch.Batch, blockRows []uint32) error {
	if o.Empty() || bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if len(blockRows) == 0 {
		return moerr.NewInternalErrorNoCtx("native fulltext sidecar requires block row layout")
	}

	resolved, pkIdx, err := o.resolveBatch(bat)
	if err != nil {
		return err
	}
	if len(resolved) == 0 {
		return nil
	}

	rowStart := 0
	for _, rows := range blockRows {
		rowCount := int(rows)
		rowEnd := rowStart + rowCount
		if rowEnd > bat.RowCount() {
			return moerr.NewInternalErrorNoCtx("native fulltext sidecar block layout exceeds batch rows")
		}
		for row := rowStart; row < rowEnd; row++ {
			pkBytes := types.EncodeValue(vector.GetAny(bat.Vecs[pkIdx], row, true), o.pkType)
			rowInBlock := uint32(row - rowStart)
			for _, idx := range resolved {
				values, ok, err := collectIndexValues(bat, row, idx.partIdxes, idx.partTypes)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				if err := idx.builder.Add(Document{
					Block:  o.nextBlk,
					Row:    rowInBlock,
					PK:     pkBytes,
					Values: values,
				}); err != nil {
					return err
				}
			}
		}
		rowStart = rowEnd
		o.nextBlk++
	}
	if rowStart != bat.RowCount() {
		return moerr.NewInternalErrorNoCtx("native fulltext sidecar block layout does not cover all rows")
	}
	return nil
}

func (o *ObjectIndexer) Write(ctx context.Context, fs fileservice.FileService, objName objectio.ObjectName) error {
	if o.Empty() {
		return nil
	}
	entries := make([]SidecarLocatorEntry, 0, len(o.builders))
	for _, idx := range o.indexes {
		builder, ok := o.builders[idx.TableName]
		if !ok {
			continue
		}
		seg := builder.Build()
		buf, err := seg.MarshalBinary()
		if err != nil {
			return err
		}
		if err := fs.Write(ctx, fileservice.IOVector{
			FilePath: SidecarPath(objName.String(), idx.TableName),
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   int64(len(buf)),
				Data:   buf,
			}},
		}); err != nil {
			return err
		}
		entries = append(entries, SidecarLocatorEntry{
			IndexTable: idx.TableName,
			FilePath:   SidecarPath(objName.String(), idx.TableName),
		})
	}
	return WriteSidecarLocator(ctx, fs, objName.String(), entries)
}

func ReadSidecar(ctx context.Context, fs fileservice.FileService, objName objectio.ObjectName, indexTableName string) (*Segment, bool, error) {
	vec := &fileservice.IOVector{
		FilePath: SidecarPath(objName.String(), indexTableName),
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	if err := fs.Read(ctx, vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	seg, err := UnmarshalBinary(vec.Entries[0].Data)
	if err != nil {
		return nil, false, err
	}
	return seg, true, nil
}

func AppendQueryBatch(
	builder *Builder,
	bat *batch.Batch,
	pkName string,
	pkType types.T,
	parts []string,
	nextDoc uint64,
) (uint64, error) {
	if builder == nil || bat == nil || bat.RowCount() == 0 {
		return nextDoc, nil
	}

	attrMap := make(map[string]int, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrMap[strings.ToLower(attr)] = i
	}
	pkIdx, ok := attrMap[strings.ToLower(pkName)]
	if !ok {
		return nextDoc, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing primary key column in batch")
	}

	partIdxes := make([]int, 0, len(parts))
	partTypes := make([]types.T, 0, len(parts))
	for _, part := range parts {
		colIdx, ok := attrMap[strings.ToLower(part)]
		if !ok {
			return nextDoc, moerr.NewInternalErrorNoCtx("native fulltext tail scan missing indexed column in batch")
		}
		partIdxes = append(partIdxes, colIdx)
		partTypes = append(partTypes, bat.Vecs[colIdx].GetType().Oid)
	}

	for row := 0; row < bat.RowCount(); row++ {
		block := nextDoc / objectio.BlockMaxRows
		if block > math.MaxUint16 {
			return nextDoc, moerr.NewInternalErrorNoCtx("native fulltext tail scan exceeded synthetic block range")
		}
		values, ok, err := collectIndexValues(bat, row, partIdxes, partTypes)
		if err != nil {
			return nextDoc, err
		}
		if !ok {
			nextDoc++
			continue
		}
		pkBytes := types.EncodeValue(vector.GetAny(bat.Vecs[pkIdx], row, true), pkType)
		if err := builder.Add(Document{
			Block:  uint16(block),
			Row:    uint32(nextDoc % objectio.BlockMaxRows),
			PK:     pkBytes,
			Values: values,
		}); err != nil {
			return nextDoc, err
		}
		nextDoc++
	}
	return nextDoc, nil
}

func parseIndexParam(idx *plan.IndexDef) (fulltext.FullTextParserParam, error) {
	var param fulltext.FullTextParserParam
	if len(idx.IndexAlgoParams) == 0 {
		return param, nil
	}
	return param, json.Unmarshal([]byte(idx.IndexAlgoParams), &param)
}

func containsDatalink(schema *catalog.Schema, parts []string) bool {
	for _, part := range parts {
		colIdx, ok := schema.NameMap[part]
		if !ok {
			continue
		}
		if schema.ColDefs[colIdx].Type.Oid == types.T_datalink {
			return true
		}
	}
	return false
}

func (o *ObjectIndexer) resolveBatch(bat *batch.Batch) ([]resolvedIndex, int, error) {
	attrMap := make(map[string]int, len(bat.Attrs))
	for i, attr := range bat.Attrs {
		attrMap[strings.ToLower(attr)] = i
	}
	pkIdx, ok := attrMap[strings.ToLower(o.pkName)]
	if !ok {
		return nil, -1, moerr.NewInternalErrorNoCtx("native fulltext sidecar missing primary key column in batch")
	}

	resolved := make([]resolvedIndex, 0, len(o.indexes))
	for _, idx := range o.indexes {
		builder, ok := o.builders[idx.TableName]
		if !ok {
			continue
		}
		partIdxes := make([]int, 0, len(idx.Parts))
		partTypes := make([]types.T, 0, len(idx.Parts))
		for _, part := range idx.Parts {
			colIdx, ok := attrMap[strings.ToLower(part)]
			if !ok {
				return nil, -1, moerr.NewInternalErrorNoCtx("native fulltext sidecar missing indexed column in batch")
			}
			partIdxes = append(partIdxes, colIdx)
			partTypes = append(partTypes, bat.Vecs[colIdx].GetType().Oid)
		}
		resolved = append(resolved, resolvedIndex{
			def:       idx,
			builder:   builder,
			partIdxes: partIdxes,
			partTypes: partTypes,
		})
	}
	return resolved, pkIdx, nil
}

func collectIndexValues(bat *batch.Batch, row int, partIdxes []int, partTypes []types.T) ([]fulltext.IndexValue, bool, error) {
	values := make([]fulltext.IndexValue, 0, len(partIdxes))
	for i, partIdx := range partIdxes {
		vec := bat.Vecs[partIdx]
		if vec.IsNull(uint64(row)) {
			continue
		}
		values = append(values, fulltext.IndexValue{
			Text: vec.GetStringAt(row),
			Raw:  vec.GetRawBytesAt(row),
			Type: partTypes[i],
		})
	}
	if len(values) == 0 {
		return nil, false, nil
	}
	return values, true, nil
}
