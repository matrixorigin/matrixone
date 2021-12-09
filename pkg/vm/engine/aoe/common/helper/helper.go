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

package helper

import (
	"bytes"
	"encoding/gob"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"

	//"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/protocol"
)

func init() {
	gob.Register(aoe.TableInfo{})
	gob.Register(types.Type{})
	gob.Register(aoe.IndexInfo{})
	gob.Register(aoe.ColumnInfo{})
}

func Transfer(sid, tid, typ uint64, name string,
	defs []engine.TableDef) (aoe.TableInfo, error) {
	var tbl aoe.TableInfo

	tbl.SchemaId = sid
	tbl.Id = tid
	tbl.Name = name
	tbl.Type = typ
	tbl.Comment = []byte(CommentDefs(defs))
	tbl.Columns = ColumnDefs(sid, tid, defs)
	mp := make(map[string]uint64)
	{
		for _, col := range tbl.Columns {
			mp[col.Name] = col.Id
		}
	}
	tbl.Indices = IndexDefs(sid, tid, mp, defs)
	tbl.Properties, _ = PropertyDef(defs)
	data, err := PartitionDef(defs)
	if err != nil {
		return tbl, err
	}
	if data != nil {
		tbl.Partition = data
	}
	return tbl, nil
}

func UnTransfer(tbl aoe.TableInfo) (uint64, uint64, uint64, string, []engine.TableDef, error) {
	var err error
	var defs []engine.TableDef
	var pdef *engine.PartitionByDef
	var comment *engine.CommentDef

	if len(tbl.Partition) > 0 {
		if pdef, _, err = protocol.DecodePartition(tbl.Partition); err != nil {
			return 0, 0, 0, "", nil, err
		}
		defs = append(defs, pdef)
	}
	for _, col := range tbl.Columns {
		defs = append(defs, &engine.AttributeDef{
			Attr: engine.Attribute{
				Alg:     compress.T(col.Alg),
				Name:    col.Name,
				Type:    col.Type,
				Default: col.Default,
			},
		})
	}
	for _, idx := range tbl.Indices {
		defs = append(defs, &engine.IndexTableDef{
			Typ:      engine.IndexT(idx.Type),
			ColNames: idx.ColumnNames,
			Name:     idx.Name,
		})
	}
	if tbl.Comment != nil {
		comment.Comment = string(tbl.Comment)
		defs = append(defs, comment)
	}
	return tbl.SchemaId, tbl.Id, tbl.Type, tbl.Name, defs, nil
}

func EncodeTable(tbl aoe.TableInfo) ([]byte, error) {
	return encoding.Encode(tbl)
}

func DecodeTable(data []byte) (aoe.TableInfo, error) {
	var tbl aoe.TableInfo

	err := encoding.Decode(data, &tbl)
	return tbl, err
}

func DecodeIndex(data []byte) (aoe.IndexInfo, error) {
	var idx aoe.IndexInfo

	err := encoding.Decode(data, &idx)
	return idx, err
}

func IndexDefs(sid, tid uint64, mp map[string]uint64, defs []engine.TableDef) []aoe.IndexInfo {
	var id uint64
	var idxs []aoe.IndexInfo

	for _, def := range defs {
		if v, ok := def.(*engine.IndexTableDef); ok {
			var tp aoe.IndexT
			switch v.Typ {
			case engine.ZoneMap:
				tp = aoe.ZoneMap
			case engine.BsiIndex:
				tp = aoe.Bsi
			default:
				tp = aoe.Invalid
			}
			idx := aoe.IndexInfo{
				SchemaId: sid,
				TableId:  tid,
				// Id:       id,
				Type: tp,
				Name: v.Name,
			}
			for _, name := range v.ColNames {
				idx.ColumnNames = append(idx.ColumnNames, name)
				//TODO
				if mp != nil {
					idx.Columns = append(idx.Columns, mp[name])
				}
			}
			idxs = append(idxs, idx)
			id++
		}
	}
	return idxs
}

func ColumnDefs(sid, tid uint64, defs []engine.TableDef) []aoe.ColumnInfo {
	var id uint64
	var cols []aoe.ColumnInfo
	var primaryKeys []string
	for _, def := range defs {
		if v, ok := def.(*engine.PrimaryIndexDef); ok {
			primaryKeys = v.Names
		}
	}
	for _, def := range defs {
		if v, ok := def.(*engine.AttributeDef); ok {
			col := aoe.ColumnInfo{
				SchemaId: sid,
				TableID:  tid,
				Id:       id,
				Name:     v.Attr.Name,
				Alg:      int(v.Attr.Alg),
				Type:     v.Attr.Type,
				Default:  v.Attr.Default,
			}
			for _, primaryKey := range primaryKeys {
				if col.Name == primaryKey {
					col.PrimaryKey = true
				}
			}
			cols = append(cols, col)
			id++
		}
	}
	return cols
}

func CommentDefs(defs []engine.TableDef) string {
	for _, def := range defs {
		if c, ok := def.(*engine.CommentDef); ok {
			return c.Comment
		}
	}
	return ""
}

func PartitionDef(defs []engine.TableDef) ([]byte, error) {
	for _, def := range defs {
		if p, ok := def.(*engine.PartitionByDef); ok {
			var buf bytes.Buffer
			if err := protocol.EncodePartition(p, &buf); err != nil {
				return nil, err
			}
			return buf.Bytes(), nil
		}
	}
	return nil, nil
}

func PropertyDef(defs []engine.TableDef) ([]aoe.Property, error) {
	for _, def := range defs {
		if propertiesDef, ok := def.(*engine.PropertiesDef); ok {
			properties := make([]aoe.Property, len(propertiesDef.Properties))
			for i, engineProperty := range propertiesDef.Properties {
				property := aoe.Property{
					Key:   engineProperty.Key,
					Value: engineProperty.Value,
				}
				properties[i] = property
			}
			return properties, nil
		}
	}
	return nil, nil
}

func Index(tbl aoe.TableInfo) []*engine.IndexTableDef {
	defs := make([]*engine.IndexTableDef, len(tbl.Indices))
	for i, idx := range tbl.Indices {
		defs[i] = &engine.IndexTableDef{
			Typ:      engine.IndexT(idx.Type),
			ColNames: idx.ColumnNames,
			Name:     idx.Name,
		}
	}
	return defs
}

func Attribute(tbl aoe.TableInfo) []engine.Attribute {
	attrs := make([]engine.Attribute, len(tbl.Columns))
	for i, col := range tbl.Columns {
		attrs[i] = engine.Attribute{
			Alg:     compress.T(col.Alg),
			Name:    col.Name,
			Type:    col.Type,
			Default: col.Default,
		}
	}
	return attrs
}
