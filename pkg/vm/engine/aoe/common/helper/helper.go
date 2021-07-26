package helper

import (
	"bytes"
	"encoding/gob"
	"matrixone/pkg/container/types"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/metadata"
)

func init() {
	gob.Register(aoe.TableInfo{})
	gob.Register(types.Type{})
	gob.Register(aoe.IndexInfo{})
	gob.Register(aoe.ColumnInfo{})
}

func Transfer(sid, tid, typ uint64, name, comment string,
	defs []engine.TableDef, pdef *engine.PartitionBy) (aoe.TableInfo, error) {
	var tbl aoe.TableInfo

	tbl.SchemaId = sid
	tbl.Id = tid
	tbl.Name = name
	tbl.Type = typ
	tbl.Comment = []byte(comment)
	tbl.Columns = ColumnDefs(sid, tid, defs)
	mp := make(map[string]uint64)
	{
		for _, col := range tbl.Columns {
			mp[col.Name] = col.Id
		}
	}
	tbl.Indices = IndexDefs(sid, tid, mp, defs)
	if pdef != nil {
		data, err := PartitionDef(pdef)
		if err != nil {
			return tbl, err
		}
		tbl.Partition = data
	}
	return tbl, nil
}

func UnTransfer(tbl aoe.TableInfo) (uint64, uint64, uint64, string, string, []engine.TableDef, *engine.PartitionBy, error) {
	var err error
	var defs []engine.TableDef
	var pdef *engine.PartitionBy

	if len(tbl.Partition) > 0 {
		if pdef, _, err = protocol.DecodePartition(tbl.Partition); err != nil {
			return 0, 0, 0, "", "", nil, nil, err
		}
	}
	for _, col := range tbl.Columns {
		defs = append(defs, &engine.AttributeDef{
			Attr: metadata.Attribute{
				Alg:  col.Alg,
				Name: col.Name,
				Type: col.Type,
			},
		})
	}
	for _, idx := range tbl.Indices {
		defs = append(defs, &engine.IndexTableDef{
			Typ:   int(idx.Type),
			Names: idx.Names,
		})
	}
	return tbl.SchemaId, tbl.Id, tbl.Type, tbl.Name, string(tbl.Comment), defs, pdef, nil
}

func EncodeTable(tbl aoe.TableInfo) ([]byte, error) {
	return encoding.Encode(tbl)
}

func DecodeTable(data []byte) (aoe.TableInfo, error) {
	var tbl aoe.TableInfo

	err := encoding.Decode(data, &tbl)
	return tbl, err
}

func IndexDefs(sid, tid uint64, mp map[string]uint64, defs []engine.TableDef) []aoe.IndexInfo {
	var id uint64
	var idxs []aoe.IndexInfo

	for _, def := range defs {
		if v, ok := def.(*engine.IndexTableDef); ok {
			idx := aoe.IndexInfo{
				SchemaId: sid,
				TableId:  tid,
				Id:       id,
				Type:     uint64(v.Typ),
			}
			for _, name := range v.Names {
				idx.Names = append(idx.Names, name)
				idx.Columns = append(idx.Columns, mp[name])
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

	for _, def := range defs {
		if v, ok := def.(*engine.AttributeDef); ok {
			cols = append(cols, aoe.ColumnInfo{
				SchemaId: sid,
				TableID:  tid,
				Id:       id,
				Name:     v.Attr.Name,
				Alg:      v.Attr.Alg,
				Type:     v.Attr.Type,
			})
			id++
		}
	}
	return cols
}

func PartitionDef(def *engine.PartitionBy) ([]byte, error) {
	var buf bytes.Buffer

	if err := protocol.EncodePartition(def, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Index(tbl aoe.TableInfo) []*engine.IndexTableDef {
	defs := make([]*engine.IndexTableDef, len(tbl.Indices))
	for i, idx := range tbl.Indices {
		defs[i] = &engine.IndexTableDef{
			Typ:   int(idx.Type),
			Names: idx.Names,
		}
	}
	return defs
}

func Attribute(tbl aoe.TableInfo) []metadata.Attribute {
	attrs := make([]metadata.Attribute, len(tbl.Columns))
	for i, col := range tbl.Columns {
		attrs[i] = metadata.Attribute{
			Alg:  col.Alg,
			Name: col.Name,
			Type: col.Type,
		}
	}
	return attrs
}
