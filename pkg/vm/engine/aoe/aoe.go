package aoe

import (
	"bytes"
	"encoding/gob"
	"matrixone/pkg/container/types"
	"matrixone/pkg/encoding"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine"
)

func init() {
	gob.Register(TableInfo{})
	gob.Register(types.Type{})
	gob.Register(IndexInfo{})
	gob.Register(ColumnInfo{})
}

func Transfer(sid, tid, typ uint64, name, comment string,
	defs []engine.TableDef, pdef *engine.PartitionBy) (TableInfo, error) {
	var tbl TableInfo

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
	tbl.Indexes = IndexDefs(sid, tid, mp, defs)
	if pdef != nil {
		data, err := PartitionDef(pdef)
		if err != nil {
			return tbl, err
		}
		tbl.Partition = data
	}
	return tbl, nil
}

func EncodeTable(tbl TableInfo) ([]byte, error) {
	return encoding.Encode(tbl)
}

func DecodeTable(data []byte) (TableInfo, error) {
	var tbl TableInfo

	err := encoding.Decode(data, &tbl)
	return tbl, err
}

func IndexDefs(sid, tid uint64, mp map[string]uint64, defs []engine.TableDef) []IndexInfo {
	var id uint64
	var idxs []IndexInfo

	for _, def := range defs {
		if v, ok := def.(*engine.IndexTableDef); ok {
			idx := IndexInfo{
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

func ColumnDefs(sid, tid uint64, defs []engine.TableDef) []ColumnInfo {
	var id uint64
	var cols []ColumnInfo

	for _, def := range defs {
		if v, ok := def.(*engine.AttributeDef); ok {
			cols = append(cols, ColumnInfo{
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
