package aoe

import (
	"bytes"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine"
)

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
