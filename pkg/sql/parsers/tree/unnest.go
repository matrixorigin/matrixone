package tree

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type UnnestParam struct {
	Origin interface{}
	Path   string
	Outer  bool
	IsCol  bool
}

type Unnest struct {
	statementImpl
	Param *UnnestParam
}

func (node *Unnest) Format(ctx *FmtCtx) {
	ctx.WriteString("unnest(")
	ctx.WriteString(node.getOrigin())
	ctx.WriteString(", ")
	ctx.WriteString(node.Param.Path)
	ctx.WriteString(", ")
	ctx.WriteString(fmt.Sprintf("%v", node.Param.Outer))
	ctx.WriteByte(')')
}

func (node Unnest) String() string {
	originStr := node.getOrigin()
	return fmt.Sprintf("unnest(%s, %s, %v)", originStr, node.Param.Path, node.Param.Outer)
}

func (node Unnest) getOrigin() string {
	switch node.Param.Origin.(type) {
	case string:
		return fmt.Sprintf("%s", node.Param.Origin)
	case *UnresolvedName:
		dbName, tableName, colName := node.Param.Origin.(*UnresolvedName).GetNames()
		if len(dbName) > 0 {
			return fmt.Sprintf("%s.%s.%s", dbName, tableName, colName)
		} else if len(tableName) > 0 {
			return fmt.Sprintf("%s.%s", tableName, colName)
		} else {
			return fmt.Sprintf("%s", colName)
		}
	default:
		panic("unknown type")
	}
}

//func (node Unnest) GetParam() map[string]string {
//
//	ret := map[string]string{
//		"origin": node.getOrigin(),
//		"path":   node.Param.Path,
//		"outer":  fmt.Sprintf("%v", node.Param.Outer),
//		"isCol":  fmt.Sprintf("%v", node.Param.IsCol),
//	}
//	return ret
//}

func (p UnnestParam) Marshal() ([]byte, error) {
	gob.Register(p.Origin)
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(p)
	return buf.Bytes(), err
}
func (p *UnnestParam) Unmarshal(data []byte) error {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	return dec.Decode(p)
}
