package order

import (
	"fmt"
)

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

type Container struct {
	ds    []bool   // ds[i] == true: the attrs[i] are in descending order
	attrs []string // sorted list of attributes
}

type Field struct {
	Attr string
	Type Direction
}

type Argument struct {
	Fs  []Field
	Ctr Container
}

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (n Field) String() string {
	s := n.Attr
	if n.Type != DefaultDirection {
		s += " " + n.Type.String()
	}
	return s
}

func (i Direction) String() string {
	if i < 0 || i > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", i)
	}
	return directionName[i]
}
