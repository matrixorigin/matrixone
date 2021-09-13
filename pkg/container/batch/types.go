package batch

import (
	"matrixone/pkg/container/vector"
)

/*
 * batch represents a part of a relationship
 * 	including an optional list of row numbers, columns and list of attributes
 * 	 	(SelsData, Sels) - list of row numbers
 * 		(Attrs) - list of attributes
 *      (vecs) 	- columns
 */
type Batch struct {
	Ro       bool // true: attrs is read only
	SelsData []byte
	Sels     []int64
	Attrs    []string
	Vecs     []*vector.Vector
}
