package myoutput

import "matrixone/pkg/container/batch"

type Argument struct {
	Attrs []string
	Data  interface{}
	Func  func(interface{}, *batch.Batch)
}
