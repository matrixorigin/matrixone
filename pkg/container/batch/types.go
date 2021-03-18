package batch

import (
	"matrixbase/pkg/container/vector"

	aio "github.com/traetox/goaio"
)

type Info struct {
	Alg int
	Ref uint64
	Wg  *WaitGroup
}

type WaitGroup struct {
	Ap *aio.AIO
	Id aio.RequestId
}

type Batch struct {
	Ro       bool // true: attrs is read only
	Is       []Info
	SelsData []byte
	Sels     []int64
	Attrs    []string
	Vecs     []*vector.Vector
}
