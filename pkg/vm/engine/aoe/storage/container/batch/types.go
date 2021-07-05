package batch

import (
	"errors"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"sync"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	BatNotFoundErr      = errors.New("not found error")
	BatAlreadyClosedErr = errors.New("already closed error")
)

type IBatch interface {
	dbi.IBatchReader
	GetVectorByAttr(attrId int) vector.IVector
}

type Batch struct {
	sync.RWMutex
	AttrsMap   map[int]int
	Attrs      []int
	Vecs       []vector.IVector
	ClosedMask *roaring.Bitmap
}
