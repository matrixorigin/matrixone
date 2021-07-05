package batch

import (
	"errors"
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"sync"
)

var (
	BatNotFoundErr      = errors.New("not found error")
	BatAlreadyClosedErr = errors.New("already closed error")
)

type IBatch interface {
	IsReadonly() bool
	Length() int
	GetAttrs() []int
	GetVectorByAttr(attrId int) vector.IVector
	Close() error
	CloseVector(idx int) error
	IsVectorClosed(idx int) bool
}

type Batch struct {
	sync.RWMutex
	AttrsMap   map[int]int
	Attrs      []int
	Vecs       []vector.IVector
	ClosedMask *roaring.Bitmap
}
