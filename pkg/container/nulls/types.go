package nulls

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

/*
type Nulls interface{
	Any() bool

	Length() int

	Add(...uint64)
	Del(...uint64)
	Range(uint64, uint64) Nulls

	Or(Nulls)

	Read([]byte) error
	Show() ([]byte, error)
}
*/

type Nulls struct {
	Np *roaring.Bitmap
}
