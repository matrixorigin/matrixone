package containers

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type windowBase struct {
	offset, length int
}

func (win *windowBase) IsView() bool                         { return true }
func (win *windowBase) Update(i int, v any)                  { panic("cannot modify window") }
func (win *windowBase) Delete(i int)                         { panic("cannot modify window") }
func (win *windowBase) Append(v any)                         { panic("cannot modify window") }
func (win *windowBase) AppendMany(vs ...any)                 { panic("cannot modify window") }
func (win *windowBase) Extend(o Vector)                      { panic("cannot modify window") }
func (win *windowBase) Length() int                          { return win.length }
func (win *windowBase) Capacity() int                        { return 0 }
func (win *windowBase) Allocated() int                       { return 0 }
func (win *windowBase) Window(offset, length int) Vector     { panic("cannot window a window") }
func (win *windowBase) DataWindow(offset, length int) []byte { panic("cannot window a window") }
func (win *windowBase) Close()                               {}

type vectorWindow struct {
	*windowBase
	ref Vector
}

func (win *vectorWindow) Data() []byte {
	return win.ref.DataWindow(win.offset, win.length)
}
func (win *vectorWindow) Get(i int) (v any) {
	return win.ref.Get(i + win.offset)
}

func (win *vectorWindow) Nullable() bool { return win.ref.Nullable() }
func (win *vectorWindow) HasNull() bool  { return win.ref.HasNull() }
func (win *vectorWindow) NullMask() *roaring64.Bitmap {
	mask := win.ref.NullMask()
	if win.offset == 0 || mask == nil {
		return mask
	}
	return common.BM64Window(mask, win.offset, win.offset+win.length)
}
func (win *vectorWindow) IsNull(i int) bool {
	return win.ref.IsNull(i + win.offset)
}
func (win *vectorWindow) GetAllocator() MemAllocator { return win.ref.GetAllocator() }
func (win *vectorWindow) GetType() types.Type        { return win.ref.GetType() }
func (win *vectorWindow) String() string             { return win.ref.String() }
