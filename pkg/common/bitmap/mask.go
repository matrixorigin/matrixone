package bitmap

type Mask interface {
	Count() int
	Iterator() Iterator
}

type BMask struct {
	bm *Bitmap
}

func (b *BMask) Init(bm *Bitmap) {
	b.bm = bm
}

func (b *BMask) Count() int {
	return int(b.bm.Len())
}

func (b *BMask) Iterator() Iterator {
	return b.bm.Iterator()
}

var _ Mask = (*BMask)(nil)
