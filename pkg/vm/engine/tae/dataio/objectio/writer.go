package objectio

type ObjectWriter struct {
	object *Object
	blocks map[uint64]*Block
}

func NewObjectWriter(
	name string,
) (*ObjectWriter, error) {
	object, err := NewObject(name)
	if err != nil {
		return nil, err
	}
	writer := &ObjectWriter{
		object: object,
	}
	return writer, nil
}

func (w *ObjectWriter) Write(data []byte) error {
	var err error
	offset, allocated := w.object.allocator.Allocate(uint32(len(data)))
	n, err := w.object.Append(data, offset)
	if err != nil {
		return err
	}
	if n != int(allocated) {
		panic(any("Write Failed!"))
	}
	return err
}

func (w *ObjectWriter) Flush() error {
	var err error
	return err
}

func (w *ObjectWriter) Sync() error {
	var err error
	return err
}
