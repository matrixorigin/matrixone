package objectio

const Magic = 0xFFFFFFFF
const Version = 1
const ObjectSize = 2 << 30

type Object struct {
	oFile     ObjectFile
	footer    *Footer
	allocator *ObjectAllocator
}

func NewObject(path string) (*Object, error) {
	var err error
	object := &Object{
		allocator: NewObjectAllocator(),
	}
	object.oFile, err = NewLocalFile(path)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (o *Object) Append(data []byte, offset uint32) (n int, err error) {
	n, err = o.oFile.WriteAt(data, int64(offset))
	if err != nil {
		return
	}
	return
}
