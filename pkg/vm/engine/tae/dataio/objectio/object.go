package objectio

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

const OBJECT_SIZE = 64 * 1024 * 1024
const PAGE_SIZE = 4096

type ObjectType uint8

const (
	DATATYPE ObjectType = iota
	METADATA
)

const (
	DATA = "data"
	META = "meta"
)

type Object struct {
	id        uint64
	mutex     sync.Mutex
	oFile     *os.File
	allocator *ObjectAllocator
	oType     ObjectType
}

func encodeName(id uint64, oType ObjectType) string {
	if oType == DATATYPE {
		return fmt.Sprintf("%d.%s", id, DATA)
	}
	return fmt.Sprintf("%d.%s", id, META)
}

func decodeName(name string) (id uint64, oType ObjectType, err error) {
	oName := strings.Split(name, ".")
	if len(oName) != 2 {
		err = fmt.Errorf("%w: %s", file.ErrInvalidName, name)
		return
	}
	id, err = strconv.ParseUint(oName[0], 10, 64)
	if err != nil {
		err = fmt.Errorf("%w: %s", file.ErrInvalidName, name)
	}
	if oName[1] == DATA {
		oType = DATATYPE
	} else {
		oType = METADATA
	}
	return
}

func OpenObject(id uint64, oType ObjectType, dir string) (object *Object, err error) {
	object = &Object{
		id:    id,
		oType: oType,
	}
	object.allocator = NewObjectAllocator(OBJECT_SIZE, PAGE_SIZE)
	path := path.Join(dir, encodeName(id, oType))
	if _, err = os.Stat(path); os.IsNotExist(err) {
		object.oFile, err = os.Create(path)
		return
	}

	if object.oFile, err = os.OpenFile(path, os.O_RDWR, os.ModePerm); err != nil {
		return
	}
	return
}

func (o *Object) Append(data []byte) (offset, allocated uint64, err error) {
	offset, allocated = o.allocator.Allocate(uint64(len(data)))
	_, err = o.oFile.WriteAt(data, int64(offset))
	if err != nil {
		return
	}
	return
}

func (o *Object) Read(offset int64, data []byte) (length int, err error) {
	length, err = o.oFile.ReadAt(data, offset)
	return
}

func (o *Object) GetSize() uint64 {
	return o.allocator.GetAvailable()
}
