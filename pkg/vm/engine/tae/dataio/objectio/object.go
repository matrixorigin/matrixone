// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"strconv"
	"strings"

	"os"
	"path"
	"sync"
)

type ObjectType uint8

const (
	DataType ObjectType = iota
	MetadataSegType
	MetadataBlkType
	NodeType
)

const (
	ObjectSize = 64 * 1024 * 1024
	PageSize   = 4096
	MetaSize   = 512
)

const (
	DATA  = "data"
	META  = "meta"
	SEG   = "seg"
	BLK   = "blk"
	INODE = "inode"
)

type Object struct {
	id        uint64
	mutex     sync.Mutex
	oFile     *os.File
	allocator *ObjectAllocator
	oType     ObjectType
}

func OpenObject(id uint64, oType ObjectType, dir string) (object *Object, err error) {
	object = &Object{
		id:    id,
		oType: oType,
	}
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

func (o *Object) Mount(capacity uint64, pageSize uint32) {
	o.allocator = NewObjectAllocator(capacity, pageSize)
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

func encodeName(id uint64, oType ObjectType) string {
	if oType == NodeType {
		return fmt.Sprintf("%d.%s", id, INODE)
	} else if oType == MetadataSegType {
		return fmt.Sprintf("%d.%s", id, SEG)
	} else if oType == MetadataBlkType {
		return fmt.Sprintf("%d.%s", id, BLK)
	}
	return fmt.Sprintf("%d.%s", id, DATA)
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
		oType = DataType
	} else if oName[1] == INODE {
		oType = NodeType
	} else if oName[1] == SEG {
		oType = MetadataSegType
	} else if oName[1] == BLK {
		oType = MetadataBlkType
	}
	return
}
