package objectio

import (
	"bytes"
	"encoding/binary"
	"sync"
	"unsafe"
)

type StateType uint8
type InodeType uint8

const (
	RESIDENT StateType = iota
	REMOVE
)

const (
	FILE InodeType = iota
	DIR
)

const MAGIC = 0xFFFFFFFF

type Inode struct {
	magic    uint64
	inode    uint64
	name     string
	algo     uint8
	size     uint64
	dataSize uint64
	rows     uint32
	cols     uint32
	idxs     uint32
	mutex    sync.RWMutex
	extents  []Extent
	typ      InodeType
	seq      uint64
	objectId uint64
}

func (i *Inode) GetFileSize() int64 {
	return int64(i.size)
}

func (i *Inode) GetDataSize() int64 {
	return int64(i.dataSize)
}

func (i *Inode) GetAlgo() uint8 {
	return i.algo
}

func (i *Inode) GetRows() uint32 {
	return i.rows
}

func (i *Inode) GetCols() uint32 {
	return i.cols
}

func (i *Inode) GetIdxs() uint32 {
	return i.idxs
}

func (i *Inode) Marshal() (buf []byte, err error) {
	var (
		buffer bytes.Buffer
	)
	if err = binary.Write(&buffer, binary.BigEndian, i.magic); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.inode); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.typ); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, uint32(len([]byte(i.name)))); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, []byte(i.name)); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.seq); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.algo); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.size); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.dataSize); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.rows); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.cols); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, i.idxs); err != nil {
		return
	}
	if err = binary.Write(&buffer, binary.BigEndian, uint64(len(i.extents))); err != nil {
		return
	}
	i.mutex.RLock()
	extents := i.extents
	i.mutex.RUnlock()
	for _, ext := range extents {
		if err = binary.Write(&buffer, binary.BigEndian, ext.typ); err != nil {
			return
		}
		if err = binary.Write(&buffer, binary.BigEndian, ext.offset); err != nil {
			return
		}
		if err = binary.Write(&buffer, binary.BigEndian, ext.length); err != nil {
			return
		}
		if err = binary.Write(&buffer, binary.BigEndian, ext.data.offset); err != nil {
			return
		}
		if err = binary.Write(&buffer, binary.BigEndian, ext.data.length); err != nil {
			return
		}
	}
	return buffer.Bytes(), err
}
func (i *Inode) UnMarshal(cache *bytes.Buffer, inode *Inode) (n int, err error) {
	var nameLen uint32
	var extentLen uint64
	n = 0
	if err = binary.Read(cache, binary.BigEndian, &inode.magic); err != nil {
		return
	}
	if inode.magic != MAGIC {
		return 0, nil
	}
	n += int(unsafe.Sizeof(inode.magic))
	if err = binary.Read(cache, binary.BigEndian, &inode.inode); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.inode))
	if err = binary.Read(cache, binary.BigEndian, &inode.typ); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.typ))
	if err = binary.Read(cache, binary.BigEndian, &nameLen); err != nil {
		return
	}
	n += int(unsafe.Sizeof(nameLen))
	name := make([]byte, nameLen)
	if err = binary.Read(cache, binary.BigEndian, name); err != nil {
		return
	}
	n += len(name)
	inode.name = string(name)
	if err = binary.Read(cache, binary.BigEndian, &inode.seq); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.seq))
	if err = binary.Read(cache, binary.BigEndian, &inode.algo); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.algo))
	if err = binary.Read(cache, binary.BigEndian, &inode.size); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.size))
	if err = binary.Read(cache, binary.BigEndian, &inode.dataSize); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.dataSize))
	if err = binary.Read(cache, binary.BigEndian, &inode.rows); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.rows))
	if err = binary.Read(cache, binary.BigEndian, &inode.cols); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.cols))
	if err = binary.Read(cache, binary.BigEndian, &inode.idxs); err != nil {
		return
	}
	n += int(unsafe.Sizeof(inode.idxs))
	if err = binary.Read(cache, binary.BigEndian, &extentLen); err != nil {
		return
	}
	n += int(unsafe.Sizeof(extentLen))
	inode.extents = make([]Extent, extentLen)
	for i := 0; i < int(extentLen); i++ {
		if err = binary.Read(cache, binary.BigEndian, &inode.extents[i].typ); err != nil {
			return
		}
		n += int(unsafe.Sizeof(inode.extents[i].typ))
		if err = binary.Read(cache, binary.BigEndian, &inode.extents[i].offset); err != nil {
			return
		}
		n += int(unsafe.Sizeof(inode.extents[i].offset))
		if err = binary.Read(cache, binary.BigEndian, &inode.extents[i].length); err != nil {
			return
		}
		n += int(unsafe.Sizeof(inode.extents[i].length))
		if err = binary.Read(cache, binary.BigEndian, &inode.extents[i].data.offset); err != nil {
			return
		}
		n += int(unsafe.Sizeof(inode.extents[i].data.offset))
		if err = binary.Read(cache, binary.BigEndian, &inode.extents[i].data.length); err != nil {
			return
		}
		n += int(unsafe.Sizeof(inode.extents[i].data.length))
	}
	return
}
