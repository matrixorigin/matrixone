package objectio

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"io"
	"unsafe"
)

type MetaDriver struct {
	//seg   []*Object
	blk   []*Object
	inode []*Object
	fs    *ObjectFS
}

type MetaPage struct {
	object *Object
	extent Extent
}

func newMetaDriver(fs *ObjectFS) *MetaDriver {
	return &MetaDriver{fs: fs}
}

func (m *MetaDriver) Append(file *ObjectFile) (err error) {
	buf, err := file.inode.Marshal()
	if err != nil {
		return err
	}
	page, err := m.GetPage(uint64(len(buf)), NodeType)
	if err != nil {
		return err
	}
	_, err = page.object.Append(buf, int64(page.extent.offset))
	if err != nil {
		return err
	}
	file.extent = page.extent
	buf, err = file.parent.Marshal()
	if err != nil {
		return err
	}
	page, err = m.GetPage(uint64(len(buf)), MetadataBlkType)
	if err != nil {
		return err
	}
	file.parent.inode.mutex.Lock()
	file.parent.inode.objectId = page.object.id
	file.parent.extent = page.extent
	file.parent.inode.mutex.Unlock()
	_, err = page.object.Append(buf, int64(page.extent.offset))
	return err
}

// GetPage Find a metadata object based on type and allocate a page
func (m *MetaDriver) GetPage(size uint64, typ ObjectType) (page *MetaPage, err error) {
	m.fs.RWMutex.Lock()
	defer m.fs.RWMutex.Unlock()
	page = &MetaPage{}
	page.extent = Extent{}
	var object *Object
	objects := &m.blk
	if typ == NodeType {
		objects = &m.inode
	}
	if len(*objects) == 0 ||
		(*objects)[len(*objects)-1].GetSize()+size >= ObjectSize {
		object, err = OpenObject(m.fs.lastId, typ, m.fs.attr.dir)
		if err != nil {
			return
		}
		object.Mount(ObjectSize, MetaSize)
		// reserve the page for writing the object header
		object.allocator.available += MetaSize
		*objects = append(*objects, object)
		m.fs.lastId++
	}
	object = (*objects)[len(*objects)-1]
	offset, length := object.allocator.Allocate(size)
	page.object = object
	page.extent = Extent{
		typ:    APPEND,
		offset: uint32(offset),
		length: uint32(length),
		data:   entry{offset: 0, length: uint32(size)},
	}
	return
}

func (m *MetaDriver) Replay() error {
	for _, blk := range m.blk {
		var off int64 = MetaSize
		for {
			cache := bytes.NewBuffer(make([]byte, 2*1024*1024))
			pos, hole, err := m.RebuildTree(cache, off, blk)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if off >= int64(blk.GetSize()) || hole >= HoleSize {
				break
			}
			off += int64(pos)
		}
	}
	return nil
}

func (m *MetaDriver) RebuildTree(data *bytes.Buffer, offset int64, object *Object) (pos int, hole uint32, err error) {
	hole = 0
	pos, err = object.oFile.ReadAt(data.Bytes(), offset)
	if err != nil && err != io.EOF {
		return 0, hole, err
	}
	buffer := data.Bytes()
	cache := bytes.NewBuffer(buffer)
	for {
		if hole >= HoleSize {
			break
		}
		inode := &Inode{}
		n, err := inode.UnMarshal(cache, inode)
		if err != nil {
			return 0, hole, err
		}
		if n == 0 {
			if MetaSize == cache.Len() {
				break
			}
			cache = bytes.NewBuffer(cache.Bytes()[MetaSize-uint32(unsafe.Sizeof(inode.magic)):])
			hole += MetaSize
			continue
		}
		inodeSize := uint32(p2roundup(uint64(n), MetaSize))
		seekLen := inodeSize - (uint32(n) % inodeSize)
		dir := m.fs.nodes[inode.parent]
		if dir == nil {
			dir = openObjectDir(m.fs, inode.parent)
			m.fs.nodes[inode.parent] = dir
		}
		if inode.state == REMOVE {
			delete(dir.(*ObjectDir).nodes, inode.name)
		} else {
			file := &ObjectDir{}
			file.fs = m.fs
			file.inode = inode
			file.nodes = make(map[string]tfs.File)
			block := dir.(*ObjectDir).nodes[file.inode.name]
			if (block == nil || block.(*ObjectDir).inode.create < file.inode.create) &&
				file.inode.state == RESIDENT {
				dir.(*ObjectDir).nodes[file.inode.name] = file
				file.Ref()
				//file.OnZeroCB = file.(*ObjectDir).Close
			}
		}
		if int(seekLen) == cache.Len() {
			break
		}
		cache = bytes.NewBuffer(cache.Bytes()[seekLen:])
	}
	return pos, hole, nil
}
