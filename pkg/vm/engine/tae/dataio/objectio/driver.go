package objectio

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
	return nil
}
