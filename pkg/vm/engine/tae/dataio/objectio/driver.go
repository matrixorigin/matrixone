package objectio

type DriverPage struct {
	object *Object
	extent Extent
}

type MetaDriver struct {
	seg   []*Object
	blk   []*Object
	inode []*Object
	fs    *ObjectFS
}

func newMetaDriver(fs *ObjectFS) *MetaDriver {
	return &MetaDriver{fs: fs}
}

func (m *MetaDriver) Append(file *ObjectFile) (err error) {
	buf, err := file.inode.Marshal()
	if err != nil {
		return err
	}
	page, err := m.GetMeta(uint64(len(buf)), NodeType)
	_, err = page.object.Append(buf, int64(page.extent.offset))
	if err != nil {
		return err
	}
	buf, err = file.parent.inode.Marshal()
	if err != nil {
		return err
	}
	page, err = m.GetMeta(uint64(len(buf)), MetadataBlkType)
	file.parent.inode.mutex.Lock()
	file.parent.inode.objectId = page.object.id
	file.parent.extent.offset = page.extent.offset
	file.parent.extent.length = page.extent.length
	file.parent.inode.mutex.Unlock()
	_, err = page.object.Append(buf, int64(page.extent.offset))
	if err != nil {
		return err
	}

	return nil
}

func (m *MetaDriver) GetMeta(size uint64, typ ObjectType) (page *DriverPage, err error) {
	m.fs.RWMutex.Lock()
	defer m.fs.RWMutex.Unlock()
	page = &DriverPage{}
	page.extent = Extent{}
	var object *Object
	if len(m.inode) == 0 ||
		m.inode[len(m.inode)-1].GetSize()+size >= ObjectSize {
		object, err = OpenObject(m.fs.lastId, typ, m.fs.attr.dir)
		if err != nil {
			return
		}
		object.Mount(ObjectSize, MetaSize)
		m.inode = append(m.inode, object)
		m.fs.lastId++
	}
	object = m.inode[len(m.inode)-1]
	offset, length := object.allocator.Allocate(size)
	page.object = object
	page.extent = Extent{
		offset: uint32(offset),
		length: uint32(length),
	}
	return
}
