package objectio

type MetaDriver struct {
	seg   []*Object
	blk   []*Object
	inode []*Object
	fs    *ObjectFS
}

func newMetaDriver(fs *ObjectFS) *MetaDriver {
	return &MetaDriver{fs: fs}
}

func (m *MetaDriver) Append(inode *Inode) (err error) {
	buf, err := inode.Marshal()
	if err != nil {
		return err
	}
	inodeData, err := m.GetMeta(uint64(len(buf)), NodeType)
	offset, allocated, err := inodeData.Append(buf)
	if err != nil {
		return err
	}

	blkData, err := m.GetMeta(uint64(len(buf)), MetadataBlkType)

	return nil
}

func (m *MetaDriver) GetMeta(size uint64, typ ObjectType) (object *Object, err error) {
	m.fs.RWMutex.Lock()
	defer m.fs.RWMutex.Unlock()
	if len(m.inode) == 0 ||
		m.inode[len(m.inode)-1].GetSize()+size >= ObjectSize {
		object, err = OpenObject(m.fs.lastId, typ, m.fs.attr.dir)
		object.Mount(ObjectSize, MetaSize)
		m.inode = append(m.inode, object)
		m.fs.lastId++
		return
	}
	return m.inode[len(m.inode)-1], nil
}
