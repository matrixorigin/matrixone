package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type ObjectReader struct {
	object *Object
	name   string
	root   string
}

func NewObjectReader(name string, dir string) (*ObjectReader, error) {
	var err error
	reader := &ObjectReader{
		name: name,
	}
	reader.object, err = NewObject(name, dir)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *ObjectReader) Read(extent Extent, idxs []uint16) (*fileservice.IOVector, error) {
	var err error
	meta := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 1),
	}
	meta.Entries[0] = fileservice.IOEntry{
		Offset: int(extent.offset),
		Size:   int(extent.Length()),
	}
	err = r.object.oFile.Read(nil, meta)
	if err != nil {
		return nil, err
	}
	block := &Block{}
	err = block.UnShowMeta(meta.Entries[0].Data)
	if err != nil {
		return nil, err
	}
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, idx := range idxs {
		col := block.columns[idx]
		entry := fileservice.IOEntry{
			Offset: int(col.meta.location.Offset()),
			Size:   int(col.meta.location.Length()),
		}
		data.Entries = append(data.Entries, entry)
	}
	err = r.object.oFile.Read(nil, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
