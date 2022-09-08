package objectio

import "github.com/matrixorigin/matrixone/pkg/fileservice"

const Magic = 0xFFFFFFFF
const Version = 1
const RootPath = "ObjectIo"
const FSName = "local"

type Object struct {
	name  string
	oFile fileservice.FileService
}

func NewObject(name string, dir string) (*Object, error) {
	var err error
	object := &Object{
		name: name,
	}
	object.oFile, err = fileservice.NewLocalFS(FSName, dir, 0)
	if err != nil {
		return nil, err
	}
	return object, nil
}
