package objectio

import "github.com/matrixorigin/matrixone/pkg/fileservice"

const Magic = 0xFFFFFFFF
const Version = 1

type Object struct {
	oFile fileservice.FileService
}

func NewObject(name string, path string) (*Object, error) {
	var err error
	object := &Object{}
	object.oFile, err = fileservice.NewLocalFS(name, path, 0)
	if err != nil {
		return nil, err
	}
	return object, nil
}
