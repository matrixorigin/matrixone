package objectio

import "github.com/matrixorigin/matrixone/pkg/fileservice"

const Magic = 0xFFFFFFFF
const Version = 1
const FSName = "local"

type Object struct {
	// name is the object file's name
	name string
	// fs is an instance of fileservice
	fs fileservice.FileService
}

func NewObject(name string, fs fileservice.FileService) *Object {
	object := &Object{
		name: name,
		fs:   fs,
	}
	return object
}
