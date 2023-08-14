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

func (o *Object) GetFs() fileservice.FileService {
	return o.fs
}
