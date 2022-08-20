// Copyright 2022 Matrix Origin
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

package fileservice

import "fmt"

func Get[T any](fs FileService, name string) (res T, err error) {
	if fs, ok := fs.(*FileServices); ok {
		f, ok := fs.mappings[name]
		if !ok {
			err = fmt.Errorf("file service not found: %s", name)
			return
		}
		res, ok = f.(T)
		if !ok {
			err = fmt.Errorf("%T does not implement %T", f, res)
			return
		}
		return
	}
	var ok bool
	res, ok = fs.(T)
	if !ok {
		err = fmt.Errorf("%T does not implement %T", fs, res)
		return
	}
	if fs.Name() != name {
		err = fmt.Errorf("file service name not match, expecting %s, got %s", name, fs.Name())
		return
	}
	return
}
