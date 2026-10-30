// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"errors"
	"os"
)

type anonymousUnlinkError struct{ cause error }

func (e *anonymousUnlinkError) Error() string { return e.cause.Error() }
func (e *anonymousUnlinkError) Unwrap() error { return e.cause }

// Publication requires successful unlink. On failure the caller receives no
// descriptor and the spill-root wrapper fences further creation. The operation
// arguments keep fault injection local, without mutable global test hooks.
func createAndRemoveFile(path string, create func(string) (*os.File, error), remove func(string) error) (*os.File, error) {
	file, err := create(path)
	if err != nil {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
		return nil, err
	}
	if err := remove(path); err != nil {
		return nil, errors.Join(&anonymousUnlinkError{cause: err}, file.Close())
	}
	return file, nil
}
